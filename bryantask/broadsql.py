from langchain_ollama import ChatOllama
from langchain_google_genai import ChatGoogleGenerativeAI
import pandas as pd
import duckdb
import logging
import os
from langchain.prompts import PromptTemplate
import re
import ast
from datacenter import run_scrape_and_markdown
# --- Init ---
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to DuckDB database
db_path = os.path.join(os.path.dirname(__file__), "bryan.db")
con = duckdb.connect(db_path) #before getting this error 
'''
  /Users/apuser6/Documents/Basic                                                        
  Demo/.venv/lib/python3.13/site-packages/streamlit/runtime/scriptrunner/exec_code.py:  
  128 in exec_func_with_error_handling                                                  
                                                                                        
  /Users/apuser6/Documents/Basic                                                        
  Demo/.venv/lib/python3.13/site-packages/streamlit/runtime/scriptrunner/script_runner  
  .py:669 in code_to_exec                                                               
                                                                                        
  /Users/apuser6/Documents/Basic Demo/bryantask/bryanui.py:47 in <module>               
                                                                                        
     44 │   │   │   start = time.time()                                                 
     45 │   │   │                                                                       
     46 │   │   │   # load & display raw                                                
  ❱  47 │   │   │   df = broadsql.con.execute(f"SELECT * FROM {table}").df()            
     48 │   │   │   st.subheader(f"Raw data: {table}")                                  
     49 │   │   │   st.dataframe(df)                                                    
     50                                                                                 
────────────────────────────────────────────────────────────────────────────────────────
CatalogException: Catalog Error: Table with name mcc_mnc_table does not exist!
Did you mean "pg_tables"?'''
#con = duckdb.connect("bryan.db")

# LLM Setup
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    google_api_key=GOOGLE_API_KEY,
    temperature=0
)

# ----------------------------------------
# Basic query wrapper
# ----------------------------------------
def query_llm(agent_input: str, model=llm) -> str:
    #logger.info(f"Calling LLM with prompt:\n{agent_input.strip()}") #-> prompts are very long - so many countries
    response = model.invoke(agent_input)
    logger.info(f"LLM Response:\n{response.content.strip()}")
    return response.content.strip()


# Step 2: Select top relevant row values
# ----------------------------------------
def extract_relevant_rows(df: pd.DataFrame, user_query: str, column: str = "Country") -> list[str]: #GOOD through testing!
    #logger.info(f"Extracting relevant rows from column '{column}' for query: {user_query}") long ash
    unique_values = df[column].dropna().unique().tolist()
    prompt = PromptTemplate.from_template(
        """
The user asked: {user_query}

You are given a list of values from the column '{column}'. Pick the most relevant values needed to answer the user's question:

{items}

Respond as a Python list of strings.
""").format(
        user_query=user_query,
        column=column,
        items="\n".join(unique_values)
    )
    raw = query_llm(prompt)

    # 1) Strip any ``` fences (and optional language tag)
    #    e.g. ```python\n['Mexico']\n```
    stripped = re.sub(r"^```[^\n]*\n|```$", "", raw.strip())

    try:
        # 2) Safely parse the literal list
        parsed = ast.literal_eval(stripped)
        logger.info(f"Values selected: {parsed}")
        return parsed
    except Exception as e:
        logger.warning(f"Failed to parse row selection. Using fallback. Error: {e}")
        # fallback: return all unique values or an empty list
        return unique_values





# ----------------------------------------
# Step 3: Filter dataframe for those values
# ----------------------------------------
def filter_df(df: pd.DataFrame, column: str, values: list[str]) -> pd.DataFrame:
    logger.info(f"Filtering DataFrame on column '{column}' for values: {values}")
    return df[df[column].isin(values)]

# ----------------------------------------
# Step 4: Format to markdown for LLM
# ----------------------------------------
def df_to_markdown(df: pd.DataFrame) -> str:
    logger.info(f"Converting filtered DataFrame with {len(df)} rows to markdown")
    return df.to_markdown(index=False)

# ----------------------------------------
# Step 5: Answer question using gathered context
# ----------------------------------------
def answer_question(user_query: str, context_md: str) -> str:
    logger.info("Generating final detailed report from LLM using gathered context")
    prompt = PromptTemplate.from_template(
        """
You are an expert data analyst and technical writer. You have the following raw table data:

{context}

Your task: produce a structured, detailed report that fully answers the user’s question:
"{user_query}"

Report requirements:
1. **Executive Summary**  
   - One or two sentences capturing the key findings.
2. **Section per Data Source**  
   For each table:
   - Use a second-level heading with the table name.
   - List every relevant item (e.g. provider names) in bullet form.
   - Include any supplemental details (e.g. network codes, display text).
   - If possible, show simple metrics (e.g. “Total providers: X”).
3. **Overall Insights**  
   - A final section summarizing cross-table observations, trends, or recommendations.
4. **Formatting**  
   - Use Markdown headings, bullet lists, and tables.
   - Keep it visually clear and easy to navigate.

If you lack data to address a sub-question, simply omit that part.

---
"""
    ).format(context=context_md, user_query=user_query)

    return query_llm(prompt)

# ----------------------------------------
# Main pipeline
# ----------------------------------------
def sql_rag_pipeline(user_query: str, table_names: list[str]) -> str:
    logger.info(f"Starting SQL-RAG pipeline for query: {user_query}")
    all_markdown = []
    countries_list = []
    count = 0 
    for table in table_names:
        logger.info(f"Processing table: {table}")
        df = con.execute(f"SELECT * FROM {table}").df()
        if df.empty:
            logger.warning(f"Table '{table}' is empty, skipping.")
            continue
        column = "Country"
        if count == 0: #Workaround to avoid the fact that otherwise different countries would be selected for different lists based on whats available
            values = extract_relevant_rows(df, user_query)
            countries_list = values
        else:
            values = countries_list
        filtered_df = filter_df(df, column, values)
        md = df_to_markdown(filtered_df)
        all_markdown.append(f"### Table: {table}\n{md}")
        count += 1

    context_md = "\n\n".join(all_markdown) or "No relevant data found."
    logger.info("Context aggregation completed.")

    # Final answer step
    final_answer = answer_question(user_query, context_md)
    logger.info("Pipeline completed successfully.")
    return final_answer

# --- Example use ---
if __name__ == "__main__":
    tables = ["mcc_mnc_table","mideye_mobile_network_list", "traforama_isp_list"] #mcc has the most data so we'll use the countries chosen by the agent from they're and keep it static
    #just india that wont work and no ones sending ppl to india!
    user_q = "Tell me all about the mobile network operators, mobile carriers and isp information for the major countires in China"
    answer = sql_rag_pipeline(user_q, tables)
    print(answer)


# LOOKING Great


