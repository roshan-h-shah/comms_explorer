'''
Takes user's natural language query -> list of countries -> filtering the SQL tables based on these countries asynchronously
'''

import pandas as pd
import duckdb
import logging
import os
from langchain.prompts import PromptTemplate
import re
import ast
import asyncio
# --- Init ---
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to DuckDB database
db_path = os.path.join(os.path.dirname(__file__), "bryan.db")
con = duckdb.connect(db_path)

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(os.path.join(project_root, '.env'))

open_ai_api_key = os.environ.get("OPENAI_API_KEY")
if not open_ai_api_key:
    raise ValueError("OpenAI environment variable not set in .env file.")

llm = ChatOpenAI(
    model="gpt-4.1-nano",  
    openai_api_key=open_ai_api_key,
  
)
# ----------------------------------------
# Async query wrapper
# ----------------------------------------
# Changed to async def
async def query_llm(agent_input: str, model=llm) -> str:
    logger.info(f"Calling LLM with prompt for parsing country list...")
    response = await model.ainvoke(agent_input) # <-- Use ainvoke for async
    logger.info(f"LLM Response received.") # Removed full response log for brevity
    return response.content.strip()


# Step 2: Select top relevant row values
# ----------------------------------------
# Changed to async def
async def extract_relevant_rows(df: pd.DataFrame, user_query: str, column: str = "Country") -> list[str]:
    logger.info(f"Extracting relevant rows from column '{column}' for query: {user_query}")
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
    # Await the async query_llm
    raw = await query_llm(prompt)

    # 1) Strip any ``` fences (and optional language tag)
    #    e.g. ```python\n['Mexico']\n```
    stripped = re.sub(r"^```(?:python|json|txt)?\n|```$", "", raw.strip())

    try:
        # 2) Safely parse the literal list
        parsed = ast.literal_eval(stripped)
        if not isinstance(parsed, list): # Ensure it's actually a list
            raise ValueError("Parsed content is not a list.")
        # Ensure all elements are strings
        parsed = [str(item) for item in parsed]
        logger.info(f"Values selected by LLM: {parsed}")
        return parsed
    except Exception as e:
        logger.warning(f"Failed to parse row selection from LLM ({raw}). Using fallback. Error: {e}")
        # fallback: return all unique values or an empty list. Consider a more
        # sophisticated fallback if this is critical, e.g., a hardcoded list for regions.
        return [] # Returning empty list to prevent potentially incorrect broad data if parsing fails


# ----------------------------------------
# Step 3: Filter dataframe for those values (no change, already synchronous)
# ----------------------------------------
def filter_df(df: pd.DataFrame, column: str, values: list[str]) -> pd.DataFrame:
    logger.info(f"Filtering DataFrame on column '{column}' for values: {values}")
    return df[df[column].isin(values)]

# ----------------------------------------
# Step 4: Format to markdown for LLM (no change, already synchronous)
# ----------------------------------------
def df_to_markdown(df: pd.DataFrame) -> str:
    logger.info(f"Converting filtered DataFrame with {len(df)} rows to markdown")
    return df.to_markdown(index=False)

# ----------------------------------------
# Step 5: Answer question using gathered context
# ----------------------------------------
# Changed to async def
async def answer_question(user_query: str, context_md: str) -> str:
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

    return await query_llm(prompt) # <-- Use await


# ----------------------------------------
# Main pipeline (now async, needs to be called with asyncio.run or await)
# ----------------------------------------
async def sql_rag_pipeline(user_query: str, table_names: list[str]) -> str:
    logger.info(f"Starting SQL-RAG pipeline for query: {user_query}")
    all_markdown = []
    countries_list = []
    count = 0

    # Execute DuckDB queries synchronously, but LLM calls will be async
    # If DuckDB queries become a bottleneck for concurrent execution
    # you would wrap `con.execute().df()` in `await run_blocking_in_executor(...)`
    # from your main pipeline.
    for table in table_names:
        logger.info(f"Processing table: {table}")
        df = con.execute(f"SELECT * FROM {table}").df() # Still synchronous
        if df.empty:
            logger.warning(f"Table '{table}' is empty, skipping.")
            continue
        column = "Country"
        if count == 0:
            values = await extract_relevant_rows(df, user_query) # <-- Await
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
    final_answer = await answer_question(user_query, context_md) # <-- Await
    logger.info("Pipeline completed successfully.")
    return final_answer

# --- Example use (now requires asyncio.run) ---
if __name__ == "__main__":
    tables = ["mcc_mnc_table","mideye_mobile_network_list", "traforama_isp_list"]
    user_q = "Tell me all about the mobile network operators, mobile carriers and isp information for the major countires in South Asia"

    async def main():
        answer = await sql_rag_pipeline(user_q, tables)
        print(answer)

    asyncio.run(main())