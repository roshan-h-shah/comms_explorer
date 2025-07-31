'''
Orchestration pipeline from all the scrapers, SQL handlers and LLM calls - this is the bulk of the code
'''
import os
import logging
import duckdb
from backend import broadsqlasync
from backend import asynccloudflare
from backend.datacenter import run_scrape_and_markdown 
from backend.ooni import scrape_ooni_explorer   
from backend.country_code_converter import get_alpha2_from_country_name
from langchain.prompts import PromptTemplate
import asyncio
import concurrent.futures  
from dotenv import load_dotenv

# --- Init ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to DuckDB database
DB_PATH = os.path.join(os.path.dirname(__file__), "bryan.db")
con = duckdb.connect(DB_PATH)

from langchain_openai import ChatOpenAI
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(os.path.join(project_root, '.env'))

open_ai_api_key = os.environ.get("OPENAI_API_KEY")
if not open_ai_api_key:
    raise ValueError("OpenAI environment variable not set in .env file.")

llm = ChatOpenAI(
    model="gpt-4.1-nano",  # This is the GPT-4.1 "Turbo" model
    openai_api_key=open_ai_api_key,
  
)

# ----------------------------------------
# Async-compatible Data Fetchers & LLM Callers
# ----------------------------------------

async def run_blocking_in_executor(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        return await loop.run_in_executor(executor, func, *args, **kwargs)

async def async_run_scrape_and_markdown_wrapper(countries_list: list[str]) -> str:
    logger.info(f"[DC] Asynchronously scraping data centers for: {countries_list}")
    return await run_blocking_in_executor(run_scrape_and_markdown, countries_list)

async def async_scrape_ooni_explorer_wrapper(test_name: str, horizon: int, country: str, only_anomalies: bool) -> tuple[str, int, int]:
    return await scrape_ooni_explorer(
        test_name=test_name,
        horizon=horizon,
        country=country,
        only_anomalies=only_anomalies
    )

async def async_fetch_and_format_markdown_wrapper(country: str = "", date_range: str = "30d") -> str:
    logger.info(f"[CF] Directly awaiting async Radar data for country: {country}")
    return await asynccloudflare.fetch_and_format_markdown(country=country, date_range=date_range)

# --- Section-specific LLM callers ---

async def answer_sql_section(user_query: str, sql_context: str) -> str:
    prompt = PromptTemplate.from_template(
        """
You are a telecom market analyst.  Below are three raw tables listing
mobile network operators and ISPs for various countries:

{sql_context}

**Task**  
Produce a summary by country in Markdown.  For each country:
- List **Mobile Network Operators** (from the MCC/MNC table).
- List **Internet Service Providers** (from the traforama and mideye tables).
- Group them under a third-level heading (`### CountryName`).

Make sure each country section is consistent and neatly bullet-listed.
"""
    ).format(sql_context=sql_context)
    resp = await llm.ainvoke(prompt)
    return resp.content.strip()

async def answer_dc_section(dc_context: str) -> str:
    prompt = PromptTemplate.from_template(
        """
You are a global infrastructure specialist.  Below is a raw Markdown table of data-center cards:

{dc_context}

**Task**  
1. At the top, write a one-line summary “Total data centers: X”.  
2. Then render a single Markdown table with columns:

| Name | Type | Address | Link |

3. For the **Name** column, render each as `[Name](Link)`.  
4. Ensure every row is correctly formatted and sorted alphabetically by country.
"""
    ).format(dc_context=dc_context)
    resp = await llm.ainvoke(prompt)
    return resp.content.strip()

# --- UPDATED answer_ooni_section to expect Country & Test columns ---
async def answer_ooni_section(ooni_context: str) -> str:
    prompt = PromptTemplate.from_template(
        """
You are a network measurement expert.  Below is raw OONI Explorer data, with each row tagged by country and test:

{ooni_context}

**Task**  
- Build a single Markdown table with columns:

| Country | Test | Anomalies | Accessible |

- One row per (country, test) pair.  
- After the table, add a bullet list **“High-anomaly alerts”** listing any country/test whose anomaly rate exceeds 5%.  
"""
    ).format(ooni_context=ooni_context)
    resp = await llm.ainvoke(prompt)
    return resp.content.strip()

async def answer_radar_section(radar_context: str, date_range: str) -> str:
    print(f"RADAR CONTEXT TO LLM {radar_context}")
    prompt = PromptTemplate.from_template(
        """
You are a web-traffic analyst. Below is raw Radar data for multiple countries.
The data is a series of metrics for each country, including Device Type, IP Version, HTTP Version, TLS Version, and OS. These metrics are all percentages. The data also includes a Domain Popularity ranking.

**Task**

1.  **For the metrics Device Type, IP Version, HTTP Version, TLS Version, and OS:**
    * Create a single Markdown table for each metric.
    * The first column of the table should be the metric's name (e.g., Device Type, IP Version, etc.).
    * The subsequent columns should be the name of each country found in the data.
    * The rows should represent the different categories for that metric (e.g., mobile, desktop, IPv4, IPv6).
    * The cells should contain the percentage value for that category in each country.
    * Sort the rows in each table by the percentage value for the first country listed in the data, in descending order.

2.  **For the Domain Popularity metric:**
    * Create a separate markdown table for each country.
    * The table should have three columns: `Rank`, `Domain`, and `Categories`.
    * Do not truncate or summarize. List all rows.
    * Ensure the table is well-formatted.

**Instructions**
* Do not use the words "Category" or "Value" in any of the tables for the percentage-based metrics.
* All percentage values should be formatted with two decimal places.
* Ensure all tables are rendered using valid Markdown.
* Use the full name for each country, not the two letter ISO country code

**Input Data**
{radar_context}
"""
    ).format(radar_context=radar_context, date_range=date_range)
    resp = await llm.ainvoke(prompt)
    return resp.content.strip()

# ----------------------------------------
# Main Asynchronous Pipeline
# ----------------------------------------
async def async_combined_pipeline(
    user_query: str,
    sql_tables: list[str],
    test_names: list[str],
    only_anomalies: bool = False,
    horizon: int = 30
) -> str:
    report_parts = {}

    # 1) SQL Context
    sql_blocks = []
    countries_list: list[str] = []
    async def get_sql_data():
        nonlocal countries_list
        for i, table in enumerate(sql_tables):
            logger.info(f"[SQL] Querying table: {table}")
            df = await run_blocking_in_executor(con.execute, f"SELECT * FROM {table}")
            df = df.df()
            if i == 0:
                countries = await broadsqlasync.extract_relevant_rows(df, user_query)
                countries_list.extend(countries)
            filtered = broadsqlasync.filter_df(df, "Country", countries_list)
            sql_blocks.append(f"### {table}\n{broadsqlasync.df_to_markdown(filtered)}")
        return "\n\n".join(sql_blocks) or "No SQL data found."
    sql_context = await get_sql_data()

    # Kick off Data Center scrape
    dc_task = asyncio.create_task(async_run_scrape_and_markdown_wrapper(countries_list))

    # Collect OONI tasks with context
    ooni_tasks: list[tuple[str,str,asyncio.Task]] = []
    for test_name in test_names:
        for country in countries_list:
            alpha2 = get_alpha2_from_country_name(country) or ""
            if not alpha2: continue
            task = asyncio.create_task(
                async_scrape_ooni_explorer_wrapper(test_name, horizon, alpha2, only_anomalies)
            )
            ooni_tasks.append((test_name, country, task))

    # Collect Radar tasks
    date_range = f"{horizon}d"
    radar_tasks: list[asyncio.Task] = []
    for country in countries_list:
        alpha2 = get_alpha2_from_country_name(country) or ""
        if alpha2:
            radar_tasks.append(
                asyncio.create_task(
                    async_fetch_and_format_markdown_wrapper(alpha2, date_range)
                )
            )

    # 2) Generate SQL section
    sql_ans = await answer_sql_section(user_query, sql_context)
    report_parts["sql"] = f"## Telecommunications and ISP Summary\n{sql_ans}"
    logger.info("Generated SQL section.")

    # 3) Wait for DC, OONI & Radar in parallel
    logger.info("Starting concurrent fetch for DC, OONI, Radar.")
    dc_result, ooni_results, radar_results = await asyncio.gather(
        dc_task,
        asyncio.gather(*(t for _,_,t in ooni_tasks), return_exceptions=True),
        asyncio.gather(*radar_tasks, return_exceptions=True),
        return_exceptions=True
    )

    # 4) Data Centers section
    if isinstance(dc_result, Exception):
        report_parts["dc"] = f"## Data Centers\nError: {dc_result}"
    else:
        dc_ans = await answer_dc_section(dc_result)
        report_parts["dc"] = f"## Data Centers\n{dc_ans}"

    # 5) Process OONI: build a labeled markdown context
    ooni_lines = ["| Country | Test | Anomalies | Accessible |"]
    for (test_name, country, _), result in zip(ooni_tasks, ooni_results):
        if isinstance(result, Exception):
            logger.warning(f"OONI {test_name}/{country} failed: {result}")
        else:
            md_table, anomalies, accessible = result
            # assume md_table is single-table; we just record counts here
            ooni_lines.append(f"| {country} | {test_name.title()} | {anomalies} | {accessible} |")
    ooni_context = "\n".join(ooni_lines) if len(ooni_lines)>1 else "No OONI data found."
    ooni_ans = await answer_ooni_section(ooni_context)
    report_parts["ooni"] = f"## Communications Tests (OONI Explorer)\n{ooni_ans}"

    # 6) Process Radar
    radar_blocks = []
    for res in radar_results:
        if isinstance(res, Exception):
            logger.warning(f"Radar fetch failed: {res}")
        else:
            radar_blocks.append(res)
    radar_context = "\n\n".join(radar_blocks) or "No Radar data found."
    radar_ans = await answer_radar_section(radar_context, date_range)
    report_parts["radar"] = f"## Device and Domain Data (Cloudflare Radar)\n{radar_ans}"

    # 7) Stitch report
    return "\n\n".join(report_parts.values())

# ----------------------------------------
# Async entrypoint
# ----------------------------------------
async def combined_pipeline(
    user_query: str,
    sql_tables: list[str],
    test_names: list[str],
    only_anomalies: str = "",
    horizon: int = 30
) -> str:
    return await async_combined_pipeline(user_query, sql_tables, test_names, only_anomalies, horizon)

# ----------------------------------------
# CLI runner
# ----------------------------------------
if __name__ == "__main__":
    tables = ["mcc_mnc_table", "traforama_isp_list", "mideye_mobile_network_list"]
    report = asyncio.run(combined_pipeline(
        user_query="United States of America",
        sql_tables=tables,
        test_names=["signal", "whatsapp"],
        only_anomalies=False,
        horizon=30
    ))
    print(report)
