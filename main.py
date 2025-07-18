from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import bryantask.final_truly_async as fta
from bryantask import broadsqlasync
from fastapi.responses import HTMLResponse
import os

app = FastAPI()

# Allow frontend to call API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For dev, restrict in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ReportRequest(BaseModel):
    user_query: str
    sql_tables: list[str]
    test_names: list[str]
    only: str = ""
    horizon: int = 30

@app.post("/run_report")
async def run_report(req: ReportRequest):
    try:
        result = await fta.combined_pipeline(
            user_query=req.user_query,
            sql_tables=req.sql_tables,
            test_names=req.test_names,
            only=req.only,
            horizon=req.horizon
        )
        return {"success": True, "report": result}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/raw_tables")
async def get_raw_tables(user_query: str = Query("Describe everything comparing india and pakistan")):
    table_names = ["mcc_mnc_table", "traforama_isp_list", "mideye_mobile_network_list"]
    raw_tables = []
    filtered_tables = []
    countries_list = []
    # Get raw tables
    for name in table_names:
        df = fta.con.execute(f"SELECT * FROM {name} LIMIT 20").df()
        columns = list(df.columns)
        rows = df.values.tolist()
        raw_tables.append({
            "name": name,
            "columns": columns,
            "rows": rows
        })
    # Get filtered tables
    for i, name in enumerate(table_names):
        df = fta.con.execute(f"SELECT * FROM {name}").df()
        if i == 0:
            countries_list = await broadsqlasync.extract_relevant_rows(df, user_query)
        filtered = broadsqlasync.filter_df(df, "Country", countries_list)
        filtered = filtered.head(20)
        columns = list(filtered.columns)
        rows = filtered.values.tolist()
        filtered_tables.append({
            "name": name,
            "columns": columns,
            "rows": rows
        })
    return {"raw_tables": raw_tables, "filtered_tables": filtered_tables}

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    with open("index.html", encoding="utf-8") as f:
        return f.read()