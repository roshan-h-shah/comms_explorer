from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import bryantask.final_truly_async as fta
from bryantask import broadsqlasync
from fastapi.responses import HTMLResponse
import os
import logging
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.exception_handlers import request_validation_exception_handler
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
    only_anomalies: bool = False
    horizon: int = 30

@app.post("/run_report")
async def run_report(req: ReportRequest):
    try:
        result = await fta.combined_pipeline(
            user_query=req.user_query,
            sql_tables=req.sql_tables,
            test_names=req.test_names,
            only_anomalies=req.only_anomalies,
            horizon=req.horizon
        )
        return {"success": True, "report": result}
    except Exception as e:
        return {"success": False, "error": str(e)}



@app.get("/raw_tables")
async def get_raw_tables(user_query: str = Query("Tell me all about Peru")):
    try:
        table_names = ["mcc_mnc_table", "traforama_isp_list", "mideye_mobile_network_list"]
        raw_tables = []
        filtered_tables = []
        countries_list = []

        # --- Get raw tables ---
        for name in table_names:
            df = fta.con.execute(f"SELECT * FROM {name}").df()
            raw_tables.append({
                "name": name,
                "columns": list(df.columns),
                "rows": df.values.tolist()
            })

        # --- Get filtered tables ---
        for i, name in enumerate(table_names):
            df = fta.con.execute(f"SELECT * FROM {name}").df()
            if i == 0:
                countries_list = await broadsqlasync.extract_relevant_rows(df, user_query)
            filtered = broadsqlasync.filter_df(df, "Country", countries_list)
            filtered_tables.append({
                "name": name,
                "columns": list(filtered.columns),
                "rows": filtered.values.tolist()
            })

        return JSONResponse(content={"raw_tables": raw_tables, "filtered_tables": filtered_tables})

    except Exception as e:
        logging.exception("Error in /raw_tables route")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    with open("index.html", encoding="utf-8") as f:
        return f.read()


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body": exc.body}
    )