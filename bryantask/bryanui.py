
# ASYNC style!

# app.py

import os
import time
import streamlit as st
import pandas as pd
import broadsql
import final_pipeline  # combined_pipeline, markdown_to_docx, docx_to_pdf33

st.set_page_config(page_title="SQL-RAG Explorer", layout="wide")
st.title("SQL-RAG Pipeline Explorer")

# --- User Inputs ---
user_query = st.text_input("Enter your query", max_chars=200)

test_names = st.multiselect(
    "Select OONI tests",
    ["signal", "web_connectivity", "whatsapp", "facebook_messenger", "telegram"],
    default=["signal"]
)
only = st.selectbox("Property filter (only)", ["none", "anomalies"], index=1)
horizon = st.slider("Horizon (days)", 1, 90, 30)

if st.button("Run Query"):
    st.markdown(f"**Query:** {user_query}")

    # --- 1) Show raw & filtered SQL tables ---
    tables = [
        "mcc_mnc_table",
        "traforama_isp_list",
        "mideye_mobile_network_list",
    ]
    metrics = []
    countries = []

    for idx, table in enumerate(tables):
        with st.spinner(f"Loading table: {table}…"):
            t0 = time.time()
            df = broadsql.con.execute(f"SELECT * FROM {table}").df()
            st.subheader(f"Raw data: {table}")
            st.dataframe(df)

            vals = broadsql.extract_relevant_rows(df, user_query)
            if idx == 0:
                countries = vals  # first table drives country list
            filtered = broadsql.filter_df(df, "Country", countries)
            st.subheader(f"Filtered rows: {table}")
            st.dataframe(filtered)

            metrics.append({
                "step": f"load_{table}",
                "rows": len(filtered),
                "time": round(time.time() - t0, 2)
            })

    # --- 2) Generate the parallelized report ---
    st.markdown("## Generating Complete Report")
    t0 = time.time()
    # this call now does SQL → DC → OONI → Radar all in parallel,
    # with four small LLM prompts under the hood
    report_md = final_pipeline.combined_pipeline(
        user_query=user_query,
        sql_tables=tables,
        test_names=test_names,
        only=only,
        horizon=horizon
    )
    elapsed = time.time() - t0
    metrics.append({"step": "final_report", "rows": "-", "time": round(elapsed, 2)})

    st.markdown(report_md)

    # --- 3) Download Word / PDF ---
    st.markdown("### 📄 Download Report")
    docx_path = final_pipeline.markdown_to_docx(report_md, "report.docx")
  

    with open(docx_path, "rb") as f_docx:
        st.download_button(
            "Download Word (.docx)",
            data=f_docx,
            file_name="report.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )

    # --- 4) Show performance metrics ---
    st.markdown("## Performance Metrics")
    st.table(pd.DataFrame(metrics))

