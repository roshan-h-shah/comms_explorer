'''
Uses OONI API - (20x faster + less code then previous playwright web-scraping version) to get results from social media tests via OONI Explorer
'''
import aiohttp
import logging
import math
from datetime import date, timedelta

async def scrape_ooni_explorer(
    test_name: str,
    horizon: int = 30,
    country: str = "",
    results: int = "50",
    only_anomalies: bool = False
) -> tuple[str, int, int]:
    today = date.today()
    since = (today - timedelta(days=horizon)).isoformat()
    until = today.isoformat()

    logging.info(f"Starting to Scrape OONI")
    params = {
        "test_name": test_name,
        "since": since,
        "until": until,
        "limit": results * 2,
        "anomaly": str(only_anomalies).lower() #needs to pass this as a lowercase str instead of bool input
        }
    if country:
        params["probe_cc"] = country.upper()

    url = "https://api.ooni.io/api/v1/measurements"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"OONI API error {resp.status}: {text}")
            data = await resp.json()

    results = data.get("results", [])
    logging.info(f"[ooni-api] Retrieved {len(results)} results from API.")

    anomaly_count = 0
    accessible_count = 0
    rows = []

    for r in results:
        country = r.get("probe_cc", "")
        asn = f"AS {r.get('probe_asn', '')}"
        timestamp = r.get("measurement_start_time", "")  # ISO 8601
        test = r.get("test_name", "")
        status = "Anomaly" if r.get("anomaly") else "Accessible"

        rows.append([country, asn, timestamp, test, status])

        if status == "Anomaly":
            anomaly_count += 1
        else:
            accessible_count += 1

    # Markdown formatting
    md_lines = ["| Country | ASN | Timestamp | Test | Status |", "|---|---|---|---|---|"]
    for i, row in enumerate(rows):
        if i % 2 == 0:  # match your pattern
            md_lines.append(f"| {' | '.join(row)} |")

    md_lines.append("")
    md_lines.append(f"**Total rows:** {math.ceil(len(rows)/2)}")
    md_lines.append(f"**Anomalies:** {int(anomaly_count/2)}")
    md_lines.append(f"**Accessible:** {math.ceil(accessible_count/2)}")
    res = "\n".join(md_lines)
    logging.info(f"Scraped From OONI API: {res}")
    return "\n".join(md_lines), anomaly_count, accessible_count
import asyncio

async def main():
    md_table, anomaly_count, accessible_count = await scrape_ooni_explorer("whatsapp", 30, "CA", 100, True)
    print(md_table)
    print(f"Anomalies: {anomaly_count}, Accessible: {accessible_count}")

if __name__ == "__main__":
    asyncio.run(main())
