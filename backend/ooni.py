#OONI official API
#much much faster (20x)
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

'''import asyncio
from bs4 import BeautifulSoup
import logging
from datetime import date, timedelta
import math
import os
import socket
from scraperapi_sdk import ScraperAPIClient
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(os.path.join(project_root, '.env'))

def get_public_ip():
    try:
        import requests
        return requests.get('https://api.ipify.org').text
    except Exception:
        return 'unknown'

async def scrape_ooni_explorer(
    test_name: str,
    horizon: int = 30,
    country: str = "",
    only: str = ""
) -> tuple[str, int, int]:
    logging.info(f"[ooni] ENV: {os.environ.get('ENV', 'local')}, public IP: {get_public_ip()}")

    today = date.today() + timedelta(days=1)
    since = (today - timedelta(days=horizon)).isoformat()
    until = today.isoformat()

    url = (
        f"https://explorer.ooni.org/search"
        f"?since={since}&until={until}"
        f"&failure=false"
        f"&probe_cc={country}"
        f"&test_name={test_name}"
        f"&only={only}"
    )
    logging.info(f"Querying URL via ScraperAPI: {url}")

    try:
        api_key = os.environ.get("SCRAPERAPI_KEY")
        if not api_key:
            raise ValueError("SCRAPERAPI_KEY environment variable not set in .env file.")
        client = ScraperAPIClient(api_key)

        # Retry fetching HTML content until it's fully rendered
        for attempt in range(3):
            html_content = client.get(url=url, params={"render": True})
            logging.info(f"[ScraperAPI] Attempt {attempt + 1}: HTML length = {len(html_content)}")

            print(html_content[:1000])  # Print a sample for debugging

            if "nprogress-busy" not in html_content and "/m/" in html_content:
                break  # Fully rendered
            logging.warning(f"[ScraperAPI] Incomplete render (attempt {attempt + 1}). Retrying...")
            await asyncio.sleep(2 * (attempt + 1))  # Backoff
        else:
            raise RuntimeError("ScraperAPI failed to render full page after 3 attempts.")

        if not html_content.strip():
            raise ValueError("Empty HTML content returned from ScraperAPI")

        logging.info(f"HTML content received for OONI — length: {len(html_content)} chars")
        with open("ooni_scraperapi_dump.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        logging.info("[debug] HTML content written to ooni_scraperapi_dump.html")

        soup = BeautifulSoup(html_content, 'html.parser')
        result_rows = soup.select('a[href^="/m/"] > div.flex.flex-wrap.items-stretch.text-gray-700.bg-white')

        logging.info(f"Found {len(result_rows)} rows")

        cols = [
            "w-1/12",
            "w-2/12",
            "w-2/12",
            "w-4/12",
            "md:w-2/5"
        ]

        all_rows = []
        anomaly_count = 0
        accessible_count = 0

        for i, row in enumerate(result_rows):
            cells = []
            for cls in cols:
                sel = cls.replace(":", "\\:").replace("/", "\\/")
                el = row.select_one(f"div.{sel}")
                text = el.get_text(strip=True) if el else ""
                cells.append(text)

            if any(cells):
                country_cc, asn, ts, test, status = cells
                all_rows.append(cells)
                if status.lower() == "anomaly":
                    anomaly_count += 1
                elif status.lower() in ("accessible", "accesible"):
                    accessible_count += 1

            if (i + 1) % 10 == 0:
                logging.info(f"Processed {i+1} rows")

        md_lines = ["| Country | ASN | Timestamp | Test | Status |",
                    "|---|---|---|---|---|"]
        _ = 0
        for country_cc, asn, ts, test, status in all_rows:
            if _ % 2 == 0:
                md_lines.append(f"| {country_cc} | {asn} | {ts} | {test} | {status} |")
            _ += 1

        md_lines.append("")
        md_lines.append(f"**Total rows:** {math.ceil(len(all_rows)/2)}")
        md_lines.append(f"**Anomalies:** {math.floor(anomaly_count/2)}")
        md_lines.append(f"**Accessible:** {math.ceil(accessible_count/2)}")

        markdown_table = "\n".join(md_lines)
        return markdown_table, anomaly_count, accessible_count

    except Exception as e:
        logging.error("==== Error Summary ====")
        logging.error(f"URL: {url}")
        logging.error(f"Hostname: {socket.gethostname()}")
        logging.error(f"Public IP: {get_public_ip()}")
        logging.error(f"SCRAPERAPI_KEY Loaded: {'Yes' if api_key else 'No'}")
        logging.error(f"Error Type: {type(e).__name__}")
        logging.error(f"Error Message: {str(e)}")
        logging.exception("Full traceback:")
        return f"| Error |\n|---|\n| {str(e)} |\n", 0, 0


# Example usage
if __name__ == "__main__":
    md, anomalies, accessible = asyncio.run(
        scrape_ooni_explorer(test_name="signal")
    )
    print(md)
    print(f"Anomalies: {anomalies}, Accessible: {accessible}")
'''