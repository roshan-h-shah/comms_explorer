import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import logging
from datetime import date, timedelta
import math

# Configure logging for better output and debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def scrape_ooni_explorer(
    test_name: str,
    horizon: int = 30,
    country: str = "",
    only: str = ""
) -> tuple[str, int, int]:
    """
    Scrapes OONI Explorer for the given parameters, returns:
      - markdown_table (str)
      - anomaly_count (int)
      - accessible_count (int)

    Params:
      test_name: the required OONI test_name (e.g. 'signal', 'web_connectivity')
      horizon:   how many days back to go (defaults to 30)
      country:   optional 2-letter probe_cc filter (defaults to '')
      only:      optional property filter: 'none' or 'anomalies' (defaults to '')
    """
    # Build date range
    today = date.today() + timedelta(days=1)       # include today
    since = (today - timedelta(days=horizon)).isoformat()
    until = today.isoformat()

    # Construct URL
    url = (
        f"https://explorer.ooni.org/search"
        f"?since={since}&until={until}"
        f"&failure=false"
        f"&probe_cc={country}"
        f"&test_name={test_name}"
        f"&only={only}"
    )
    logging.info(f"Querying URL: {url}")

    browser = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Load and wait for rows
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await page.wait_for_selector('div.flex.items-center', state='visible', timeout=45000)

            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')

            # --- Find all result rows ---
            result_rows = soup.select('div.flex.items-center')
            logging.info(f"Found {len(result_rows)} rows")

            # Define the five columns in order
            cols = [
                "w-1/12",    # country code
                "w-2/12",    # ASN
                "w-2/12",    # timestamp
                "w-4/12",    # test name
                "md:w-2/5"   # status (Accessible/Anomaly)
            ]

            all_rows = []
            anomaly_count    = 0
            accessible_count = 0

            # --- Extract each row’s columns ---
            for i, row in enumerate(result_rows):
                cells = []
                for cls in cols:
                    sel = cls.replace(":", "\\:").replace("/", "\\/")
                    el = row.select_one(f"div.{sel}")
                    text = el.get_text(strip=True) if el else ""
                    cells.append(text)

                # Only keep non-empty rows
                if any(cells):
                    country_cc, asn, ts, test, status = cells
                    all_rows.append(cells)

                    # Tally statuses
                    if status.lower() == "anomaly":
                        anomaly_count += 1
                    elif status.lower() in ("accessible", "accesible"):  # typo tolerance
                        accessible_count += 1

                if (i + 1) % 10 == 0:
                    logging.info(f"Processed {i+1} rows")

            # --- Build Markdown table ---
            md_lines = ["| Country | ASN | Timestamp | Test | Status |",
                        "|---|---|---|---|---|"]
            #odd even switch 
            _ = 0
            for country_cc, asn, ts, test, status in all_rows:
                if _ % 2 == 0:
                    md_lines.append(f"| {country_cc} | {asn} | {ts} | {test} | {status} |")
                _ += 1

            # Append summary
            md_lines.append("")
            md_lines.append(f"**Total rows:** {math.ceil(len(all_rows)/2)}")
            md_lines.append(f"**Anomalies:** {anomaly_count}")
            md_lines.append(f"**Accessible:** {accessible_count}")

            markdown_table = "\n".join(md_lines)
            return markdown_table, anomaly_count, accessible_count

    except Exception:
        logging.exception("Error scraping")
        # Return empty table and zero counts on error
        return "| Country | ASN | Timestamp | Test | Status |\n|---|---|---|---|---|\n", 0, 0

    finally:
        if browser:
            await browser.close()
            logging.info("Browser closed.")


# Example usage
if __name__ == "__main__":
    # Must supply test_name; horizon, country, only use defaults
    md, anomalies, accessible = asyncio.run(
        scrape_ooni_explorer(test_name="signal")
    )
    print(md)
    print(f"Anomalies: {anomalies}, Accessible: {accessible}")
