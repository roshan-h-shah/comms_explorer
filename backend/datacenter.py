'''
Scrapes from datacenters.com - this is the hardest scraping task - hard website to scrape + can get backoff or timeout errors
'''
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
import logging
from urllib.parse import urljoin
import os
import socket
from scraperapi_sdk import ScraperAPIClient
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env in the project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(os.path.join(project_root, '.env'))
    
async def scrape_datacenter_cards_df(keyword: str) -> pd.DataFrame:
    web_format_keyword = keyword.replace(" ", "%20")
    url = f"https://www.datacenters.com/locations?query={web_format_keyword}"

    try:
        api_key = os.environ.get("SCRAPERAPI_KEY")
        if not api_key:
            raise ValueError("SCRAPERAPI_KEY environment variable not set in .env file.")
        client = ScraperAPIClient(api_key)
        logging.info(f"Beginning Scraping Datacenter with Keyword: {keyword}")
        # Use render=True to enable JS rendering
        html_content = client.get(url=url, params={"render": False})
        soup = BeautifulSoup(html_content, "html.parser")

        card_sel = (
            "a.flex.flex-col.gap-2.rounded.border.border-gray-100."
            "p-2.hover\\:border-teal-300.hover\\:shadow-lg.hover\\:shadow-teal-600\\/40"
        )
        rows = []
        for card in soup.select(card_sel):
            href = card.get("href", "")
            link = urljoin(url, href)
            name_div = card.find("div", class_="text font-medium hover:text-purple")
            name = name_div.get_text(strip=True) if name_div else ""
            gray_divs = card.find_all("div", class_="text-xs text-gray-500")
            dc_type = gray_divs[0].get_text(strip=True) if len(gray_divs) > 0 else ""
            address = gray_divs[1].get_text(strip=True) if len(gray_divs) > 1 else ""
            rows.append({"Name": name, "Type": dc_type, "Address": address, "Link": link})

        df = pd.DataFrame(rows)
        logging.info(f"[{keyword}] scraped {len(df)} rows before filtering")

        if df.empty or "Address" not in df.columns:
            logging.warning(f"[{keyword}] No usable data found — skipping filtering.")
            return pd.DataFrame(columns=["Name", "Type", "Address", "Link"])

        # filter by country
        df["_country"] = df["Address"].apply(lambda a: a.split(",")[-1].strip().lower())
        df.loc[df["_country"] == "usa", "_country"] = "united states"
        df = df[df["_country"] == keyword.lower()].drop(columns=["_country"])
        logging.info(f"[{keyword}] {len(df)} rows remain after filtering to country == '{keyword}'")

        return df

    except Exception as e:
        logging.error("==== Error Summary ====")
        logging.error(f"URL: {url}")
        logging.error(f"Keyword: {keyword}")
        logging.error(f"Hostname: {socket.gethostname()}")
        logging.error(f"SCRAPERAPI_KEY Loaded: {'Yes' if api_key else 'No'}")
        logging.error(f"Error Type: {type(e).__name__}")
        logging.error(f"Error Message: {str(e)}")
        logging.exception("Full traceback:")
        return pd.concat([
            pd.DataFrame(columns=["Name", "Type", "Address", "Link", "error"]),
            pd.DataFrame([{"Name": "", "Type": "", "Address": "", "Link": "", "error": str(e)}])
        ], ignore_index=True)


async def scrape_all(keywords: list[str]) -> pd.DataFrame:
    tasks = [scrape_datacenter_cards_df(k) for k in keywords]
    dfs = await asyncio.gather(*tasks)
    combined = pd.concat(dfs, ignore_index=True)
    deduped = combined.drop_duplicates(subset=["Link"])
    logging.info(f"Combined {len(combined)} rows → {len(deduped)} unique rows")
    return deduped

def run_scrape_and_markdown(keywords: list[str]) -> str:
    df: pd.DataFrame = asyncio.run(scrape_all(keywords))
    return df.to_markdown(index=False)

if __name__ == "__main__":
    keys = ["iran", "pakistan", "united states"]
    print("### Combined Data Center Listings\n")
    print(run_scrape_and_markdown(keys))

