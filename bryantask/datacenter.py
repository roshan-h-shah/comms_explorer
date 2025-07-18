
#Code to avoid backoff errors:
# KEEPS GETTING URL BLOCKED!
#basically have to change vpn on off/change server every once in a while
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
import pandas as pd
import logging
from urllib.parse import urljoin
import os
import socket

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_public_ip():
    try:
        import requests
        return requests.get('https://api.ipify.org').text
    except Exception:
        return 'unknown'

async def scrape_datacenter_cards_df(keyword: str) -> pd.DataFrame:
    web_format_keyword = keyword.replace(" ", "%20")
    
    url = f"https://www.datacenters.com/locations?query={web_format_keyword}"
    browser = None

    # Log environment and public IP
    logging.info(f"[datacenter] ENV: {os.environ.get('ENV', 'local')}, public IP: {get_public_ip()}")

    try:
        async with async_playwright() as p:
            proxy = os.environ.get('DATACENTER_PROXY')
            launch_args = {"headless": True}
            if proxy:
                launch_args["proxy"] = {"server": proxy}
            browser = await p.chromium.launch(**launch_args)
            page = await browser.new_page()
            # try to seem human like - helps a little
            await page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Connection": "keep-alive",
            })
            
            # --- Block non-critical resources to speed up load ---
            await page.route("**/*", lambda route, req:
                             route.abort()
                             if req.resource_type in {"image", "stylesheet", "font"}
                             else route.continue_())

            logging.info(f"[{keyword}] Loading {url}")
            
            # --- Retry loop around goto ---
            for attempt in range(1, 4):
                try:
                    await page.goto(
                        url,
                        wait_until="domcontentloaded",   # fire as soon as HTML is loaded
                        timeout=15_000                    # 15 seconds
                    )
                    break
                except PlaywrightTimeoutError:
                    logging.warning(f"[{keyword}] goto timeout, attempt {attempt}/3")
                    if attempt < 3:
                        await asyncio.sleep(2 ** attempt)  # back off: 2s, then 4s
                    else:
                        raise
            
            # --- Wait for your card selector, not full networkidle ---
            card_sel = (
                "a.flex.flex-col.gap-2.rounded.border.border-gray-100."
                "p-2.hover\\:border-teal-300.hover\\:shadow-lg.hover\\:shadow-teal-600\\/40"
            )
            await page.wait_for_selector(card_sel, state="visible", timeout=5_000)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

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

            # filter by country
            df["_country"] = df["Address"].apply(lambda a: a.split(",")[-1].strip().lower())
            df.loc[df["_country"] == "usa", "_country"] = "united states"
            df = df[df["_country"] == keyword.lower()].drop(columns=["_country"])
            logging.info(f"[{keyword}] {len(df)} rows remain after filtering to country == '{keyword}'")

            return df

    except Exception as e:
        logging.exception(f"Error scraping {url}")
        return pd.DataFrame(columns=["Name", "Type", "Address", "Link", "error"]).append(
            {"Name": "", "Type": "", "Address": "", "Link": "", "error": str(e)}, ignore_index=True
        )

    finally:
        if browser:
            await browser.close()
            logging.info("Browser closed.")


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

