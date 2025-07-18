import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import csv
import logging
import duckdb
import pandas as pd
# Configure logging for better output and debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def scrape_traforama_isp_list_playwright(url: str):
    """
    Scrapes the list of Internet Service Providers by country from Traforama support.
    Uses Playwright to handle dynamically loaded content.
    Extracts country names from h3 tags and providers from p tags, associating them.

    Args:
        url (str): The URL of the Traforama support page.
    """
    browser_headless_mode = True # Set to False for debugging
    browser = None # Initialize browser outside try for finally block

    try:
        async with async_playwright() as p:
            logging.info(f"Launching browser in headless mode: {browser_headless_mode}")
            browser = await p.chromium.launch(headless=browser_headless_mode)
            page = await browser.new_page()

            logging.info(f"Navigating to {url}...")
            # Wait until network is mostly idle AND the specific main content div is visible.
            # This is a robust wait for dynamic content.
            main_content_div_selector = 'div.css-11y878r'
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000) # Load initial DOM
                logging.info("Initial page DOM loaded.")
                # Now wait for the specific content div to appear and be visible
                await page.wait_for_selector(main_content_div_selector, state='visible', timeout=30000)
                logging.info(f"Main content div '{main_content_div_selector}' is visible.")
            except Exception as e:
                logging.error(f"Error navigating to page or waiting for content div: {e}")
                return

            # --- Step 1: Extract the HTML of the main content div ---
            # This will include all dynamically loaded h3 and p tags within it.
            main_content_html = ""
            try:
                logging.info(f"Extracting inner HTML of '{main_content_div_selector}'...")
                main_content_html = await page.inner_html(main_content_div_selector)
                logging.info("Main content HTML extracted successfully.")
            except Exception as e:
                logging.error(f"Error extracting HTML from '{main_content_div_selector}': {e}")
                return

            # --- Step 2: Parse the extracted HTML with BeautifulSoup ---
            logging.info("Parsing extracted HTML with BeautifulSoup...")
            soup = BeautifulSoup(main_content_html, 'html.parser')

            all_extracted_data = []
            current_country = None

            # Find all h3 and p tags directly within the main content div (which is now our soup)
            for element in soup.find_all(['h3', 'p']):
                if element.name == 'h3' and 'graf--h' in element.get('class', []):
                    country_name = element.get_text(strip=True)
                    country_name = country_name.split(' - ')[0].strip() # Clean up potential extra info
                    current_country = country_name
                    # logging.info(f"Processing Country: {current_country}") # Too verbose for full run
                elif element.name == 'p' and 'graf--p' in element.get('class', []):
                    if current_country:
                        providers_text = element.get_text(strip=True)
                        all_extracted_data.append({
                            'Country': current_country,
                            'Providers': providers_text
                        })
                        current_country = None # Reset for the next country-provider pair
                    else:
                        logging.warning(f"Found provider paragraph '{element.get_text(strip=True)}' without a preceding country. Skipping.")
                # We ignore other tags or tags with different classes

            if not all_extracted_data:
                logging.warning("No country and provider data was extracted. Check selectors or page structure.")
                return

            logging.info(f"Successfully extracted {len(all_extracted_data)} country-provider pairs.")

            # --- Step 3: Save data to CSV ---
            output_filename = 'traforama_isp_list.csv'
            try:
                csv_headers = ['Country', 'Providers']
                with open(output_filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=csv_headers)
                    writer.writeheader()
                    writer.writerows(all_extracted_data)
                logging.info(f"Data saved successfully to {output_filename}")
            except Exception as e:
                logging.error(f"Error saving data to CSV file '{output_filename}': {e}")

    except Exception as main_exc:
        logging.critical(f"An unhandled error occurred during scraping: {main_exc}", exc_info=True)
    finally:
        if browser:
            await browser.close()
            logging.info("Browser closed.")

# Define the target URL
target_url = "https://support.traforama.com/en/articles/list-of-internet-service-providers-by-country"

con = duckdb.connect('bryan.db')
# Create the table (wrap names with spaces in double quotes)
con.execute("""
    CREATE OR REPLACE TABLE traforama_isp_list (
        Country TEXT,
        Providers TEXT
    )
""")

# Load from CSV into table
con.execute("""
    COPY traforama_isp_list 
    FROM 'traforama_isp_list.csv' 
    (FORMAT CSV, HEADER)
""")

con.close()
logging.info("Data saved successfully to traforama_isp_list.db")

if __name__ == "__main__":
    asyncio.run(scrape_traforama_isp_list_playwright(target_url))

#Good - could be cleaned but not sure how neccesary ot is for Rag