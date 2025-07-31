'''
Scrapes tables from Mideye website into SQL tables - makes it faster instead of web-scraping everything at inference
'''
import requests
from bs4 import BeautifulSoup
import csv
import logging
import pandas as pd
import duckdb
# Configure logging for better output and debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_mideye_mobile_networks(url: str):
    """
    Scrapes the mobile network list table from mideye.com.
    Assumes the table is fully loaded on initial page load,
    has no ID, is the only table on the page, and resides within
    a div with class 'entry-content'. It also assumes no thead/tfoot
    and the first tbody row serves as headers.

    Args:
        url (str): The URL of the page containing the table.
    """
    logging.info(f"Attempting to fetch content from: {url}")
    try:
        # Use requests to get the HTML content of the page
        # Added a User-Agent header to mimic a real browser, which can help prevent some blocks
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=15) # Added timeout for safety
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        logging.info("Successfully fetched page content.")

    except requests.exceptions.HTTPError as errh:
        logging.error(f"HTTP Error: {errh} - Status Code: {response.status_code}")
        return
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
        return
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
        return
    except requests.exceptions.RequestException as err:
        logging.error(f"An unexpected error occurred: {err}")
        return

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    logging.info("HTML content parsed with BeautifulSoup.")

    # --- Step 1: Locate the parent div with class 'entry-content' ---
    entry_content_div = soup.find('div', class_='entry-content')
    if not entry_content_div:
        logging.error("Error: Div with class 'entry-content' not found.")
        return

    logging.info("Found 'entry-content' div.")

    # --- Step 2: Locate the table within the 'entry-content' div ---
    # Since it's the only table, we can directly find it within the parent div.
    table = entry_content_div.find('table')
    if not table:
        logging.error("Error: Table not found within the 'entry-content' div.")
        return

    logging.info("Found the data table.")

    # --- Step 3: Extract headers and data from tbody ---
    # Since there's no thead, we assume the first <tr> in tbody is the header.
    all_data = []
    headers = []

    tbody = table.find('tbody')
    if not tbody:
        logging.error("Error: tbody element not found within the table.")
        return

    rows = tbody.find_all('tr')
    if not rows:
        logging.warning("No rows found in the table tbody.")
        return

    logging.info(f"Found {len(rows)} rows in the table body.")

    # Extract headers from the first row
    first_row_cells = rows[0].find_all(['td', 'th']) # Use both td/th in case headers are td
    if first_row_cells:
        headers = [cell.get_text(strip=True) for cell in first_row_cells]
        all_data.append(headers)
        logging.info(f"Extracted headers (from first row of tbody): {headers}")
    else:
        logging.warning("Could not extract headers from the first row.")

    # Extract data from subsequent rows
    # Start from index 1 because index 0 (the first row) was treated as headers
    for i, row in enumerate(rows[1:]):
        cells = row.find_all('td') # Data rows typically only have <td>
        row_data = [cell.get_text(strip=True) for cell in cells]
        if row_data:
            all_data.append(row_data)
            # Log progress for large tables (e.g., every 500 rows)
            if (i + 1) % 500 == 0:
                logging.info(f"Processed {i + 1} data rows...")
    return all_data

output_filename = 'mideye_mobile_network_list.csv'
def clean_data(input:list): #could look at polars instead of pd for speed improvement.
    input = input[2:]
    df = pd.DataFrame(input, columns=["Country","Operator", "Network Code", "Display Text"])
    for i in range(len(df)):
        if df.loc[i,"Country"] == "":
            df.loc[i, "Country"] = df.loc[i-1,"Country"]

    df.to_csv(output_filename, index=False)
    print(df.head(10))
# Define the target URL
target_url = "https://mideye.com/authentication-service/global-coverage/mobile-network-list/"

con = duckdb.connect('bryan.db')
con.execute("""
    CREATE OR REPLACE TABLE mideye_mobile_network_list (
        "Country" TEXT,
        "Operator" TEXT,
        "Network Code" TEXT,
        "Display Text" TEXT
    )
""")

# Load from CSV into table
con.execute("""
    COPY mideye_mobile_network_list 
    FROM 'mideye_mobile_network_list.csv' 
    (FORMAT CSV, HEADER)
""")

con.close()
logging.info("Data saved successfully to mideye_mobile_network_list.db")

if __name__ == "__main__":
    im1 = scrape_mideye_mobile_networks(target_url)
    clean_data(im1)