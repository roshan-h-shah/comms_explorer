'''
Scrapes tables from MCC website into SQL tables - makes it faster instead of web-scraping everything at inference
'''
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import logging
import duckdb
import pandas as pd

# Configure logging for better troubleshooting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def scrape_mcc_mnc_table(url: str):
    """
    Scrapes the full MCC-MNC table from the given URL by dynamically
    injecting and selecting an "All" option in the DataTables dropdown.

    Args:
        url (str): The URL of the page containing the DataTables.

    Returns:
        list[list[str]]: A list of lists representing the table data,
                         with the first sublist being headers.
                         Returns an empty list if scraping fails.
    """
    browser_headless_mode = True # Set to False for debugging (will open a browser window)
    browser = None # Initialize browser outside try for finally block
    all_data = [] # Initialize all_data here to ensure it's always defined

    try:
        async with async_playwright() as p:
            logging.info(f"Launching browser in headless mode: {browser_headless_mode}")
            browser = await p.chromium.launch(headless=browser_headless_mode)
            page = await browser.new_page()

            logging.info(f"Navigating to {url}...")
            await page.goto(url, wait_until="networkidle", timeout=30000)
            logging.info("Page loaded successfully.")

            # --- Step 1: Locate the <select> element for table length ---
            select_selector = 'select[name="mncmccTable_length"]'
            try:
                await page.wait_for_selector(select_selector, timeout=15000)
                logging.info(f"Select element found using selector: '{select_selector}'")
            except Exception as e:
                logging.error(f"Error: Select element with selector '{select_selector}' not found within timeout. Details: {e}", exc_info=True)
                return [] # Return empty list on failure

            # --- Step 2: Dynamically Inject the "All" Option ---
            desired_value_for_all = '5000'
            desired_text_for_all = 'All (2080 Entries)' # Updated to reflect actual entry count

            logging.info(f"Attempting to inject option value='{desired_value_for_all}' into the dropdown...")
            try:
                await page.evaluate("""
                    (args) => {
                        const selectElement = document.querySelector(args.selector);
                        if (selectElement) {
                            if (!selectElement.querySelector(`option[value="${args.value}"]`)) {
                                const newOption = document.createElement('option');
                                newOption.value = args.value;
                                newOption.textContent = args.text;
                                selectElement.appendChild(newOption);
                            }
                            selectElement.value = args.value; // Set the new option as selected
                            selectElement.dispatchEvent(new Event('change', { bubbles: true })); // Trigger change
                        }
                    }
                """, { 'selector': select_selector, 'value': desired_value_for_all, 'text': desired_text_for_all })
                logging.info("Option injected successfully and change event dispatched.")
            except Exception as e:
                logging.error(f"Error injecting option or dispatching change event: {e}", exc_info=True)
                return [] # Return empty list on failure

            # --- Step 3: Select the newly added "All" option (redundant but good for robustness) ---
            # This step might be redundant if page.evaluate already set the value and dispatched change,
            # but keeping it provides a fallback.
            logging.info(f"Attempting to select injected value '{desired_value_for_all}' from the dropdown...")
            try:
                await page.select_option(select_selector, value=desired_value_for_all)
                logging.info(f"Selected '{desired_value_for_all}'. DataTables should now be loading.")
            except Exception as e:
                logging.error(f"Error selecting option '{desired_value_for_all}': {e}", exc_info=True)
                return [] # Return empty list on failure

            # --- Step 4: Wait for the table to fully load all data ---
            datatables_info_selector = '#mncmccTable_info'
            expected_total_entries_text_part = "2,080 entries"

            logging.info(f"Waiting for DataTables info to show '{expected_total_entries_text_part}'...")
            try:
                await page.wait_for_function(
                    f"document.querySelector('{datatables_info_selector}').innerText.includes('{expected_total_entries_text_part}')",
                    timeout=60000 # Increased timeout to 1 minute for data loading
                )
                logging.info(f"DataTables info updated, indicating {expected_total_entries_text_part} are loaded.")
            except Exception as e:
                logging.warning(f"DataTables info did not show '{expected_total_entries_text_part}' within timeout ({e}). Data might be incomplete.", exc_info=True)
                try:
                    current_info_text = await page.evaluate(f"document.querySelector('{datatables_info_selector}').innerText")
                    logging.warning(f"Current info text: {current_info_text}")
                except Exception as inner_e:
                    logging.warning(f"Could not retrieve current info text for debugging: {inner_e}", exc_info=True)
                # Proceed even if timeout occurs, to extract whatever is available.

            # --- Step 5: Extract the HTML of the TABLE WRAPPER ---
            table_wrapper_selector = '#mncmccTable_wrapper'
            page_html_content = ""
            try:
                logging.info(f"Extracting HTML of the table wrapper: '{table_wrapper_selector}'...")
                page_html_content = await page.inner_html(table_wrapper_selector)
                logging.info("Table wrapper HTML extracted successfully.")
            except Exception as e:
                logging.error(f"Error extracting HTML from '{table_wrapper_selector}': {e}", exc_info=True)
                return [] # Return empty list on failure

            # --- Step 6: Parse the extracted HTML with BeautifulSoup ---
            logging.info("Parsing extracted HTML with BeautifulSoup to find the table...")
            soup = BeautifulSoup(page_html_content, 'html.parser')
            table = soup.find('table', id='mncmccTable')
            if not table:
                logging.error(f"Error: Table with ID 'mncmccTable' not found in the extracted HTML content from '{table_wrapper_selector}'.")
                logging.error("This indicates either the table HTML was not fully loaded/rendered, or the selector is incorrect.")
                return [] # Return empty list on failure

            # Extract headers from thead
            headers = []
            thead = table.find('thead')
            if thead:
                header_row = thead.find('tr')
                if header_row:
                    headers = [th.get_text(strip=True) for th in header_row.find_all('th')]
                if headers:
                    # all_data.append(headers) # Removed: pandas DataFrame will handle header based on `columns` argument
                    logging.info(f"Found headers: {headers}")
                else:
                    logging.warning("No headers found within thead.")
            else:
                logging.warning("No thead element found in the table.")

            # Extract data rows from tbody
            data_rows_count = 0
            tbody = table.find('tbody')
            extracted_rows = [] # Store rows temporarily for DataFrame creation
            if tbody:
                rows = tbody.find_all('tr')
                data_rows_count = len(rows)
                logging.info(f"Found {data_rows_count} rows in the extracted table body (from BeautifulSoup).")
                for row in rows:
                    cells = row.find_all('td')
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    if row_data:
                        extracted_rows.append(row_data) # Append only data rows
            else:
                logging.error("No tbody element found in the table. Data extraction will be incomplete.")

            # Combine headers and extracted_rows correctly
            if headers and extracted_rows:
                all_data.append(headers) # Add headers as the first element for a consistent list format
                all_data.extend(extracted_rows) # Add all extracted data rows
            elif extracted_rows: # If no headers were found but data rows exist
                logging.warning("Data rows extracted but no headers found. CSV export might need manual header definition.")
                all_data.extend(extracted_rows)


            extracted_data_rows = len(all_data) - (1 if headers else 0) # Adjust count if headers were added
            logging.info(f"Total extracted data rows (excluding header): {extracted_data_rows}. Total list items: {len(all_data)}")

            if extracted_data_rows < 2080:
                logging.warning(f"Warning: Less than the expected 2080 data rows were extracted ({extracted_data_rows}). Data might be incomplete.")

            return all_data # Return the collected data

    except Exception as main_exc:
        logging.critical(f"An unhandled error occurred during scraping: {main_exc}", exc_info=True)
        return [] # Return empty list on critical failure
    finally:
        if browser:
            await browser.close()
            logging.info("Browser closed.")

async def main():
    target_url = "https://mcc-mnc.com/"
    table_data = await scrape_mcc_mnc_table(target_url)

    if not table_data:
        logging.error("Scraping failed or returned no data. Exiting without saving to CSV or DB.")
        return

    # --- Step 7: Save data to CSV ---
    output_csv_filename = 'mcc_mnc_table_data.csv'
    try:
        if table_data and len(table_data) > 1: # Ensure there's at least a header and one data row
            headers = table_data[0]
            data_rows = table_data[1:]

            df = pd.DataFrame(data_rows, columns=headers)

            # Rename columns as needed for better readability in CSV/DB
            df = df.rename(columns={
                "MCC": 'Mobile Country Code',
                "MNC": 'Mobile Network Code',
                "ISO": "ISO Country Code",
                "Country": 'Country',
                "Country Code": 'Country Code',
                "Network": 'Network Operator'
            })
            
            # Example of a data cleaning step (uncomment if you need it)
            df['Country'] = df['Country'].replace("United States of America", "United States")

            df.to_csv(output_csv_filename, index=False, header=True, encoding='utf-8')
            logging.info(f"Data saved successfully to {output_csv_filename}")
        else:
            logging.warning("No complete data (headers + rows) available to save to CSV.")
    except Exception as e:
        logging.error(f"Error saving data to CSV file '{output_csv_filename}': {e}", exc_info=True)
        # If CSV saving fails, we might not want to proceed with DB operations
        return

    # --- Step 8: Save data to DuckDB ---
    duckdb_filename = 'bryan.db'
    con = None # Initialize con outside try for finally block
    try:
        con = duckdb.connect(duckdb_filename)

        # Create the table with column names matching the RENAMED CSV headers
        con.execute("""
            CREATE OR REPLACE TABLE mcc_mnc_table (
                "Mobile Country Code" TEXT,
                "Mobile Network Code" TEXT,
                "ISO Country Code" TEXT,
                Country TEXT,
                "Country Code" TEXT,
                "Network Operator" TEXT
            )
        """)
        logging.info("DuckDB table 'mcc_mnc_table' created or replaced successfully.")

        # Load from CSV into table using the HEADER option, assuming CSV now has a header
        con.execute(f"""
            COPY mcc_mnc_table
            FROM '{output_csv_filename}'
            (FORMAT CSV, HEADER TRUE)
        """)
        logging.info(f"Data successfully copied from '{output_csv_filename}' to DuckDB table 'mcc_mnc_table'.")

    except Exception as e:
        logging.error(f"Error saving data to DuckDB '{duckdb_filename}': {e}", exc_info=True)
    finally:
        if con:
            con.close()
            logging.info("DuckDB connection closed.")

if __name__ == "__main__":
    asyncio.run(main())