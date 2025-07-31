# Comms Explorer Automated Pipeline

## TL;DR
Comms Explorer is an adaptle automatic pipeline to turn natural language users queries and output detailed and accurate information about communication sites, domain providers, internet users and service providers in a digestable and organized manner leveraging a combination of webscraping, pre-downloaded SQL databases and Large Language Models.

## Overview
Comms Explorer automatically fetches data from verified open-source intelligence sites as well as pre-scraped SQL tables from various trustworth websites. This pipeline will fetch data about the availablitliy of social media sites (eg. Whatsapp, Instagram, Signal), datacenters, Internet Service Providers, Mobile Network Operators, Mobile Carrier Codes, and domain and web traffic information. All fetched data is sent to state-of-the-art Large Language Models (LLMS) which solidify this information into well-organized and structured tables for easy viewing. The entire automation process saves the need for costly human research and human browsing, and can even improve accruacy.

## Project Structure
- **`main.py`**: Entry point for pipeline execution.
- **`index.html`**: Frontend framework to display information/calls from main.py and the rest of the backend.
- **`backend/`**: Folder with all the backend web-scraping, SQL handling and LLM calls.
- **`.gitignore`**: Certain files/passwords/keys to ignore.
- **`Dockerfile`**: Defines global configurations to containerize and run on the cloud.
- **`requirements.txt`**: List of all the dependencies needed to run the pipeline.

```
├── __pycache__
├── .gitignore
├── DockerFile
├── README.md
├── index.html
├── main.py
├── requirements.txt
├── backend
│   ├── __init__.py
│   ├── asynccloudflare.py
│   ├── broadsqlasync.py
│   ├── bryan.db
│   ├── country_code_converter.py
│   ├── datacenter.py
│   ├── final_truly_async.py
│   ├── mcc.py
│   ├── mideye.py
│   ├── ooni.py
│   ├── traforama.py


```

## Installation


### Setup

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/roshan-h-shah/comms_explorer.git
   ```

2. **Install Dependencies:**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   pip install playwright
   playwright install --with-deps
   playwright install chromium
   ```

3. **Set Up Environment Variables:**
   Create a `.env` file:
   ```bash
   OPENAI_API_KEY=your_openai_key
   ```

4. **Build and Run Services with Uvicorn:**
   ```bash
   uvicorn main:app --reload
   ```
