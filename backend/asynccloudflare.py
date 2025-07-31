#full async hope!


import os
import logging
import httpx  # <-- Changed from 'requests' to 'httpx'
import json
import asyncio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CF_API_TOKEN = "JUabJp8XMB3gnrm7qZ3ldrvWkoEDme8oWIHRLsuB"  # Ideally from env

HEADERS = {
    "Authorization": f"Bearer {CF_API_TOKEN}",
    "Content-Type": "application/json"
}

# The set of endpoints we want to render
ENDPOINTS = {
    "device_type":       "/http/summary/device_type",
    "ip_version":        "/http/summary/ip_version",
    "http_version":      "/http/summary/http_version",
    "tls_version":       "/http/summary/tls_version",
    "os":                "/http/summary/os",

    # Bump limit or remove entirely if you need more than 20
    "domain_popularity": "/ranking/top",
}

async def fetch_and_format_markdown(
    country: str = "",
    date_range: str = "30d"
) -> str:
    """
    Fetches each metric in ENDPOINTS asynchronously and builds one Markdown report string.
    """
    md_lines = [
        f"# Cloudflare Radar Summary (Country: {country or 'Global'}, Range: {date_range})",
        ""
    ]

    async with httpx.AsyncClient(timeout=30.0) as client:
        for metric, path in ENDPOINTS.items():
            url = f"https://api.cloudflare.com/client/v4/radar{path}"
            params = {"format": "json", "dateRange": date_range}
            if country:
                params["location"] = country

            if metric == "domain_popularity":
                params["name"] = "top"
                params["limit"] = 20
            
            

            logger.info(f"Fetching {metric} for country {country}...")
            try:
                resp = await client.get(url, headers=HEADERS, params=params)
                resp.raise_for_status()
                body = resp.json()

                if not body.get("success"):
                    logger.error(f"  ↳ {metric}: API error {body.get('errors')}")
                    continue

                data = body["result"]
                title = metric.replace("_", " ").title()
                md_lines.append(f"## {title}")
                md_lines.append("")
                logging.info(f"DATA TYPES: {data}")
                # 1) summary_0 as before...
                if "summary_0" in data:
                    summary = data["summary_0"]
                    if isinstance(summary, list):
                        md_lines.append("| Category | Share   | Requests |")
                        md_lines.append("|---|---:|---:|")
                        for item in summary:
                            name = item.get("name", "")
                            share = item.get("share", 0.0) * 100 \
                                if isinstance(item.get("share"), (int,float)) else item.get("share")
                            reqs  = item.get("requests", 0)
                            share_str = f"{share:.2f}%" if isinstance(share, float) else str(share)
                            md_lines.append(f"| {name} | {share_str:>7s} | {reqs:>8d} |")
                        md_lines.append("")
                    elif isinstance(summary, dict):
                        md_lines.append("| Category | Value |")
                        md_lines.append("|---|---:|")
                        for cat, val in summary.items():
                            md_lines.append(f"| {cat} | {val} |")
                        md_lines.append("")
                    else:
                        md_lines.append("```json")
                        md_lines.append(json.dumps(summary, indent=2))
                        md_lines.append("```")
                        md_lines.append("")

                # 2) CLEAN domain_popularity formatting
                elif metric == "domain_popularity":
                    top_list = data.get("top") or data.get("top_0")
                    if isinstance(top_list, list):
                        md_lines.append("| Rank | Domain                | Categories                         |")
                        md_lines.append("|---:|:----------------------|:-----------------------------------|")
                        for item in top_list:
                            rank   = item.get("rank", "")
                            domain = item.get("domain", "")
                            cats   = item.get("categories", [])
                            names  = ", ".join(c.get("name","") for c in cats)
                            md_lines.append(f"| {rank:>2d} | {domain:22s} | {names:35s} |")
                        md_lines.append("")
                    else:
                        logger.warning(f"  ↳ {metric}: no 'top' list found, falling back to raw JSON")
                        md_lines.append("```json")
                        md_lines.append(json.dumps(data, indent=2))
                        md_lines.append("```")
                        md_lines.append("")

                # 3) any other "top" endpoints get a raw dump
                elif "top" in data:
                    logger.warning(f"  ↳ {metric}: unexpected 'top' shape; dumping raw JSON.")
                    md_lines.append("```json")
                    md_lines.append(json.dumps(data["top"], indent=2))
                    md_lines.append("```")
                    md_lines.append("")

                else:
                    # fallback for unknown shapes
                    logger.warning(f"  ↳ {metric}: Unknown data shape; dumping raw JSON.")
                    md_lines.append("```json")
                    md_lines.append(json.dumps(data, indent=2))
                    md_lines.append("```")
                    md_lines.append("")

            except httpx.HTTPStatusError as e:
                logger.warning(f"  ↳ {metric}: HTTP {e.response.status_code} – skipping")
            except httpx.RequestError as e:
                logger.error(f"  ↳ {metric}: Request error: {e}")
            except json.JSONDecodeError as e:
                logger.error(f"  ↳ {metric}: JSON decode error: {e}")
            except Exception:
                logger.exception(f"  ↳ {metric}: unexpected failure")

    return "\n".join(md_lines)


# --- Example usage ---
if __name__ == "__main__":
    async def main():
        md_report = await fetch_and_format_markdown(country="IN", date_range="30d")
        print(md_report)
        with open("radar_report_IN_30d.md", "w", encoding="utf-8") as f:
            f.write(md_report)

    asyncio.run(main())
