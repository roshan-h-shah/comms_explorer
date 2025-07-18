# radar_markdown.py

import os
import logging
import requests
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CF_API_TOKEN = "JUabJp8XMB3gnrm7qZ3ldrvWkoEDme8oWIHRLsuB"

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

 
  
    "domain_popularity": "/ranking/top?name=top&limit=20",

}

def fetch_and_format_markdown(
    country: str = "",
    date_range: str = "30d"
) -> str:
    """
    Fetches each metric in ENDPOINTS and builds one Markdown report string.
    """
    md_lines = [f"# Cloudflare Radar Summary (Country: {country or 'Global'}, Range: {date_range})", ""]
    
    for metric, path in ENDPOINTS.items():
        url = f"https://api.cloudflare.com/client/v4/radar{path}"
        params = {"format": "json", "dateRange": date_range}
        if country:
            params["location"] = country #should return proper results -> country is not a valid endpoint
            # FIXED NOW!

        logger.info(f"Fetching {metric} …")
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if resp.status_code == 400:
            logger.warning(f"  ↳ {metric}: 400 Bad Request – skipping")
            continue
        resp.raise_for_status()
        body = resp.json()
        if not body.get("success"):
            logger.error(f"  ↳ {metric}: API error {body.get('errors')}")
            continue

        data = body["result"]
        title = metric.replace("_", " ").title()
        md_lines.append(f"## {title}")
        md_lines.append("")

        # 1) summary_0 as list-of-dicts or dict-of-values
        if "summary_0" in data:
            summary = data["summary_0"]
            # list-of-dicts
            if isinstance(summary, list):
                md_lines.append("| Category | Share   | Requests |")
                md_lines.append("|---|---:|---:|")
                for item in summary:
                    name = item.get("name", "")
                    share = item.get("share", 0.0) * 100 if isinstance(item.get("share"), (int,float)) else item.get("share")
                    reqs  = item.get("requests", "")
                    # format share as percent
                    share_str = f"{share:.2f}%" if isinstance(share, float) else str(share)
                    md_lines.append(f"| {name} | {share_str:>7s} | {reqs:>8d} |")
                md_lines.append("")

            # dict-of-values
            elif isinstance(summary, dict):
                md_lines.append("| Category | Value |")
                md_lines.append("|---|---:|")
                for cat, val in summary.items():
                    md_lines.append(f"| {cat} | {val} |")
                md_lines.append("")
            else:
                # fallback to JSON
                md_lines.append("```json")
                md_lines.append(json.dumps(summary, indent=2))
                md_lines.append("```")
                md_lines.append("")

        # 2) domain_popularity / top case
        elif "top" in data:
            md_lines.append("| Rank | Domain                | Categories                         |")
            md_lines.append("|---:|:----------------------|:-----------------------------------|")
            for item in data["top"]:
                rank    = item.get("rank", "")
                domain  = item.get("domain", "")
                cats    = item.get("categories", [])
                # join category names
                names   = ", ".join(c.get("name","") for c in cats)
                md_lines.append(f"| {rank:>2d} | {domain:22s} | {names:35s} |")
            md_lines.append("")

        else:
            # unknown shape: dump raw JSON
            md_lines.append("```json")
            md_lines.append(json.dumps(data, indent=2))
            md_lines.append("```")
            md_lines.append("")
    print(f"CLOUDFLARE FINAL RETURN {"\n".join(md_lines)}")
    return "\n".join(md_lines)


# --- Example usage ---
if __name__ == "__main__":
    md_report = fetch_and_format_markdown(country="IN", date_range="30d")
    print(md_report)
    # optionally save to .md
    with open("radar_report_IN_30d.md", "w", encoding="utf-8") as f:
        f.write(md_report)

