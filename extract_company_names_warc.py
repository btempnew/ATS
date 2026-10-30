"""
Extract Human-Readable Company Names from Common Crawl WARC Records using warcio.

Implements the byte-range random access technique demonstrated in whirlwind-python:
1. Reads sample (warc_filename, warc_record_offset, warc_record_length) from index results.
2. Performs an HTTP Range request directly to https://data.commoncrawl.org.
3. Uses warcio to parse the single WARC record without downloading full multi-GB files.
4. Extracts clean company titles from:
   - Schema.org JSON-LD (hiringOrganization.name)
   - OpenGraph <meta property="og:site_name">
   - <title> tags (e.g., "Careers at Figma", "Jobs at Stripe")
"""

import re
import json
import io
import argparse
from typing import Optional, Dict, Any

import requests
from warcio.archiveiterator import ArchiveIterator


def fetch_warc_record_bytes(warc_filename: str, offset: int, length: int) -> bytes:
    """
    Downloads only the single WARC record using HTTP Range headers,
    as demonstrated in whirlwind-python Tasks 4 and 6.
    """
    url = f"https://data.commoncrawl.org/{warc_filename}"
    headers = {
        "Range": f"bytes={offset}-{offset + length - 1}",
        "User-Agent": "ATS-Company-Researcher/1.0"
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.content


def parse_company_metadata_from_html(html_text: str) -> Dict[str, Any]:
    """
    Extracts official company name and job title from page HTML.
    """
    info = {
        "company_name": None,
        "job_title": None,
        "page_title": None
    }
    
    # 1. Search for JSON-LD schema.org/JobPosting or schema.org/Organization
    json_ld_matches = re.findall(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html_text,
        re.DOTALL | re.IGNORECASE
    )
    for block in json_ld_matches:
        try:
            data = json.loads(block.strip())
            # Sometimes it's a list or dictionary
            items = data if isinstance(data, list) else [data]
            for item in items:
                # JobPosting schema
                if item.get("@type") == "JobPosting":
                    hiring_org = item.get("hiringOrganization")
                    if isinstance(hiring_org, dict) and hiring_org.get("name"):
                        info["company_name"] = hiring_org["name"]
                    elif isinstance(hiring_org, str):
                        info["company_name"] = hiring_org
                    if item.get("title"):
                        info["job_title"] = item.get("title")
                    if info["company_name"]:
                        return info
                # Organization schema
                elif item.get("@type") == "Organization" and item.get("name"):
                    info["company_name"] = item.get("name")
                    return info
        except Exception:
            continue

    # 2. OpenGraph site name: <meta property="og:site_name" content="Company Name">
    og_match = re.search(r'<meta[^>]*property=["\']og:site_name["\'][^>]*content=["\']([^"\']+)["\']', html_text, re.IGNORECASE)
    if og_match:
        info["company_name"] = og_match.group(1).strip()
        return info

    # 3. HTML Title tag fallback: <title>Careers at Acme Corp</title>
    title_match = re.search(r'<title[^>]*>(.*?)</title>', html_text, re.IGNORECASE | re.DOTALL)
    if title_match:
        title = re.sub(r'\s+', ' ', title_match.group(1)).strip()
        info["page_title"] = title
        
        # Common ATS title patterns
        # e.g. "Careers at Stripe", "Jobs at Datadog", "Software Engineer - Figma"
        patterns = [
            r'(?:Careers|Jobs|Openings)\s+(?:at|with)\s+([^|\-–—]+)',
            r'([^|\-–—]+)\s+(?:Careers|Jobs)',
            r'[-–—|]\s*([^|\-–—]+)$'
        ]
        for pat in patterns:
            m = re.search(pat, title, re.IGNORECASE)
            if m:
                extracted = m.group(1).strip()
                if len(extracted) > 1 and not re.search(r'greenhouse|lever|ashby|workday', extracted, re.I):
                    info["company_name"] = extracted
                    break

    return info


def extract_company_from_warc(warc_filename: str, offset: int, length: int) -> Dict[str, Any]:
    """
    Fetches the WARC record slice and parses company information.
    """
    raw_data = fetch_warc_record_bytes(warc_filename, offset, length)
    stream = io.BytesIO(raw_data)
    
    for record in ArchiveIterator(stream):
        if record.rec_type in ("response", "conversion"):
            payload = record.content_stream().read()
            # Decode HTML
            try:
                html_text = payload.decode("utf-8")
            except UnicodeDecodeError:
                html_text = payload.decode("latin1", errors="ignore")
                
            metadata = parse_company_metadata_from_html(html_text)
            metadata["target_uri"] = record.rec_headers.get_header("WARC-Target-URI")
            return metadata
            
    return {"error": "No response record found in slice"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract company name from WARC record")
    parser.add_argument("--filename", required=True, help="warc_filename from index")
    parser.add_argument("--offset", type=int, required=True, help="warc_record_offset from index")
    parser.add_argument("--length", type=int, required=True, help="warc_record_length from index")
    
    args = parser.parse_args()
    print(f"Fetching slice from {args.filename} [offset: {args.offset}, length: {args.length}]...")
    res = extract_company_from_warc(args.filename, args.offset, args.length)
    print(json.dumps(res, indent=2))

