#!/usr/bin/env python3
"""
Greenhouse Live Job Extractor.

Extracts all live open jobs with complete metadata (Title, Location, Apply URL,
Department, Office, Description, Date) for any Greenhouse company or across all
verified companies in `greenhouse_companies_us_active.txt`.
"""

import sys
import json
import asyncio
import argparse
import sqlite3
import html
import os
from typing import List, Dict, Any, Optional

from ats_diacovery_2 import AsyncHTTPClient, is_us_location


def clean_html_description(raw_html: str) -> str:
    """Unescapes and cleans HTML description if present."""
    if not raw_html:
        return ""
    unescaped = html.unescape(raw_html)
    return unescaped


async def fetch_company_jobs(
    http: AsyncHTTPClient,
    slug: str,
    include_full_content: bool = True
) -> List[Dict[str, Any]]:
    """
    Fetches all live jobs for a single Greenhouse company slug.
    Adding `?content=true` pulls the full HTML job description in a single call.
    """
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    if include_full_content:
        url += "?content=true"

    status, raw_body, _ = await http.request("GET", url)
    if status != 200 or not raw_body.strip():
        return []

    try:
        data = json.loads(raw_body)
    except Exception:
        return []

    company_name = data.get("name") or slug.title()
    jobs = data.get("jobs") or []
    extracted = []

    for j in jobs:
        loc_obj = j.get("location") or {}
        loc_name = loc_obj.get("name", "") if isinstance(loc_obj, dict) else str(loc_obj)

        depts = [d.get("name") for d in (j.get("departments") or []) if d.get("name")]
        offices = [o.get("name") for o in (j.get("offices") or []) if o.get("name")]

        extracted.append({
            "job_id": str(j.get("id")),
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("title", ""),
            "location": loc_name,
            "is_us": is_us_location(loc_name),
            "apply_url": j.get("absolute_url", ""),
            "department": ", ".join(depts),
            "office": ", ".join(offices),
            "updated_at": j.get("updated_at", ""),
            "first_published": j.get("first_published", ""),
            "content": clean_html_description(j.get("content", "")) if include_full_content else "",
        })

    return extracted


async def extract_jobs_batch(
    slugs: List[str],
    output_db: str = "greenhouse_jobs_master.db",
    include_content: bool = False,
    concurrency: int = 40,
    only_us: bool = False,
):
    print(f"Connecting to {len(slugs)} company boards (concurrency={concurrency}, full_content={include_content})...")
    conn = sqlite3.connect(output_db)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS live_jobs (
            job_id TEXT PRIMARY KEY,
            company_name TEXT,
            company_slug TEXT,
            title TEXT,
            location TEXT,
            is_us INTEGER,
            apply_url TEXT,
            department TEXT,
            office TEXT,
            updated_at TEXT,
            first_published TEXT,
            content TEXT
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON live_jobs(company_slug);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_us ON live_jobs(is_us);")
    conn.commit()

    total_jobs_saved = 0
    async with AsyncHTTPClient(concurrency=concurrency, per_host_concurrency=10) as http:
        # Process in batches of 200 companies
        batch_size = 200
        for i in range(0, len(slugs), batch_size):
            batch = slugs[i : i + batch_size]
            results = await asyncio.gather(*(fetch_company_jobs(http, s, include_content) for s in batch))

            to_insert = []
            for company_jobs in results:
                for j in company_jobs:
                    if only_us and not j["is_us"]:
                        continue
                    to_insert.append((
                        j["job_id"], j["company_name"], j["company_slug"], j["title"],
                        j["location"], 1 if j["is_us"] else 0, j["apply_url"],
                        j["department"], j["office"], j["updated_at"], j["first_published"],
                        j["content"]
                    ))

            if to_insert:
                cur.executemany("""
                    INSERT OR REPLACE INTO live_jobs (
                        job_id, company_name, company_slug, title, location, is_us,
                        apply_url, department, office, updated_at, first_published, content
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, to_insert)
                conn.commit()
                total_jobs_saved += len(to_insert)

            print(f"[{min(i + batch_size, len(slugs))}/{len(slugs)}] Companies scanned | Total Live Jobs Saved: {total_jobs_saved:,}")

    conn.close()
    print(f"\nFinished! Total live jobs saved in '{output_db}': {total_jobs_saved:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract live Greenhouse job postings")
    parser.add_argument("--slug", help="Single company slug (e.g. figma, stripe)")
    parser.add_argument("--input-file", default="greenhouse_companies_us_active.txt", help="Input file of company names/slugs")
    parser.add_argument("--output-db", default="greenhouse_jobs_master.db", help="Output SQLite database")
    parser.add_argument("--full-content", action="store_true", help="Include full HTML job description & requirements")
    parser.add_argument("--only-us", action="store_true", help="Only save US jobs")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of companies (0 for all)")
    parser.add_argument("--concurrency", type=int, default=40)

    args = parser.parse_args()

    if args.slug:
        async def single_test():
            async with AsyncHTTPClient(concurrency=10) as http:
                jobs = await fetch_company_jobs(http, args.slug, include_full_content=args.full_content)
                print(f"Extracted {len(jobs)} live jobs for '{args.slug}':")
                for j in jobs[:5]:
                    print(f"  • {j['title']} ({j['location']})")
                    print(f"    Apply: {j['apply_url']}")
        asyncio.run(single_test())
    else:
        if not os.path.exists(args.input_file):
            print(f"Input file '{args.input_file}' not found.")
            sys.exit(1)

        with open(args.input_file, "r", encoding="utf-8") as f:
            slug_list = [line.strip().lower().replace(" ", "-") for line in f if line.strip()]

        if args.limit > 0:
            slug_list = slug_list[:args.limit]

        asyncio.run(
            extract_jobs_batch(
                slugs=slug_list,
                output_db=args.output_db,
                include_content=args.full_content,
                concurrency=args.concurrency,
                only_us=args.only_us,
            )
        )

