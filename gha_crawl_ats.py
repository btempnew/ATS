"""
GitHub Actions Runner: Common Crawl ATS Company Extractor (DuckDB & Whirlwind)

Designed specifically for CI/CD runners (GitHub Actions):
- 100% Free (no cloud accounts or paid APIs needed)
- Memory efficient (< 1 GB RAM usage)
- Resilient to network throttling with exponential backoff
- Prunes remote Parquet shards and extracts ATS companies
- Appends new discoveries directly to existing repo lists
"""

import os
import sys
import time
import gzip
import argparse
import urllib.request
from pathlib import Path
from typing import List, Set, Dict, Optional

import duckdb
from ats_definitions import ATS_REGISTRY


def setup_duckdb(ext_dir: str = "/tmp/duckdb_ext") -> duckdb.DuckDBPyConnection:
    os.makedirs(ext_dir, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET extension_directory = '{ext_dir}'")
    try:
        con.execute("INSTALL httpfs;")
    except Exception:
        pass
    con.execute("LOAD httpfs;")
    con.execute("SET threads = 2;")
    con.execute("SET http_retries = 20;")
    con.execute("SET http_keep_alive = true;")
    return con


def get_crawl_shards(crawl: str, paths_file: Optional[str] = None) -> List[str]:
    """
    Retrieves the list of Parquet shard URLs for a Common Crawl index.
    If a local paths_file exists on disk, reads it.
    Otherwise, automatically downloads and parses `cc-index-table.paths.gz`
    directly from Common Crawl over HTTPS without requiring any local files.
    """
    if paths_file and os.path.exists(paths_file):
        print(f"Reading shard paths from local file: {paths_file}")
        with gzip.open(paths_file, "rt", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        if lines and lines[0].startswith("http"):
            return lines
        elif lines and lines[0].startswith("cc-index/"):
            return [f"https://data.commoncrawl.org/{l}" for l in lines]
        else:
            prefix = f"https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl={crawl}/subset=warc/"
            return [prefix + l for l in lines]

    # Dynamically fetch from Common Crawl official table manifest
    manifest_url = f"https://data.commoncrawl.org/crawl-data/{crawl}/cc-index-table.paths.gz"
    print(f"Fetching remote shard manifest from: {manifest_url}...")
    req = urllib.request.Request(
        manifest_url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; ATSDiscoveryBot/1.0)"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        decompressed = gzip.decompress(resp.read()).decode("utf-8")

    shard_urls = [
        f"https://data.commoncrawl.org/{line.strip()}"
        for line in decompressed.splitlines()
        if "subset=warc" in line and line.strip().endswith(".parquet")
    ]
    if not shard_urls:
        raise ValueError(f"No WARC parquet shards found in manifest for crawl '{crawl}'. Check crawl ID.")

    print(f"Discovered {len(shard_urls)} Parquet shards for {crawl}")
    return shard_urls


def query_shard_for_ats(con: duckdb.DuckDBPyConnection, parquet_url: str) -> List[tuple]:
    """
    Queries a single Parquet shard over HTTPS for all supported ATS domains.
    Returns list of (platform, company_slug, sample_url).
    """
    domains = "', '".join({cfg["registered_domain"] for cfg in ATS_REGISTRY.values()})
    tlds = "', '".join({cfg["tld"] for cfg in ATS_REGISTRY.values()})
    
    # Platform mapping
    platform_cases = " ".join([
        f"WHEN url_host_registered_domain = '{cfg['registered_domain']}' THEN '{cfg['name']}'"
        for cfg in ATS_REGISTRY.values()
    ])
    
    # Slug extraction
    slug_cases = []
    for cfg in ATS_REGISTRY.values():
        if cfg["type"] == "path_slug":
            pattern = cfg["url_pattern"].replace("\\", "\\\\")
            slug_cases.append(
                f"WHEN url_host_registered_domain = '{cfg['registered_domain']}' "
                f"THEN lower(regexp_extract(url, '{pattern}', 1))"
            )
        elif cfg["type"] == "subdomain":
            pattern = cfg["host_pattern"].replace("\\", "\\\\")
            slug_cases.append(
                f"WHEN url_host_registered_domain = '{cfg['registered_domain']}' "
                f"THEN lower(regexp_extract(url_host_name, '{pattern}', 1))"
            )
    slug_case_sql = "CASE " + " ".join(slug_cases) + " ELSE NULL END"
    
    sql = f"""
    WITH filtered AS (
        SELECT
            CASE {platform_cases} ELSE 'Other' END AS ats_platform,
            {slug_case_sql} AS company_slug,
            url
        FROM read_parquet('{parquet_url}')
        WHERE subset = 'warc'
          AND url_host_tld IN ('{tlds}')
          AND url_host_registered_domain IN ('{domains}')
    )
    SELECT
        ats_platform,
        company_slug,
        MIN(url) as sample_url,
        COUNT(*) as total_urls
    FROM filtered
    WHERE company_slug IS NOT NULL
      AND length(company_slug) > 1
      AND company_slug NOT IN ('www', 'app', 'static', 'assets', 'api', 'help', 'search', 'apply')
    GROUP BY ats_platform, company_slug
    """
    
    for attempt in range(1, 4):
        try:
            res = con.sql(sql).fetchall()
            return res
        except Exception as e:
            wait = attempt * 5
            print(f"    [Retry {attempt}/3] HTTP error on shard: {e}. Pausing {wait}s...", file=sys.stderr)
            time.sleep(wait)
    return []


def run_gha_crawler(
    crawl: str = "CC-MAIN-2026-43",
    paths_file: Optional[str] = None,
    start_shard: int = 0,
    max_shards: int = 5,
    greenhouse_file: str = "greenhouse_companies.txt"
):
    print("=" * 65)
    print(" GitHub Actions Common Crawl ATS Extractor")
    print("=" * 65)
    print(f"Crawl: {crawl} | Shards: {start_shard} to {start_shard + max_shards - 1}")
    
    # 1. Load existing Greenhouse list for immediate de-duplication
    existing_gh: Set[str] = set()
    if os.path.exists(greenhouse_file):
        with open(greenhouse_file, "r", encoding="utf-8") as f:
            existing_gh = {line.strip().lower() for line in f if line.strip()}
    print(f"Existing {greenhouse_file}: {len(existing_gh)} companies")
    
    # 2. Get shards
    all_shards = get_crawl_shards(crawl, paths_file)
    selected_shards = all_shards[start_shard : start_shard + max_shards]
    print(f"Processing {len(selected_shards)} Parquet shards...\n")
    
    con = setup_duckdb()
    
    new_by_platform: Dict[str, Set[str]] = {}
    new_gh_names = []
    
    t0 = time.time()
    for idx, shard_url in enumerate(selected_shards, start=start_shard):
        shard_name = shard_url.split("/")[-1][:35]
        print(f"[{idx+1}/{len(all_shards)}] Scanning {shard_name}...", end=" ", flush=True)
        shard_start = time.time()
        
        matches = query_shard_for_ats(con, shard_url)
        shard_elapsed = time.time() - shard_start
        print(f"done ({shard_elapsed:.1f}s, {len(matches)} ATS companies)")
        
        for platform, slug, sample_url, count in matches:
            if platform not in new_by_platform:
                new_by_platform[platform] = set()
            new_by_platform[platform].add(slug)
            
            # Specific Greenhouse tracking
            if platform == "Greenhouse":
                if slug.lower() not in existing_gh:
                    existing_gh.add(slug.lower())
                    new_gh_names.append(slug)
                    print(f"  + New Greenhouse: {slug} ({sample_url})")

    total_time = time.time() - t0
    print("\n" + "=" * 65)
    print(f" Completed scan in {total_time:.1f} seconds")
    print("=" * 65)
    
    for platform, slugs in new_by_platform.items():
        print(f"  - {platform}: {len(slugs)} unique companies found in this batch")
        
    # Append new Greenhouse companies to file
    if new_gh_names:
        print(f"\nAppending {len(new_gh_names)} new companies to {greenhouse_file}...")
        # Re-read all, combine, and sort
        with open(greenhouse_file, "r", encoding="utf-8") as f:
            all_current = [line.strip() for line in f if line.strip()]
        
        all_combined = sorted(list(set(all_current) | set(new_gh_names)), key=lambda s: s.lower())
        with open(greenhouse_file, "w", encoding="utf-8") as f:
            for item in all_combined:
                f.write(item + "\n")
        print(f"Updated {greenhouse_file} total size: {len(all_combined)} companies.")
    else:
        print(f"\nNo new Greenhouse companies in this shard batch (all were already present).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GitHub Actions Common Crawl ATS Runner")
    parser.add_argument("--crawl", default="CC-MAIN-2026-43", help="Crawl ID (e.g. CC-MAIN-2026-43)")
    parser.add_argument("--paths-file", default=None, help="Optional local paths.gz file")
    parser.add_argument("--start", type=int, default=0, help="Start shard index")
    parser.add_argument("--shards", type=int, default=5, help="Number of shards to process")
    parser.add_argument("--greenhouse-file", default="greenhouse_companies.txt")
    
    args = parser.parse_args()
    run_gha_crawler(
        crawl=args.crawl,
        paths_file=args.paths_file,
        start_shard=args.start,
        max_shards=args.shards,
        greenhouse_file=args.greenhouse_file
    )

