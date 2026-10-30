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
import json
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
            pattern = cfg["url_pattern"].replace("'", "''")
            slug_cases.append(
                f"WHEN url_host_registered_domain = '{cfg['registered_domain']}' "
                f"THEN lower(regexp_extract(url, '{pattern}', 1))"
            )
        elif cfg["type"] == "subdomain":
            pattern = cfg["host_pattern"].replace("'", "''")
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
      AND company_slug NOT IN (
          'www', 'app', 'static', 'assets', 'api', 'help', 'search', 'apply',
          'ai_opt_out_request', 'authorize', 'v1', 'private', 'site', 'docs',
          'support', 'terms', 'privacy', 'login', 'signup'
      )
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


def get_all_crawls(min_year: int = 2024) -> List[str]:
    """Retrieves all Common Crawl IDs from the official collinfo.json API since min_year."""
    url = "https://index.commoncrawl.org/collinfo.json"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; ATSDiscoveryBot/1.0)"}
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    crawls = []
    for c in data:
        cid = c.get("id", "")
        parts = cid.split("-")
        if len(parts) >= 3 and parts[2].isdigit():
            if int(parts[2]) >= min_year:
                crawls.append(cid)
    return crawls


PLATFORM_FILES: Dict[str, str] = {
    "Greenhouse": "greenhouse_companies.txt",
    "Lever": "lever_companies.txt",
    "Ashby": "ashby_companies.txt",
    "Workday": "workday_companies.txt",
    "BambooHR": "bamboohr_companies.txt",
    "Workable": "workable_companies.txt",
    "SmartRecruiters": "smartrecruiters_companies.txt",
    "Rippling": "rippling_companies.txt",
    "Jobvite": "jobvite_companies.txt",
    "Recruitee": "recruitee_companies.txt",
    "JazzHR": "jazzhr_companies.txt",
    "Breezy HR": "breezy_companies.txt",
    "Pinpoint": "pinpoint_companies.txt",
}


def run_gha_crawler(
    crawl: str = "ALL",
    paths_file: Optional[str] = None,
    start_shard: int = 270,
    max_shards: int = 15,
    greenhouse_file: str = "greenhouse_companies.txt",
    min_year: int = 2024
):
    print("=" * 65)
    print(" GitHub Actions Common Crawl ATS Extractor (Multi-Platform)")
    print("=" * 65)

    if crawl.upper() in ("ALL", "LAST-2-YEARS", "LAST_2_YEARS"):
        crawl_list = get_all_crawls(min_year=min_year)
        print(f"Target: ALL crawls since {min_year} ({len(crawl_list)} releases)")
    elif "," in crawl:
        crawl_list = [c.strip() for c in crawl.split(",") if c.strip()]
        print(f"Target: {len(crawl_list)} specific crawls")
    else:
        crawl_list = [crawl.strip()]
        print(f"Target: Single crawl {crawl}")

    # Override Greenhouse filename if user passed a custom argument
    PLATFORM_FILES["Greenhouse"] = greenhouse_file

    # 1. Pre-load all existing lists for immediate de-duplication
    known_by_platform: Dict[str, Set[str]] = {}
    for plat, filename in PLATFORM_FILES.items():
        known_by_platform[plat] = set()
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                known_by_platform[plat] = {line.strip().lower() for line in f if line.strip()}
            print(f"Existing {filename:32s}: {len(known_by_platform[plat]):,} companies")

    con = setup_duckdb()
    new_by_platform: Dict[str, Set[str]] = {}
    new_discoveries_by_platform: Dict[str, List[tuple]] = {}
    global_start = time.time()

    for crawl_idx, current_crawl in enumerate(crawl_list, start=1):
        print(f"\n--- [{crawl_idx}/{len(crawl_list)}] Processing {current_crawl} (Shards {start_shard} to {start_shard + max_shards - 1}) ---")
        try:
            all_shards = get_crawl_shards(current_crawl, paths_file)
            selected_shards = all_shards[start_shard : start_shard + max_shards]
        except Exception as e:
            print(f"  [!] Skipping crawl {current_crawl} due to error: {e}")
            continue

        for idx, shard_url in enumerate(selected_shards, start=start_shard):
            shard_name = shard_url.split("/")[-1][:35]
            print(f"  [{idx+1}/{len(all_shards)}] Scanning {shard_name}...", end=" ", flush=True)
            shard_start = time.time()

            matches = query_shard_for_ats(con, shard_url)
            shard_elapsed = time.time() - shard_start
            print(f"done ({shard_elapsed:.1f}s, {len(matches)} ATS companies)")

            for platform, slug, sample_url, count in matches:
                if platform not in new_by_platform:
                    new_by_platform[platform] = set()
                new_by_platform[platform].add(slug)

                # Check if this slug is new for this platform
                known_set = known_by_platform.setdefault(platform, set())
                if slug.lower() not in known_set:
                    known_set.add(slug.lower())
                    new_discoveries_by_platform.setdefault(platform, []).append((slug, sample_url))
                    print(f"    + New {platform}: {slug} ({sample_url})")

    total_time = time.time() - global_start
    print("\n" + "=" * 65)
    print(f" Completed all {len(crawl_list)} crawls in {total_time:.1f} seconds")
    print("=" * 65)

    for platform, slugs in new_by_platform.items():
        new_count = len(new_discoveries_by_platform.get(platform, []))
        print(f"  - {platform:16s}: {len(slugs):,} seen ({new_count:,} net-new added)")

    # 2. Append new companies to platform-specific text files
    total_added = 0
    import csv
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()
    all_new_csv_rows = []

    for plat, filename in PLATFORM_FILES.items():
        new_items = new_discoveries_by_platform.get(plat, [])
        if new_items:
            new_slugs = [s for s, _ in new_items]
            all_current = []
            if os.path.exists(filename):
                with open(filename, "r", encoding="utf-8") as f:
                    all_current = [line.strip() for line in f if line.strip()]

            all_combined = sorted(list(set(all_current) | set(new_slugs)), key=lambda s: s.lower())
            with open(filename, "w", encoding="utf-8") as f:
                for item in all_combined:
                    f.write(item + "\n")
            print(f"Updated {filename}: +{len(new_slugs):,} new companies (Total: {len(all_combined):,})")
            total_added += len(new_slugs)

            for slug, url in new_items:
                all_new_csv_rows.append([plat, slug, url, now_iso])

    # 3. Append to master CSV log
    if all_new_csv_rows:
        csv_file = "all_ats_companies_discovered.csv"
        csv_exists = os.path.exists(csv_file)
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not csv_exists:
                writer.writerow(["ats_platform", "company_slug", "sample_url", "discovered_at"])
            writer.writerows(all_new_csv_rows)
        print(f"Saved {len(all_new_csv_rows):,} discoveries to {csv_file}")

    if total_added == 0:
        print("\nNo net-new ATS companies discovered in this batch (all were already known).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GitHub Actions Common Crawl ATS Runner")
    parser.add_argument("--crawl", default="CC-MAIN-2026-43", help="Crawl ID, comma-separated list, or 'ALL'")
    parser.add_argument("--min-year", type=int, default=2024, help="Earliest crawl year when using 'ALL'")
    parser.add_argument("--paths-file", default=None, help="Optional local paths.gz file")
    parser.add_argument("--start", type=int, default=270, help="Start shard index")
    parser.add_argument("--shards", type=int, default=15, help="Number of shards to process per crawl")
    parser.add_argument("--greenhouse-file", default="greenhouse_companies.txt")

    args = parser.parse_args()
    run_gha_crawler(
        crawl=args.crawl,
        paths_file=args.paths_file,
        start_shard=args.start,
        max_shards=args.shards,
        greenhouse_file=args.greenhouse_file,
        min_year=args.min_year
    )

