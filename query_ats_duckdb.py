"""
Query Common Crawl Columnar Index using DuckDB to extract ATS companies.

Based on the architecture in Common Crawl's whirlwind-python:
- Uses DuckDB's httpfs extension to query remote Parquet files without downloading them all.
- Leverages Parquet predicate pushdown (url_host_registered_domain & url_host_tld).
- Extracts company identifiers for Greenhouse, Lever, Ashby, Workday, BambooHR, etc.
- Stores sample WARC offsets for human-readable name extraction with warcio.
"""

import os
import sys
import time
import gzip
import argparse
from pathlib import Path
from typing import List, Optional

import duckdb
from ats_definitions import ATS_REGISTRY


def get_registered_domains() -> List[str]:
    return list({cfg["registered_domain"] for cfg in ATS_REGISTRY.values()})


def get_tlds() -> List[str]:
    return list({cfg["tld"] for cfg in ATS_REGISTRY.values()})


def build_sql_query(table_or_files: str, crawl: Optional[str] = None, limit: Optional[int] = None) -> str:
    """
    Constructs an optimized SQL query that filters on indexed columns
    and extracts company slugs and sample WARC pointers.
    """
    domains = "', '".join(get_registered_domains())
    tlds = "', '".join(get_tlds())
    
    crawl_filter = f"AND crawl = '{crawl}'" if crawl else ""
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # Build platform resolution CASE statement
    platform_cases = []
    for key, cfg in ATS_REGISTRY.items():
        platform_cases.append(
            f"WHEN url_host_registered_domain = '{cfg['registered_domain']}' THEN '{cfg['name']}'"
        )
    platform_case_sql = "CASE " + " ".join(platform_cases) + " ELSE 'Unknown' END"
    
    # Build company slug extractor CASE statement
    slug_cases = []
    for key, cfg in ATS_REGISTRY.items():
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
    
    query = f"""
    WITH base_records AS (
        SELECT
            {platform_case_sql} AS ats_platform,
            {slug_case_sql} AS company_slug,
            url_host_registered_domain,
            url_host_name,
            url,
            fetch_status,
            warc_filename,
            warc_record_offset,
            warc_record_length
        FROM {table_or_files}
        WHERE subset = 'warc'
          {crawl_filter}
          AND url_host_tld IN ('{tlds}')
          AND url_host_registered_domain IN ('{domains}')
    )
    SELECT
        ats_platform,
        company_slug,
        COUNT(*) AS total_postings_found,
        MIN(url) AS sample_job_url,
        MIN(warc_filename) AS sample_warc_filename,
        MIN(warc_record_offset) AS sample_warc_offset,
        MIN(warc_record_length) AS sample_warc_length
    FROM base_records
    WHERE company_slug IS NOT NULL
      AND length(company_slug) > 1
      AND company_slug NOT IN ('www', 'app', 'static', 'assets', 'api', 'help', 'search', 'apply')
    GROUP BY ats_platform, company_slug
    ORDER BY total_postings_found DESC
    {limit_clause};
    """
    return query


def setup_duckdb_connection(temp_dir: str = "/tmp/duckdb_ext") -> duckdb.DuckDBPyConnection:
    os.makedirs(temp_dir, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET extension_directory = '{temp_dir}'")
    try:
        con.execute("INSTALL httpfs;")
    except Exception:
        pass
    con.execute("LOAD httpfs")
    con.execute("SET threads = 2;")  # Conservative thread limit to prevent CloudFront 503 SlowDown
    con.execute("SET http_retries = 30;")
    con.execute("SET http_keep_alive = true;")
    return con


def load_file_list(crawl: str, paths_file: Optional[str] = None) -> List[str]:
    import urllib.request
    if paths_file and os.path.exists(paths_file):
        with gzip.open(paths_file, mode="rt", encoding="utf8") as fd:
            lines = [line.strip() for line in fd if line.strip()]
        if lines and lines[0].startswith("http"):
            return lines
        elif lines and lines[0].startswith("cc-index/"):
            return [f"https://data.commoncrawl.org/{l}" for l in lines]
        external_prefix = f"https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl={crawl}/subset=warc/"
        return [external_prefix + line for line in lines]
        
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
        raise ValueError(f"No WARC parquet shards found in manifest for crawl '{crawl}'")

    print(f"Discovered {len(shard_urls)} Parquet shards for {crawl}")
    return shard_urls


def run_ats_extraction(
    crawl: str = "CC-MAIN-2024-22",
    mode: str = "remote",
    local_dir: Optional[str] = None,
    partition_limit: Optional[int] = 5,
    output_prefix: str = "ats_companies"
):
    print("=" * 70)
    print(" Common Crawl ATS Company Extractor (DuckDB & Whirlwind)")
    print("=" * 70)
    print(f"Crawl: {crawl}")
    print(f"Mode: {mode}")
    print(f"Tracking Platforms: {', '.join(ATS_REGISTRY.keys())}")
    
    con = setup_duckdb_connection()
    
    if mode == "local":
        if not local_dir:
            raise ValueError("local_dir must be specified for local mode")
        table_source = f"read_parquet('{local_dir}/**/*.parquet', hive_partitioning=True)"
    else:
        files = load_file_list(crawl)
        if partition_limit and partition_limit > 0:
            files = files[:partition_limit]
            print(f"Testing on first {partition_limit} parquet partition(s)...")
        else:
            print(f"Querying all {len(files)} parquet partitions over CloudFront...")
        
        # Format list for SQL read_parquet
        file_list_repr = repr(files)
        table_source = f"read_parquet({file_list_repr}, hive_partitioning=True)"

    query = build_sql_query(table_source, crawl=crawl)
    
    print("\nExecuting SQL query against Common Crawl columnar index...")
    t0 = time.time()
    
    retries = 5
    while retries > 0:
        try:
            rel = con.sql(query)
            # Fetch results
            results = rel.fetchall()
            columns = [desc[0] for desc in rel.description]
            break
        except Exception as e:
            print(f"Query error: {e}. Retrying in 10s...", file=sys.stderr)
            time.sleep(10)
            retries -= 1
            if retries == 0:
                raise
                
    elapsed = time.time() - t0
    print(f"Query completed in {elapsed:.2f} seconds.")
    print(f"Total Unique Company Job Boards Found: {len(results)}\n")
    
    if not results:
        print("No companies matched in the selected partition(s). Try scanning more partitions or another crawl.")
        return
        
    # Display top results
    print(f"{'Platform':<16} {'Company Slug':<25} {'Postings':<10} Sample URL")
    print("-" * 75)
    for row in results[:20]:
        platform, slug, count, sample_url = row[0], row[1], row[2], row[3]
        print(f"{platform:<16} {slug:<25} {count:<10} {sample_url[:45]}")
        
    # Save results to parquet and csv
    parquet_out = f"{output_prefix}.parquet"
    csv_out = f"{output_prefix}.csv"
    
    con.execute(f"CREATE TABLE ats_results AS {query}")
    con.execute(f"COPY ats_results TO '{parquet_out}' (FORMAT PARQUET)")
    con.execute(f"COPY ats_results TO '{csv_out}' (HEADER, DELIMITER ',')")
    
    print(f"\nSaved results to:")
    print(f"  - Parquet: {parquet_out}")
    print(f"  - CSV:     {csv_out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract ATS companies from Common Crawl index using DuckDB")
    parser.add_argument("--crawl", default="CC-MAIN-2024-22", help="Crawl ID")
    parser.add_argument("--mode", default="remote", choices=["remote", "local"], help="Query mode")
    parser.add_argument("--local-dir", default=None, help="Directory of local parquet files if mode=local")
    parser.add_argument("--partitions", type=int, default=10, help="Number of parquet partitions to scan (0 for all)")
    parser.add_argument("--output", default="ats_companies", help="Output file prefix")
    
    args = parser.parse_args()
    run_ats_extraction(
        crawl=args.crawl,
        mode=args.mode,
        local_dir=args.local_dir,
        partition_limit=args.partitions,
        output_prefix=args.output
    )

