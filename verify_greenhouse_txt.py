#!/usr/bin/env python3
"""
Validate Greenhouse Companies from Text List using the Repository's Verification Algorithm.

Reuses the core components from `ats_diacovery_2.py`:
- `AsyncHTTPClient`: Concurrency throttling, per-host semaphores, 429/403 cooldowns.
- `PublicATSPayloadParser._parse_greenhouse`: Greenhouse JSON API parsing, active job counts,
  US location filtering, and sample titles.
- `EntityResolver`: Confidence scoring, root domain resolution, active status classification.
- `ATSDatabase`: SQLite storage, indexing, and CSV/TXT exports.
"""

import os
import sys
import argparse
import asyncio
import sqlite3
import csv
import logging
from pathlib import Path
from typing import List, Set, Dict, Tuple, Optional

# Import components from existing repository engine
from ats_diacovery_2 import (
    AsyncHTTPClient,
    ATSBoardVerifier,
    PublicATSPayloadParser,
    EntityResolver,
    ATSDatabase,
    DiscoveredBoard,
    VerifiedATSBoard,
    ATS_PLATFORMS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("validate_greenhouse")


def clean_slug_candidate(raw_line: str) -> str:
    """
    Cleans a line from greenhouse_companies.txt into a canonical Greenhouse board slug.
    Example:
      '#1-800-contacts' -> '1-800-contacts'
      'Speechify'        -> 'speechify'
      'Bluestone Provider' -> 'bluestone-provider'
    """
    s = raw_line.strip().lstrip("#").strip()
    if not s:
        return ""
    # If the string contains spaces, convert to lowercase hyphenated slug
    if " " in s:
        s = s.replace(" ", "-")
    return s.lower()


def load_candidate_slugs(
    input_file: str,
    shard_index: int = 0,
    total_shards: int = 1,
) -> Tuple[List[str], int]:
    """
    Reads the text list and partitions it according to shard_index / total_shards.
    """
    with open(input_file, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    # Preserve unique slugs in original order
    seen: Set[str] = set()
    cleaned_slugs: List[str] = []
    for line in lines:
        slug = clean_slug_candidate(line)
        if slug and slug not in seen:
            seen.add(slug)
            cleaned_slugs.append(slug)

    total_candidates = len(cleaned_slugs)

    # Shard partition (round-robin or chunk slice)
    # Using chunk slicing:
    chunk_size = (total_candidates + total_shards - 1) // total_shards
    start_idx = shard_index * chunk_size
    end_idx = min(start_idx + chunk_size, total_candidates)
    shard_slugs = cleaned_slugs[start_idx:end_idx]

    return shard_slugs, total_candidates


async def run_validation(
    input_file: str = "greenhouse_companies.txt",
    output_db: str = "greenhouse_validation.db",
    existing_db: Optional[str] = "ats_shard_greenhouse_us.db",
    shard_index: int = 0,
    total_shards: int = 1,
    concurrency: int = 50,
    batch_size: int = 500,
):
    logger.info("=" * 65)
    logger.info(" Greenhouse Company Validation Engine (Repository Algorithm)")
    logger.info("=" * 65)
    logger.info("Input file: %s", input_file)
    logger.info("Shard: %d of %d", shard_index + 1, total_shards)

    db = ATSDatabase(output_db)
    cur = db.conn.cursor()

    # 1. Check existing verified database if available to skip known active boards
    already_verified_slugs: Set[str] = set()
    if existing_db and os.path.exists(existing_db):
        try:
            ex_conn = sqlite3.connect(existing_db)
            ex_cur = ex_conn.cursor()
            ex_cur.execute("SELECT slug FROM verified_boards WHERE is_active = 1")
            already_verified_slugs = {r[0].lower() for r in ex_cur.fetchall()}
            ex_conn.close()
            logger.info("Found %d pre-verified active boards in '%s'.", len(already_verified_slugs), existing_db)
        except Exception as e:
            logger.warning("Could not read existing DB '%s': %s", existing_db, e)

    # 2. Load candidate slugs for this shard
    shard_slugs, total_candidates = load_candidate_slugs(input_file, shard_index, total_shards)
    logger.info(
        "Shard assigned %d candidates out of %d total (Index %d to %d).",
        len(shard_slugs),
        total_candidates,
        shard_index * ((total_candidates + total_shards - 1) // total_shards),
        shard_index * ((total_candidates + total_shards - 1) // total_shards) + len(shard_slugs),
    )

    # Filter out already verified in output_db or pre-verified
    cur.execute("SELECT slug FROM verified_boards")
    done_in_output = {r[0].lower() for r in cur.fetchall()}

    candidates_to_probe: List[DiscoveredBoard] = []
    pre_verified_to_copy: List[str] = []

    for slug in shard_slugs:
        if slug in done_in_output:
            continue
        if slug in already_verified_slugs:
            pre_verified_to_copy.append(slug)
        else:
            candidates_to_probe.append(
                DiscoveredBoard(
                    ats_id="greenhouse",
                    slug=slug,
                    board_url=f"https://job-boards.greenhouse.io/{slug}",
                    discovery_source="greenhouse_companies_txt",
                )
            )

    # If existing DB has full records, copy them directly into output DB
    if pre_verified_to_copy and existing_db and os.path.exists(existing_db):
        try:
            ex_conn = sqlite3.connect(existing_db)
            ex_cur = ex_conn.cursor()
            placeholders = ",".join("?" for _ in pre_verified_to_copy)
            ex_rows = ex_cur.execute(
                f"SELECT * FROM verified_boards WHERE slug IN ({placeholders})",
                pre_verified_to_copy,
            ).fetchall()
            col_names = [desc[0] for desc in ex_cur.description]
            ins_placeholders = ",".join("?" for _ in col_names)
            cur.executemany(
                f"INSERT OR REPLACE INTO verified_boards ({','.join(col_names)}) VALUES ({ins_placeholders})",
                [tuple(r) for r in ex_rows],
            )
            db.conn.commit()
            ex_conn.close()
            logger.info("Copied %d pre-verified records into '%s'.", len(ex_rows), output_db)
        except Exception as e:
            logger.warning("Failed copying pre-verified rows: %s", e)

    logger.info("Net-new candidate boards to probe over HTTP: %d", len(candidates_to_probe))

    if not candidates_to_probe:
        logger.info("All candidates in this shard are already verified!")
        db.close()
        return

    # 3. Async HTTP Probe using the repository's AsyncHTTPClient & ATSBoardVerifier
    async with AsyncHTTPClient(concurrency=concurrency, per_host_concurrency=10) as http:
        verifier = ATSBoardVerifier(http)

        for i in range(0, len(candidates_to_probe), batch_size):
            batch = candidates_to_probe[i : i + batch_size]
            batch_results = await asyncio.gather(*(verifier.verify_board(b) for b in batch))
            resolved_batch = EntityResolver.score_and_resolve(batch_results)
            db.upsert_verified_boards(resolved_batch)

            active_count = sum(1 for b in resolved_batch if b.is_active)
            us_count = sum(1 for b in resolved_batch if b.is_active and b.has_us_jobs)
            logger.info(
                "Progress [%d/%d]: Verified %d candidates (+%d active, +%d US active)",
                min(i + batch_size, len(candidates_to_probe)),
                len(candidates_to_probe),
                len(batch),
                active_count,
                us_count,
            )

    db.close()
    logger.info("Validation complete for shard %d.", shard_index + 1)


def export_validated_results(
    db_path: str = "greenhouse_validation.db",
    output_dir: str = "./exports_greenhouse",
):
    """
    Exports clean CSV and TXT lists from the validated database.
    """
    os.makedirs(output_dir, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Total stats
    total_verified = cur.execute("SELECT COUNT(*) FROM verified_boards").fetchone()[0]
    total_active = cur.execute("SELECT COUNT(*) FROM verified_boards WHERE is_active = 1").fetchone()[0]
    total_us = cur.execute("SELECT COUNT(*) FROM verified_boards WHERE is_active = 1 AND has_us_jobs = 1").fetchone()[0]
    total_jobs = cur.execute("SELECT SUM(active_job_count) FROM verified_boards WHERE is_active = 1").fetchone()[0] or 0
    total_us_jobs = cur.execute("SELECT SUM(us_job_count) FROM verified_boards WHERE is_active = 1 AND has_us_jobs = 1").fetchone()[0] or 0

    # 1. Export Active Companies TXT
    active_rows = cur.execute(
        "SELECT company_name, slug FROM verified_boards WHERE is_active = 1 ORDER BY company_name COLLATE NOCASE"
    ).fetchall()
    active_txt = os.path.join(output_dir, "greenhouse_companies_active.txt")
    with open(active_txt, "w", encoding="utf-8") as f:
        for r in active_rows:
            name = (r["company_name"] or r["slug"]).strip()
            if name:
                f.write(name + "\n")

    # 2. Export US Active Companies TXT
    us_rows = cur.execute(
        "SELECT company_name, slug FROM verified_boards WHERE is_active = 1 AND has_us_jobs = 1 ORDER BY company_name COLLATE NOCASE"
    ).fetchall()
    us_txt = os.path.join(output_dir, "greenhouse_companies_us_active.txt")
    with open(us_txt, "w", encoding="utf-8") as f:
        for r in us_rows:
            name = (r["company_name"] or r["slug"]).strip()
            if name:
                f.write(name + "\n")

    # 3. Export Full Master CSV
    all_active = cur.execute(
        """
        SELECT 
            company_name,
            slug,
            resolved_domain,
            board_url,
            active_job_count,
            us_job_count,
            has_us_jobs,
            confidence_score,
            board_status,
            latest_posted_at,
            sample_job_titles_json,
            locations_json
        FROM verified_boards
        WHERE is_active = 1
        ORDER BY active_job_count DESC, us_job_count DESC
        """
    ).fetchall()

    master_csv = os.path.join(output_dir, "greenhouse_companies_verified.csv")
    with open(master_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "company_name", "slug", "resolved_domain", "board_url",
            "active_job_count", "us_job_count", "has_us_jobs", "confidence_score",
            "board_status", "latest_posted_at", "sample_job_titles", "locations"
        ])
        for r in all_active:
            writer.writerow([
                r["company_name"],
                r["slug"],
                r["resolved_domain"],
                r["board_url"],
                r["active_job_count"],
                r["us_job_count"],
                r["has_us_jobs"],
                r["confidence_score"],
                r["board_status"],
                r["latest_posted_at"],
                r["sample_job_titles_json"],
                r["locations_json"]
            ])

    # 4. Generate SUMMARY.md
    summary_md = os.path.join(output_dir, "VALIDATION_SUMMARY.md")
    with open(summary_md, "w", encoding="utf-8") as f:
        f.write("# Greenhouse Company Validation Summary\n\n")
        f.write(f"- **Total Slugs Evaluated**: **{total_verified:,}**\n")
        f.write(f"- **Active Verified Boards**: **{total_active:,}**\n")
        f.write(f"- **Verified with US Jobs**: **{total_us:,}**\n")
        f.write(f"- **Total Live Open Jobs**: **{total_jobs:,}**\n")
        f.write(f"- **Total US Open Jobs**: **{total_us_jobs:,}**\n\n")
        f.write("### Generated Artifacts:\n")
        f.write(f"- `greenhouse_companies_active.txt`: All {total_active:,} verified active companies\n")
        f.write(f"- `greenhouse_companies_us_active.txt`: {total_us:,} companies with verified US job postings\n")
        f.write(f"- `greenhouse_companies_verified.csv`: Complete metadata dataset\n")

    con.close()
    logger.info("=" * 65)
    logger.info("Exported Validation Summary to '%s':", output_dir)
    logger.info("  - Active Verified Companies: %d (Live Jobs: %d)", total_active, total_jobs)
    logger.info("  - US Active Companies:       %d (US Jobs: %d)", total_us, total_us_jobs)
    logger.info("=" * 65)


def main():
    parser = argparse.ArgumentParser(description="Validate Greenhouse company list using repo algorithm")
    sub = parser.add_subparsers(dest="command", required=True)

    p_val = sub.add_parser("validate", help="Validate candidates across shards")
    p_val.add_argument("--input", default="greenhouse_companies.txt", help="Input text file")
    p_val.add_argument("--db", default="greenhouse_validation.db", help="Output SQLite database")
    p_val.add_argument("--existing-db", default="ats_shard_greenhouse_us.db", help="Existing DB to reuse")
    p_val.add_argument("--shard-index", type=int, default=0, help="Shard index (0-based)")
    p_val.add_argument("--total-shards", type=int, default=1, help="Total number of shards")
    p_val.add_argument("--concurrency", type=int, default=50, help="Async HTTP concurrency")
    p_val.add_argument("--batch-size", type=int, default=500, help="Batch size per checkpoint")

    p_merge = sub.add_parser("merge", help="Merge shard databases")
    p_merge.add_argument("--inputs", required=True, help="Comma-separated shard databases")
    p_merge.add_argument("--output", default="greenhouse_validation_merged.db", help="Merged output database")

    p_exp = sub.add_parser("export", help="Export validated CSV and TXT lists")
    p_exp.add_argument("--db", default="greenhouse_validation.db", help="SQLite database to export from")
    p_exp.add_argument("--output-dir", default="./exports_greenhouse", help="Output directory")

    args = parser.parse_args()
    if args.command == "validate":
        asyncio.run(
            run_validation(
                input_file=args.input,
                output_db=args.db,
                existing_db=args.existing_db,
                shard_index=args.shard_index,
                total_shards=args.total_shards,
                concurrency=args.concurrency,
                batch_size=args.batch_size,
            )
        )
    elif args.command == "merge":
        shard_paths = [p.strip() for p in args.inputs.split(",") if p.strip()]
        master_db = ATSDatabase(args.output)
        disc_cnt, ver_cnt = master_db.merge_from_databases(shard_paths)
        master_db.close()
        logger.info("Merged %d shard DBs into '%s' (%d verified records).", len(shard_paths), args.output, ver_cnt)
    elif args.command == "export":
        export_validated_results(db_path=args.db, output_dir=args.output_dir)


if __name__ == "__main__":
    main()

