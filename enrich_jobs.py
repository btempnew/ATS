
#!/usr/bin/env python3
"""
Stand-Alone Job Description Enrichment CLI.

Enriches raw job records with deep extraction facts:
- Salary (min, max, currency, period, formatted string)
- Skills (extracted skills, required, preferred)
- Work Arrangement (remote, hybrid, on-site)
- Experience & Education requirements
- Visa Sponsorship & H-1B Eligibility (status + verbatim evidence)
- Reporting line / manager title

Supports enriching:
1. SQLite databases: python enrich_jobs.py --db live_jobs.db
2. CSV datasets: python enrich_jobs.py --csv live_jobs.csv --output live_jobs_enriched.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
from typing import Any, Dict, List

# Ensure local packages are importable
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from scrapers.job_extraction.engine import extract_job
from scrapers.job_extraction.jd_enricher import parse_salary, detect_work_mode
from matching.jd_facts import extract_jd_facts


def enrich_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Enriches a single job record dictionary with all facts."""
    out = dict(raw)
    desc = raw.get("content") or raw.get("description") or raw.get("body") or ""
    title = raw.get("title") or raw.get("job_title") or ""
    location = raw.get("location") or ""
    company = raw.get("company_name") or raw.get("company") or ""
    platform = raw.get("platform") or raw.get("source_board") or ""

    # Default fields
    out.setdefault("salary", "")
    out.setdefault("salary_min", None)
    out.setdefault("salary_max", None)
    out.setdefault("salary_currency", "")
    out.setdefault("salary_period", "")
    out.setdefault("skills", "")
    out.setdefault("skills_required", "")
    out.setdefault("skills_preferred", "")
    out.setdefault("work_mode", "")
    out.setdefault("experience_min", None)
    out.setdefault("experience_max", None)
    out.setdefault("education", "")
    out.setdefault("h1b_status", "unknown")
    out.setdefault("visa_sponsorship_status", "unknown")
    out.setdefault("sponsorship_evidence", "")
    out.setdefault("reports_to", "")

    # 1. Main JD extraction engine
    if desc:
        try:
            eng = extract_job({
                "title": title,
                "location": location,
                "description": desc,
                "company": company,
                "source_board": platform,
            })
            if eng:
                out["salary"] = eng.get("salary") or ""
                out["salary_min"] = eng.get("salary_min")
                out["salary_max"] = eng.get("salary_max")
                out["salary_currency"] = eng.get("salary_currency") or ""
                out["salary_period"] = eng.get("salary_period") or ""
                out["skills"] = eng.get("skills") or ""
                out["skills_required"] = ", ".join(eng.get("skills_required") or [])
                out["skills_preferred"] = ", ".join(eng.get("skills_preferred") or [])
                out["work_mode"] = eng.get("work_mode") or ""
                out["experience_min"] = eng.get("experience_min")
                out["experience_max"] = eng.get("experience_max")
                edu = eng.get("education_requirements") or []
                out["education"] = ", ".join(edu) if isinstance(edu, list) else str(edu)
        except Exception:
            pass

    # 2. Fallback salary parser
    if desc and not out.get("salary_min") and not out.get("salary"):
        try:
            sal = parse_salary(desc)
            if sal.get("salary_min") or sal.get("salary_max"):
                out["salary_min"] = sal.get("salary_min")
                out["salary_max"] = sal.get("salary_max")
                out["salary_currency"] = sal.get("salary_currency") or ""
                out["salary_period"] = sal.get("salary_period") or ""
        except Exception:
            pass

    # 3. Work mode fallback
    if not out.get("work_mode"):
        try:
            wm = detect_work_mode(raw)
            if wm:
                out["work_mode"] = wm.title()
        except Exception:
            pass

    # 4. JD Facts: H-1B, Visa Sponsorship, and Reporting Line
    if desc:
        try:
            facts = extract_jd_facts(desc)
            if facts:
                out["h1b_status"] = facts.h1b_status or "unknown"
                out["visa_sponsorship_status"] = facts.visa_sponsorship_status or "unknown"
                out["sponsorship_evidence"] = " | ".join(facts.sponsorship_evidence)
                out["reports_to"] = facts.reports_to or ""
        except Exception:
            pass

    return out


def enrich_csv_file(input_path: str, output_path: str) -> int:
    """Reads a CSV file, enriches every row, and writes to output CSV."""
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"CSV file not found: {input_path}")

    with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print(f"No rows found in {input_path}")
        return 0

    print(f"Enriching {len(rows):,} jobs from {input_path}...")
    enriched_rows = []
    has_salary = 0
    has_skills = 0
    has_h1b = 0

    for idx, row in enumerate(rows, 1):
        enriched = enrich_record(row)
        if enriched.get("salary_min") or enriched.get("salary"):
            has_salary += 1
        if enriched.get("skills"):
            has_skills += 1
        if enriched.get("h1b_status") in ("yes", "no", "conditional"):
            has_h1b += 1
        enriched_rows.append(enriched)

        if idx % 500 == 0 or idx == len(rows):
            print(f"[{idx:,}/{len(rows):,}] Processed | Salaries: {has_salary:,} | Skills: {has_skills:,} | H-1B Detected: {has_h1b:,}", flush=True)

    fieldnames = list(enriched_rows[0].keys())
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched_rows)

    print(f"\nDone! Enriched dataset saved to: {output_path}")
    print(f"  • With Identified Compensation: {has_salary:,} / {len(rows):,}")
    print(f"  • With Extracted Skills:        {has_skills:,} / {len(rows):,}")
    print(f"  • With H-1B / Visa Decisions:   {has_h1b:,} / {len(rows):,}")
    return len(enriched_rows)


def _enrich_single_record_wrapper(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Helper wrapper for parallel multi-process enrichment."""
    enr = enrich_record(raw)
    return {k: sanitize_sqlite_value(v) for k, v in enr.items()}


def sanitize_sqlite_value(val: Any) -> Any:
    """Ensures values bound to SQLite are primitive types (str, int, float, None) within 64-bit limits."""
    if val is None:
        return None
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, int):
        if val > 9223372036854775807 or val < -9223372036854775808:
            return str(val)
        return val
    if isinstance(val, float):
        import math
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    if isinstance(val, (str, bytes)):
        return val
    if isinstance(val, (list, tuple, set)):
        return ", ".join(str(x) for x in val if x is not None)
    if isinstance(val, dict):
        for k in ("name", "label", "text", "value", "title", "name_text", "addressLocality"):
            if k in val and val[k]:
                return str(val[k])
        try:
            import json
            return json.dumps(val, default=str)
        except Exception:
            return str(val)
    return str(val)


def export_sqlite_to_csv(db_path: str, csv_path: str, include_content: bool = False):
    """Exports SQLite live_jobs table to CSV, omitting heavy HTML content by default."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(live_jobs);")
    cols = [r[1] for r in cur.fetchall()]
    if not include_content and "content" in cols:
        cols = [c for c in cols if c != "content"]
    col_str = ", ".join(f'"{c}"' for c in cols)
    cur.execute(f"SELECT {col_str} FROM live_jobs ORDER BY posted_at DESC;")
    columns = [desc[0] for desc in cur.description]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        while True:
            rows = cur.fetchmany(5000)
            if not rows:
                break
            writer.writerows(rows)

    cur.execute("SELECT COUNT(*) FROM live_jobs;")
    count = cur.fetchone()[0]
    conn.close()
    print(f"Exported {count:,} enriched jobs to '{csv_path}' (include_content={include_content}).")


def enrich_sqlite_db(
    db_path: str,
    export_csv: str | None = None,
    include_content_in_csv: bool = False,
    workers: int | None = None,
) -> int:
    """Enriches all jobs in an existing SQLite database in-place with multi-core parallelism."""
    import concurrent.futures

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Ensure all enriched columns exist
    cur.execute("PRAGMA table_info(live_jobs);")
    existing_cols = {r["name"] for r in cur.fetchall()}
    columns_to_add = [
        ("salary", "TEXT"),
        ("salary_min", "REAL"),
        ("salary_max", "REAL"),
        ("salary_currency", "TEXT"),
        ("salary_period", "TEXT"),
        ("skills", "TEXT"),
        ("skills_required", "TEXT"),
        ("skills_preferred", "TEXT"),
        ("work_mode", "TEXT"),
        ("experience_min", "REAL"),
        ("experience_max", "REAL"),
        ("education", "TEXT"),
        ("h1b_status", "TEXT"),
        ("visa_sponsorship_status", "TEXT"),
        ("sponsorship_evidence", "TEXT"),
        ("reports_to", "TEXT"),
    ]
    for col_name, col_type in columns_to_add:
        if col_name not in existing_cols:
            cur.execute(f"ALTER TABLE live_jobs ADD COLUMN {col_name} {col_type};")
    conn.commit()

    cur.execute("SELECT * FROM live_jobs;")
    jobs = [dict(r) for r in cur.fetchall()]
    total = len(jobs)
    if total == 0:
        print("No jobs found in database.")
        conn.close()
        return 0

    if workers is None:
        workers = min(os.cpu_count() or 4, 8)

    print(f"Enriching {total:,} jobs in {db_path} (Parallel Workers: {workers})...")

    update_sql = """
        UPDATE live_jobs SET
            salary = :salary,
            salary_min = :salary_min,
            salary_max = :salary_max,
            salary_currency = :salary_currency,
            salary_period = :salary_period,
            skills = :skills,
            skills_required = :skills_required,
            skills_preferred = :skills_preferred,
            work_mode = :work_mode,
            experience_min = :experience_min,
            experience_max = :experience_max,
            education = :education,
            h1b_status = :h1b_status,
            visa_sponsorship_status = :visa_sponsorship_status,
            sponsorship_evidence = :sponsorship_evidence,
            reports_to = :reports_to
        WHERE job_id = :job_id;
    """

    has_salary = 0
    has_skills = 0
    has_h1b = 0
    processed_count = 0
    batch_size = 2000

    # Multi-process parallel mapping
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            # Chunk generator
            for i in range(0, total, batch_size):
                chunk = jobs[i : i + batch_size]
                chunk_results = list(executor.map(_enrich_single_record_wrapper, chunk, chunksize=100))
                cur.executemany(update_sql, chunk_results)
                conn.commit()

                for clean in chunk_results:
                    if clean.get("salary_min") or clean.get("salary"):
                        has_salary += 1
                    if clean.get("skills"):
                        has_skills += 1
                    if clean.get("h1b_status") in ("yes", "no", "conditional"):
                        has_h1b += 1

                processed_count += len(chunk_results)
                print(
                    f"[{processed_count:,}/{total:,}] Enriched | Salaries: {has_salary:,} | Skills: {has_skills:,} | H-1B: {has_h1b:,}",
                    flush=True,
                )
    except Exception as e:
        print(f"Parallel executor notice ({e}), falling back to single-threaded batching...")
        batch = []
        for idx, j in enumerate(jobs, 1):
            clean = _enrich_single_record_wrapper(j)
            batch.append(clean)
            if clean.get("salary_min") or clean.get("salary"):
                has_salary += 1
            if clean.get("skills"):
                has_skills += 1
            if clean.get("h1b_status") in ("yes", "no", "conditional"):
                has_h1b += 1

            if len(batch) >= 500 or idx == total:
                cur.executemany(update_sql, batch)
                conn.commit()
                batch.clear()
                print(
                    f"[{idx:,}/{total:,}] Enriched | Salaries: {has_salary:,} | Skills: {has_skills:,} | H-1B: {has_h1b:,}",
                    flush=True,
                )

    conn.close()
    print(f"\nDone! Database {db_path} enriched successfully.")
    print(f"  • With Identified Compensation: {has_salary:,} / {total:,}")
    print(f"  • With Extracted Skills:        {has_skills:,} / {total:,}")
    print(f"  • With H-1B / Visa Decisions:   {has_h1b:,} / {total:,}")

    if export_csv:
        export_sqlite_to_csv(db_path, export_csv, include_content=include_content_in_csv)

    return total


def main():
    parser = argparse.ArgumentParser(description="Job Description Enrichment Engine")
    parser.add_argument("--csv", help="Input CSV file to enrich")
    parser.add_argument("--output", help="Output enriched CSV path (defaults to <name>_enriched.csv)")
    parser.add_argument("--db", help="SQLite database to enrich in-place")
    parser.add_argument("--export-csv", help="Optional path to export enriched SQLite database to CSV")
    parser.add_argument("--include-content", action="store_true", help="Include full raw HTML descriptions in exported CSV")
    parser.add_argument("--workers", type=int, default=None, help="Number of parallel worker processes")
    args = parser.parse_args()

    if args.db:
        enrich_sqlite_db(
            args.db,
            export_csv=args.export_csv,
            include_content_in_csv=args.include_content,
            workers=args.workers,
        )
    elif args.csv:
        out = args.output or args.csv.replace(".csv", "_enriched.csv")
        enrich_csv_file(args.csv, out)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
