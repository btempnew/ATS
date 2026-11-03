#!/usr/bin/env python3
"""Publish a harvested SQLite dataset to the Getworkin Neon catalog.

The GitHub artifact remains the durable fallback.  Catalog cleanup is bounded to
inactive or old jobs and never touches user/application/hiring-post tables.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
import sqlite3
from typing import Any, Iterable


CATALOG_COLUMNS = (
    "catalog_key", "source_project", "source_job_id", "job_title", "company",
    "location", "country", "posted_at", "first_seen_at", "source_board",
    "job_type", "work_mode", "salary_min", "salary_max", "salary_currency",
    "salary_period", "industry", "h1b_status", "visa_sponsorship_status",
    "poster_name", "poster_title", "poster_profile_url", "poster_email",
    "job_url", "apply_url", "status", "canonical_identity", "content_hash",
    "source_updated_at",
)

DETAIL_COLUMNS = (
    "catalog_key", "description", "extraction", "skills_required",
    "skills_preferred", "experience_min", "experience_max",
    "basic_qualifications", "preferred_qualifications", "key_responsibilities",
    "raw_salary", "content_hash",
)

CATALOG_CONFLICT = """
ON CONFLICT (catalog_key) DO UPDATE SET
  job_title=EXCLUDED.job_title, company=EXCLUDED.company,
  location=EXCLUDED.location, country=EXCLUDED.country,
  posted_at=COALESCE(EXCLUDED.posted_at, job_catalog.posted_at),
  source_board=EXCLUDED.source_board, job_type=EXCLUDED.job_type,
  work_mode=EXCLUDED.work_mode, salary_min=EXCLUDED.salary_min,
  salary_max=EXCLUDED.salary_max, salary_currency=EXCLUDED.salary_currency,
  salary_period=EXCLUDED.salary_period, industry=EXCLUDED.industry,
  h1b_status=EXCLUDED.h1b_status,
  visa_sponsorship_status=EXCLUDED.visa_sponsorship_status,
  job_url=EXCLUDED.job_url, apply_url=EXCLUDED.apply_url, status='active',
  canonical_identity=EXCLUDED.canonical_identity,
  content_hash=EXCLUDED.content_hash,
  source_updated_at=EXCLUDED.source_updated_at, synced_at=now()
"""

DETAIL_CONFLICT = """
ON CONFLICT (catalog_key) DO UPDATE SET
  description=CASE WHEN EXCLUDED.description <> '' THEN EXCLUDED.description
                   ELSE job_details.description END,
  extraction=EXCLUDED.extraction,
  skills_required=EXCLUDED.skills_required,
  skills_preferred=EXCLUDED.skills_preferred,
  experience_min=EXCLUDED.experience_min,
  experience_max=EXCLUDED.experience_max,
  raw_salary=EXCLUDED.raw_salary,
  content_hash=EXCLUDED.content_hash, updated_at=now()
"""


def text(value: Any) -> str:
    return str(value or "").strip()


def number(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def timestamp(value: Any) -> str | None:
    value = text(value)
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return None


def string_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [text(item) for item in value if text(item)]
    raw = text(value)
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [text(item) for item in parsed if text(item)]
        except json.JSONDecodeError:
            pass
    return [part.strip() for part in raw.split(",") if part.strip()]


def stable_hash(payload: Any) -> str:
    return sha256(json.dumps(payload, sort_keys=True, default=str,
                             separators=(",", ":")).encode()).hexdigest()


def transform(row: dict[str, Any], source: str) -> tuple[dict[str, Any], dict[str, Any]]:
    platform = text(row.get("platform")).lower() or "direct"
    raw_id = text(row.get("job_id"))
    source_id = f"{platform}:{raw_id}"
    key = f"{source}:{source_id}"
    title = text(row.get("title")) or "Untitled role"
    company = text(row.get("company_name") or row.get("company_slug"))
    location = text(row.get("location"))
    description = text(row.get("content"))
    required = string_list(row.get("skills_required") or row.get("skills"))
    preferred = string_list(row.get("skills_preferred"))
    extraction = {
        "department": text(row.get("department")) or None,
        "office": text(row.get("office")) or None,
        "education": text(row.get("education")) or None,
        "reports_to": text(row.get("reports_to")) or None,
        "harvest_enriched": bool(row.get("is_enriched")),
    }
    extraction = {key: value for key, value in extraction.items() if value is not None}
    payload_hash = stable_hash({
        "title": title, "company": company, "location": location,
        "description": description, "apply_url": row.get("apply_url"),
        "posted_at": row.get("posted_at"), "extraction": extraction,
    })
    identity = stable_hash([company.lower(), title.lower(), location.lower(),
                            text(row.get("apply_url")).lower()])
    now = datetime.now(timezone.utc).isoformat()
    card = {
        "catalog_key": key,
        "source_project": source,
        "source_job_id": source_id,
        "job_title": title,
        "company": company,
        "location": location,
        "country": "United States" if row.get("is_us") else None,
        "posted_at": timestamp(row.get("posted_at")),
        "first_seen_at": timestamp(row.get("extracted_at")) or now,
        "source_board": platform,
        "job_type": text(row.get("employment_type")) or None,
        "work_mode": text(row.get("work_mode")) or None,
        "salary_min": number(row.get("salary_min")),
        "salary_max": number(row.get("salary_max")),
        "salary_currency": text(row.get("salary_currency")) or None,
        "salary_period": text(row.get("salary_period")) or None,
        "industry": None,
        "h1b_status": text(row.get("h1b_status")) or "unknown",
        "visa_sponsorship_status": text(row.get("visa_sponsorship_status")) or "unknown",
        "poster_name": None, "poster_title": None,
        "poster_profile_url": None, "poster_email": None,
        "job_url": text(row.get("apply_url")) or None,
        "apply_url": text(row.get("apply_url")) or None,
        "status": "active",
        "canonical_identity": identity,
        "content_hash": payload_hash,
        "source_updated_at": timestamp(row.get("updated_at") or row.get("extracted_at")) or now,
    }
    detail = {
        "catalog_key": key, "description": description,
        "extraction": extraction, "skills_required": required,
        "skills_preferred": preferred,
        "experience_min": number(row.get("experience_min")),
        "experience_max": number(row.get("experience_max")),
        "basic_qualifications": None, "preferred_qualifications": None,
        "key_responsibilities": None,
        "raw_salary": {"text": text(row.get("salary"))} if row.get("salary") else None,
        "content_hash": payload_hash,
    }
    return card, detail


def bulk_upsert(connection, table: str, columns: tuple[str, ...],
                rows: Iterable[dict[str, Any]], conflict: str) -> None:
    from psycopg.types.json import Jsonb

    values = list(rows)
    if not values:
        return
    placeholders = "(" + ",".join(["%s"] * len(columns)) + ")"
    statement = (f"INSERT INTO public.{table} ({', '.join(columns)}) VALUES "
                 + ",".join([placeholders] * len(values)) + " " + conflict)
    parameters: list[Any] = []
    json_fields = {"extraction", "skills_required", "skills_preferred", "raw_salary"}
    for row in values:
        parameters.extend(Jsonb(row[col]) if col in json_fields and row[col] is not None
                          else row[col] for col in columns)
    connection.execute(statement, parameters)


def new_or_owned_cards(connection, cards: list[dict[str, Any]],
                       details: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Skip jobs already represented by another source instead of duplicating cards."""
    identities = [card["canonical_identity"] for card in cards]
    keys = [card["catalog_key"] for card in cards]
    existing = connection.execute(
        "SELECT catalog_key, canonical_identity FROM public.job_catalog "
        "WHERE catalog_key = ANY(%s) OR canonical_identity = ANY(%s)",
        (keys, identities),
    ).fetchall()
    owned = {row["catalog_key"] for row in existing if row["catalog_key"] in keys}
    represented = {row["canonical_identity"] for row in existing
                   if row["catalog_key"] not in owned}
    keep_indexes = [index for index, card in enumerate(cards)
                    if card["catalog_key"] in owned
                    or card["canonical_identity"] not in represented]
    return ([cards[index] for index in keep_indexes],
            [details[index] for index in keep_indexes],
            len(cards) - len(keep_indexes))


def prune(connection, retention_days: int, max_rows: int) -> dict[str, int]:
    """Reclaim reusable pages without touching posts, profiles, or applications."""
    inactive = connection.execute(
        "DELETE FROM public.job_catalog WHERE lower(coalesce(status,'active')) "
        "IN ('expired','deleted','closed') RETURNING 1"
    ).rowcount
    old = connection.execute(
        "DELETE FROM public.job_catalog WHERE "
        "coalesce(posted_at, first_seen_at, synced_at) < "
        "now() - make_interval(days => %s) RETURNING 1",
        (retention_days,),
    ).rowcount
    overflow = 0
    if max_rows > 0:
        total = connection.execute(
            "SELECT count(*) AS total FROM public.job_catalog"
        ).fetchone()["total"]
        excess = max(0, total - max_rows)
        if excess:
            overflow = connection.execute(
                "DELETE FROM public.job_catalog WHERE catalog_key IN ("
                "SELECT catalog_key FROM public.job_catalog "
                "ORDER BY coalesce(posted_at, first_seen_at, synced_at) ASC NULLS FIRST "
                "LIMIT %s) RETURNING 1", (excess,)
            ).rowcount
    connection.commit()
    return {"inactive": inactive, "older_than_retention": old, "overflow": overflow}


def prune_overflow(connection, max_rows: int) -> int:
    """Keep incremental inserts inside the row budget during a large harvest."""
    if max_rows <= 0:
        return 0
    total = connection.execute(
        "SELECT count(*) AS total FROM public.job_catalog"
    ).fetchone()["total"]
    excess = max(0, total - max_rows)
    if not excess:
        return 0
    removed = connection.execute(
        "DELETE FROM public.job_catalog WHERE catalog_key IN ("
        "SELECT catalog_key FROM public.job_catalog "
        "ORDER BY coalesce(posted_at, first_seen_at, synced_at) ASC NULLS FIRST "
        "LIMIT %s) RETURNING 1", (excess,)
    ).rowcount
    connection.commit()
    return removed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="live_jobs.db")
    parser.add_argument("--source-project", default="ats-harvest")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--retention-days", type=int, default=30)
    parser.add_argument("--max-catalog-rows", type=int, default=450_000)
    args = parser.parse_args()
    url = os.environ.get("JOB_CATALOG_DATABASE_URL", "").strip()
    if not url:
        raise SystemExit("JOB_CATALOG_DATABASE_URL is required")
    if not os.path.exists(args.db):
        raise SystemExit(f"Harvest database not found: {args.db}")

    import psycopg
    from psycopg.rows import dict_row

    source = sqlite3.connect(args.db)
    source.row_factory = sqlite3.Row
    target = psycopg.connect(url, row_factory=dict_row)
    try:
        required = target.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' "
            "AND table_name IN ('job_catalog','job_details')"
        ).fetchone()["count"]
        if required != 2:
            raise RuntimeError("Neon job_catalog/job_details schema is missing")
        cleanup = prune(target, args.retention_days, args.max_catalog_rows)
        total = inserted = duplicates = 0
        cursor = source.execute("SELECT * FROM live_jobs ORDER BY job_id")
        while True:
            raw_rows = cursor.fetchmany(args.batch_size)
            if not raw_rows:
                break
            transformed = [transform(dict(row), args.source_project) for row in raw_rows]
            cards = [item[0] for item in transformed]
            details = [item[1] for item in transformed]
            cards, details, skipped = new_or_owned_cards(target, cards, details)
            bulk_upsert(target, "job_catalog", CATALOG_COLUMNS, cards, CATALOG_CONFLICT)
            bulk_upsert(target, "job_details", DETAIL_COLUMNS, details, DETAIL_CONFLICT)
            target.commit()
            total += len(raw_rows)
            inserted += len(cards)
            duplicates += skipped
            if inserted and inserted % 10_000 < len(cards):
                cleanup["overflow"] += prune_overflow(target, args.max_catalog_rows)
            print(json.dumps({"processed": total, "written": inserted,
                              "duplicates_skipped": duplicates}), flush=True)
        final_cleanup = prune(target, args.retention_days, args.max_catalog_rows)
        stats = target.execute(
            "SELECT count(*) AS catalog_rows, pg_database_size(current_database()) AS database_bytes "
            "FROM public.job_catalog"
        ).fetchone()
        report = {"harvest_rows": total, "upserted": inserted,
                  "duplicates_skipped": duplicates, "cleanup_before": cleanup,
                  "cleanup_after": final_cleanup,
                  **dict(stats)}
        print(json.dumps(report, default=str), flush=True)
        summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary:
            with open(summary, "a", encoding="utf-8") as handle:
                handle.write("\n### Neon catalog sync\n\n")
                handle.write(f"- Harvest rows upserted: **{inserted:,}**\n")
                removed = sum(cleanup.values()) + sum(final_cleanup.values())
                handle.write(f"- Existing duplicates skipped: **{duplicates:,}**\n")
                handle.write(f"- Old/inactive rows pruned: **{removed:,}**\n")
                handle.write(f"- Catalog rows after sync: **{stats['catalog_rows']:,}**\n")
        return 0
    finally:
        source.close()
        target.close()


if __name__ == "__main__":
    raise SystemExit(main())
