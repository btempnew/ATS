#!/usr/bin/env python3
"""Rebuild derived job fields in a merged CSV before export/database upload."""

from __future__ import annotations

import argparse
import ast
import csv
import json
from pathlib import Path
from typing import Any

from job_extraction import extract_job


JSON_FIELDS = {"extraction", "structured_fields", "baseSalary", "salaryRange"}
LIST_FIELDS = {"skills_required", "skills_preferred", "education_requirements"}


def _decode(value: Any) -> Any:
    if not isinstance(value, str) or not value.strip():
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        pass
    # Scraper shard CSVs are written with csv.DictWriter, which stores nested
    # values as Python reprs ({'a': None}). Losing them here would discard the
    # board-provided job type/salary kept in extraction.source_fields.
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError, MemoryError, RecursionError):
        return value


def _prepare(row: dict[str, Any], board: str) -> dict[str, Any]:
    prepared = dict(row)
    prepared.pop("Index_No", None)
    if board:
        prepared["source_board"] = prepared.get("source_board") or board
    for field in JSON_FIELDS | LIST_FIELDS:
        if field in prepared:
            prepared[field] = _decode(prepared[field])
    if not isinstance(prepared.get("extraction"), dict):
        prepared.pop("extraction", None)
    return prepared


def _encode(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)
    return value


def enrich_csv(path: Path, board: str = "") -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    enriched = [extract_job(_prepare(row, board)) for row in rows]

    fieldnames = ["Index_No"]
    for row in enriched:
        for key in row:
            if key != "Index_No" and key not in fieldnames:
                fieldnames.append(key)

    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        for index, row in enumerate(enriched, start=1):
            encoded = {key: _encode(value) for key, value in row.items()}
            encoded["Index_No"] = index
            writer.writerow(encoded)
    temporary.replace(path)
    return len(enriched)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--board", default="")
    args = parser.parse_args()
    count = enrich_csv(args.path, args.board)
    print(f"Enriched {count} rows in {args.path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
