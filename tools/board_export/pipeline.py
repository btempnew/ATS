"""Partition, crawl, and merge the discovered US iCIMS and Taleo board catalogs."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]
PROVIDERS = ("icims", "taleo")
FIELDS = [
    "source_board", "company", "job_title", "location", "countries", "posted_at",
    "job_type", "work_mode", "is_remote", "salary_min", "salary_max",
    "salary_currency", "salary_period", "skills", "description", "job_url",
    "apply_url", "external_id", "board_id", "country_evidence", "structured_fields",
]


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def board_id(provider, url):
    host = url.split("//", 1)[-1].split("/", 1)[0].lower()
    if provider == "icims":
        if host.startswith("careers-") and host.endswith(".icims.com"):
            return host[len("careers-"):-len(".icims.com")]
        return host
    marker = "/careersection/"
    section = url.split(marker, 1)[1].split("/", 1)[0] if marker in url else ""
    return f"{host}/{section}" if section else ""


def catalog():
    rows, seen = [], set()
    for provider, filename in (("icims", "us_icims_companies.csv"),
                               ("taleo", "us_taleo_companies.csv")):
        path = REPO / "exports" / filename
        with path.open(encoding="utf-8-sig", newline="") as handle:
            for item in csv.DictReader(handle):
                board = board_id(provider, item.get("board_url", ""))
                key = (provider, board.casefold())
                if not board or key in seen:
                    continue
                seen.add(key)
                rows.append({"provider": provider, "board": board,
                             "company": item.get("company_name") or item.get("slug") or board,
                             "status": "pending", "provenance": filename})
    return rows


def prepare(args):
    boards = catalog()
    slices = []
    for provider in PROVIDERS:
        group = [row for row in boards if row["provider"] == provider]
        if args.max_boards:
            group = group[:args.max_boards]
        for offset in range(0, len(group), args.boards_per_slice):
            sid = f"{provider}-{offset // args.boards_per_slice:03d}"
            path = args.output / "slices" / f"{sid}.json"
            write_json(path, group[offset:offset + args.boards_per_slice])
            slices.append({"id": sid, "provider": provider, "boards": len(group[offset:offset + args.boards_per_slice])})
    matrix = {"include": [{"id": row["id"], "provider": row["provider"]} for row in slices]}
    manifest = {"slices": slices, "matrix": matrix,
                "catalog_hash": hashlib.sha256(json.dumps(boards, sort_keys=True).encode()).hexdigest()[:16],
                "boards": {provider: sum(row["boards"] for row in slices if row["provider"] == provider)
                           for provider in PROVIDERS}}
    write_json(args.output / "manifest.json", manifest)
    if args.github_output:
        with args.github_output.open("a") as handle:
            handle.write("matrix=" + json.dumps(matrix, separators=(",", ":")) + "\n")
    print(json.dumps({"slices": len(slices), "boards": manifest["boards"]}))


def crawl(args):
    args.output.mkdir(parents=True, exist_ok=True)
    jobs = args.output / "jobs.json"
    report = args.output / "crawl-report.json"
    command = [str(args.binary.resolve()), "--catalog", str(args.catalog.resolve()),
               "--providers", args.provider, "--output", str(jobs.resolve()),
               "--report", str(report.resolve()), "--state", str(args.state.resolve()),
               "--workers", str(args.workers), "--board-timeout", args.board_timeout]
    code = subprocess.run(command).returncode
    if not jobs.exists() or not report.exists():
        raise RuntimeError("crawler produced no output/report")
    data = json.loads(jobs.read_text())
    result = json.loads(report.read_text())
    result.update(slice_id=args.slice_id, process_exit_code=code, exported_jobs=len(data))
    write_json(report, result)
    with (args.output / "jobs.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in data:
            writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value
                             for key, value in row.items()})
    print(json.dumps({"slice": args.slice_id, "jobs": len(data), "failed_boards": result.get("failed_boards", 0)}))
    # Successful rows are still useful when stale boards fail; coverage remains in the report.
    return 0


def merge(args):
    manifest = json.loads(args.manifest.read_text())
    args.output.mkdir(parents=True, exist_ok=True)
    files = {provider: (args.output / f"{provider}_usa_jobs.csv").open("w", encoding="utf-8-sig", newline="")
             for provider in PROVIDERS}
    writers = {provider: csv.DictWriter(handle, fieldnames=FIELDS) for provider, handle in files.items()}
    for writer in writers.values():
        writer.writeheader()
    seen = set()
    counts = {provider: 0 for provider in PROVIDERS}
    reports, missing = [], []
    try:
        for item in manifest["slices"]:
            folder = args.input / f"ats-slice-{item['id']}"
            jobs_path, report_path = folder / "jobs.json", folder / "crawl-report.json"
            if not jobs_path.exists() or not report_path.exists():
                missing.append(item["id"])
                continue
            reports.append(json.loads(report_path.read_text()))
            for row in json.loads(jobs_path.read_text()):
                provider = row.get("source_board", "").lower()
                key = (provider, row.get("job_url", "").split("#", 1)[0])
                if provider not in writers or not key[1] or key in seen:
                    continue
                seen.add(key)
                writers[provider].writerow({field: json.dumps(row.get(field), ensure_ascii=False)
                                             if isinstance(row.get(field), (dict, list)) else row.get(field, "")
                                             for field in FIELDS})
                counts[provider] += 1
    finally:
        for handle in files.values():
            handle.close()
    summary = {"jobs": counts, "total_jobs": sum(counts.values()),
               "boards_expected": manifest["boards"], "slices_expected": len(manifest["slices"]),
               "slices_received": len(reports), "missing_slices": missing,
               "failed_or_partial_boards": sum(row.get("failed_boards", 0) for row in reports),
               "catalog_hash": manifest["catalog_hash"]}
    write_json(args.output / "summary.json", summary)
    write_json(args.output / "board-results.json", [board for report in reports for board in report.get("boards", [])])
    print(json.dumps(summary, indent=2))
    return 1 if missing else 0


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--boards-per-slice", type=int, default=100)
    p.add_argument("--max-boards", type=int, default=0)
    p.add_argument("--github-output", type=Path)
    p = sub.add_parser("crawl")
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--catalog", type=Path, required=True)
    p.add_argument("--provider", choices=PROVIDERS, required=True)
    p.add_argument("--slice-id", required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--state", type=Path, required=True)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--board-timeout", default="5m")
    p = sub.add_parser("merge")
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    return {"prepare": prepare, "crawl": crawl, "merge": merge}[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
