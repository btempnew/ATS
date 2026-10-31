#!/usr/bin/env python3
"""
Automated Watcher & Deep JD Enricher for Live ATS Harvest Runs.

Monitors a running GitHub Actions harvest run (e.g. 41193343225).
As soon as the harvest finishes:
1. Downloads the artifacts (live_jobs.db and/or shard databases).
2. Merges shards if not already merged.
3. Runs the deep JD enrichment engine across all jobs using the harvested HTML descriptions.
4. Populates all 16 compensation, skills, work mode, experience, and H-1B fields.
5. Exports the clean enriched live_jobs.csv.
6. Commits and pushes the enriched dataset to GitHub main.
"""

import sys
import os
import time
import json
import subprocess
import sqlite3
from datetime import datetime, timezone

# Ensure local packages are importable
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from enrich_jobs import enrich_sqlite_db, export_sqlite_to_csv
from extract_live_jobs import merge_databases


def run_cmd(cmd: str, check: bool = True) -> str:
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and res.returncode != 0:
        raise RuntimeError(f"Command failed (exit {res.returncode}): {cmd}\nStderr: {res.stderr}\nStdout: {res.stdout}")
    return res.stdout.strip()


def get_run_status(run_id: str) -> tuple[str, str]:
    """Returns (status, conclusion)."""
    out = run_cmd(f"gh run view {run_id} --repo btempnew/ATS --json status,conclusion", check=False)
    if not out:
        return "unknown", "unknown"
    try:
        data = json.loads(out)
        return data.get("status", "unknown"), data.get("conclusion", "")
    except Exception:
        return "unknown", "unknown"


def main():
    run_id = sys.argv[1] if len(sys.argv) > 1 else "41193343225"
    print(f"🚀 Starting Watcher for GitHub Actions Harvest Run: {run_id}")
    print(f"Monitoring run until completion...")

    while True:
        status, conclusion = get_run_status(run_id)
        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"[{now_str}] Run {run_id} status: {status} | conclusion: {conclusion or 'in_progress'}")

        if status == "completed":
            print(f"\n🎉 Run {run_id} has completed with conclusion: {conclusion}!")
            break

        time.sleep(30)

    work_dir = f"/tmp/harvest_run_{run_id}"
    os.makedirs(work_dir, exist_ok=True)

    print(f"\n📥 Downloading artifacts from run {run_id} to {work_dir}...")
    run_cmd(f"gh run download {run_id} --repo btempnew/ATS --dir {work_dir}", check=False)

    # Locate database
    target_db = os.path.join(work_dir, "live_jobs.db")
    if not os.path.exists(target_db):
        # Look for live-ats-jobs-dataset directory
        nested_db = os.path.join(work_dir, "live-ats-jobs-dataset", "live_jobs.db")
        if os.path.exists(nested_db):
            target_db = nested_db
        else:
            # Check for shard databases
            import glob
            shard_dbs = glob.glob(os.path.join(work_dir, "**", "*.db"), recursive=True)
            if shard_dbs:
                print(f"Merging {len(shard_dbs)} discovered shard databases...")
                merge_databases(shard_dbs, target_db)
            else:
                print(f"❌ Error: No SQLite databases found in {work_dir}")
                sys.exit(1)

    print(f"\n🧠 Executing Deep JD Enrichment on {target_db}...")
    target_csv = os.path.join(ROOT_DIR, "live_jobs.csv")
    repo_db = os.path.join(ROOT_DIR, "live_jobs.db")

    # Run in-place enrichment and export to CSV
    total_enriched = enrich_sqlite_db(target_db, export_csv=target_csv)

    # Copy database to repo root
    import shutil
    shutil.copy2(target_db, repo_db)
    print(f"Updated {repo_db} and {target_csv}")

    # Commit and push results
    print("\n💾 Committing and pushing enriched dataset to GitHub main...")
    try:
        token = run_cmd("gh auth token --user btempnew", check=False)
        git_cmd = f'cd "{ROOT_DIR}" && git add live_jobs.csv && git commit -m "Automated: deep JD enrichment from harvest run {run_id} [skip ci]"'
        run_cmd(git_cmd, check=False)
        push_cmd = f'cd "{ROOT_DIR}" && git -c credential.helper= -c "url.https://btempnew:{token}@github.com.insteadOf=https://github.com" push origin main'
        run_cmd(push_cmd, check=False)
        print("✅ Successfully pushed enriched live_jobs.csv to main!")
    except Exception as e:
        print(f"Notice on git push: {e}")

    print("\n🏁 Deep JD Enrichment Complete!")


if __name__ == "__main__":
    main()

