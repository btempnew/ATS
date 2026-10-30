"""
Extracts and combines unique Greenhouse company names from all workspace sources:
1. ats_shard_greenhouse_us.db (SQLite database - verified_boards table)
2. us_companies_by_ats_master.csv (Master CSV dataset)
3. greenhouse_companies copy.txt (Extended board dumps with # tags)

Outputs: greenhouse_companies.txt
"""

import sqlite3
import csv
import os
from pathlib import Path


def extract_combined_greenhouse_companies(
    db_path: str = "ats_shard_greenhouse_us.db",
    csv_path: str = "us_companies_by_ats_master.csv",
    copy_path: str = "greenhouse_companies copy.txt",
    output_txt: str = "greenhouse_companies.txt"
):
    combined_dict = {}

    # 1. Extract from SQLite DB (Verified active boards)
    if os.path.exists(db_path):
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute("""
            SELECT DISTINCT company_name 
            FROM verified_boards 
            WHERE company_name IS NOT NULL AND trim(company_name) != ''
        """)
        db_count = 0
        for row in cur.fetchall():
            name = row[0].strip()
            if name:
                combined_dict[name.lower()] = name
                db_count += 1
        con.close()
        print(f"Loaded {db_count} verified companies from '{db_path}'")

    # 2. Extract from Master CSV
    if os.path.exists(csv_path):
        csv_count = 0
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ats_id = row.get("ats_id", "").strip().lower()
                if ats_id in ("greenhouse", ""):
                    name = row.get("company_name", "").strip()
                    if name and name.lower() not in combined_dict:
                        combined_dict[name.lower()] = name
                        csv_count += 1
        print(f"Added {csv_count} additional companies from '{csv_path}'")

    # 3. Extract from copy.txt
    if os.path.exists(copy_path):
        copy_count = 0
        with open(copy_path, mode="r", encoding="utf-8") as f:
            for line in f:
                cleaned = line.strip().lstrip('#').strip()
                if cleaned and cleaned.lower() not in combined_dict:
                    combined_dict[cleaned.lower()] = cleaned
                    copy_count += 1
        print(f"Added {copy_count} additional companies/slugs from '{copy_path}'")

    # Sort alphabetically (case-insensitive)
    sorted_names = sorted(combined_dict.values(), key=lambda s: s.lower())

    with open(output_txt, mode="w", encoding="utf-8") as out:
        for name in sorted_names:
            out.write(name + "\n")

    print(f"\nFinal combined list saved to '{output_txt}' ({len(sorted_names)} companies).")
    return sorted_names


if __name__ == "__main__":
    extract_combined_greenhouse_companies()
