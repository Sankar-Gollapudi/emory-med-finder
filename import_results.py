#!/usr/bin/env python3
"""Import search results from a JSON file into the database.

Used by the scheduled agent: it does web searches itself, writes results
to a JSON file, then calls this script to import them with dedup.

Usage:
    python3 import_results.py results.json [--db-path emory_med.db]
"""
import argparse
import json
import sys
import os

# Reuse all the DB/import logic from search.py
from search import init_db, import_leads, get_existing_names, get_school_coverage, SEARCH_VERTICALS, validate_lead


def main():
    parser = argparse.ArgumentParser(description="Import search results into DB")
    parser.add_argument("json_file", help="Path to JSON file with leads")
    parser.add_argument("--db-path", default="emory_med.db", help="SQLite database path")
    args = parser.parse_args()

    if not os.path.exists(args.json_file):
        print(f"File not found: {args.json_file}")
        sys.exit(1)

    with open(args.json_file) as f:
        leads = json.load(f)

    if not isinstance(leads, list):
        print(f"Expected a JSON array, got {type(leads).__name__}")
        sys.exit(1)

    init_db(args.db_path)

    print(f"Importing {len(leads)} leads into {args.db_path}...")
    imported, skipped, rejected, details = import_leads(leads, args.db_path)

    print(f"\nImported: {imported}")
    print(f"Skipped (duplicates): {skipped}")
    print(f"Rejected: {rejected}")
    if details:
        for d in details[:10]:
            print(f"  {d}")

    # Git commit and push if new leads were added
    if imported > 0:
        import subprocess
        try:
            subprocess.run(["git", "add", args.db_path], check=True, capture_output=True)
            subprocess.run(
                ["git", "commit", "-m", f"Add {imported} new Emory med school leads"],
                check=True, capture_output=True,
            )
            subprocess.run(["git", "push"], check=True, capture_output=True)
            print(f"Pushed updated DB to GitHub ({imported} new leads)")
        except subprocess.CalledProcessError as e:
            print(f"Git push failed: {e.stderr.decode()[:200] if e.stderr else str(e)}")


if __name__ == "__main__":
    main()
