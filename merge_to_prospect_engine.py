#!/usr/bin/env python3
"""Merge contacts from emory_med.db (GitHub) into prospect_engine's emory_to_med_school.db.

Designed to run on EC2 via cron after git-pulling the latest emory_med.db.
Maps our clean schema into prospect_engine's fuller schema, deduplicating
by (first_name, last_name).
"""
import sqlite3
import sys
from pathlib import Path

# Paths on EC2
GITHUB_DB = Path(__file__).parent / "emory_med.db"
PROSPECT_DB = Path("/home/ubuntu/apps/prospect_engine/backend/profiles/emory_to_med_school.db")


def merge(github_db: Path = GITHUB_DB, prospect_db: Path = PROSPECT_DB):
    if not github_db.exists():
        print(f"Source DB not found: {github_db}")
        sys.exit(1)
    if not prospect_db.exists():
        print(f"Target DB not found: {prospect_db}")
        sys.exit(1)

    src = sqlite3.connect(str(github_db))
    src.row_factory = sqlite3.Row
    dst = sqlite3.connect(str(prospect_db))
    dst.row_factory = sqlite3.Row

    # Build company name/domain -> id map in target
    companies_added = 0
    contacts_added = 0
    contacts_skipped = 0

    src_companies = src.execute("SELECT * FROM companies").fetchall()
    company_map = {}  # src_id -> dst_id

    for sc in src_companies:
        # Check if company already exists in target by name or domain
        existing = None
        if sc["domain"]:
            existing = dst.execute(
                "SELECT id FROM companies WHERE domain = ?", (sc["domain"],)
            ).fetchone()
        if not existing and sc["name"]:
            existing = dst.execute(
                "SELECT id FROM companies WHERE name = ?", (sc["name"],)
            ).fetchone()

        if existing:
            company_map[sc["id"]] = existing["id"]
        else:
            cursor = dst.execute(
                """INSERT INTO companies (name, domain, industry, size, state, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (sc["name"], sc["domain"], sc["industry"], sc["size"], sc.get("state"), sc["created_at"]),
            )
            company_map[sc["id"]] = cursor.lastrowid
            companies_added += 1

    # Merge contacts — dedup by (first_name, last_name)
    src_contacts = src.execute("SELECT * FROM contacts").fetchall()

    for ct in src_contacts:
        dst_company_id = company_map.get(ct["company_id"])
        if not dst_company_id:
            contacts_skipped += 1
            continue

        existing = dst.execute(
            "SELECT id FROM contacts WHERE first_name = ? AND last_name = ?",
            (ct["first_name"], ct["last_name"]),
        ).fetchone()

        if existing:
            # Update fields that are NULL in target but populated in source
            target = dst.execute(
                "SELECT * FROM contacts WHERE id = ?", (existing["id"],)
            ).fetchone()
            updates = {}
            for field in ("job_title", "prospect_notes"):
                if not target[field] and ct.get(field):
                    updates[field] = ct[field]
            # Upgrade lifecycle_stage from lead to verified
            if target["lifecycle_stage"] == "lead" and ct.get("lifecycle_stage") == "verified":
                updates["lifecycle_stage"] = "verified"
            if updates:
                set_clause = ", ".join(f"{k} = ?" for k in updates)
                dst.execute(
                    f"UPDATE contacts SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (*updates.values(), existing["id"]),
                )
            contacts_skipped += 1
        else:
            # Check email uniqueness
            if ct["email"]:
                email_exists = dst.execute(
                    "SELECT id FROM contacts WHERE email = ?", (ct["email"],)
                ).fetchone()
                if email_exists:
                    contacts_skipped += 1
                    continue

            dst.execute(
                """INSERT INTO contacts (first_name, last_name, email, job_title,
                   lifecycle_stage, prospect_notes, company_id, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (ct["first_name"], ct["last_name"], ct["email"], ct.get("job_title"),
                 ct.get("lifecycle_stage", "lead"), ct.get("prospect_notes"),
                 dst_company_id, ct["created_at"]),
            )
            contacts_added += 1

    dst.commit()
    src.close()
    dst.close()

    print(f"Merge complete: +{companies_added} companies, +{contacts_added} contacts, {contacts_skipped} skipped")
    return contacts_added


if __name__ == "__main__":
    merge()
