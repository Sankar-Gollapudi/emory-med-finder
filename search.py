#!/usr/bin/env python3
"""Emory Med School Finder — finds Emory undergrad alumni at top medical schools.

Standalone CLI that uses Claude Code CLI (subprocess) to search the web,
verify people, and store results in a local SQLite database with dedup.
"""
import argparse
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CLAUDE_CLI = "claude"
MAX_WORKERS = 4
DOMAIN_RE = re.compile(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

# ── SEARCH CRITERIA (extracted from BusinessProfile) ──────────────────────

DESCRIPTION = (
    "Research project tracking Emory University undergraduate alumni who went on "
    "to attend top-20 medical schools (including Ivy League med schools like Harvard "
    "Medical School, Columbia Vagelos, Penn Perelman, Cornell Weill, Yale School of "
    "Medicine, Dartmouth Geisel, and other top programs like Johns Hopkins, Stanford, "
    "UCSF, Duke, WashU, UChicago Pritzker, NYU Grossman, Baylor, Northwestern Feinberg, "
    "UCLA Geffen, UMich, Vanderbilt, UCSD, Sinai Icahn). Goal: identify their "
    "undergraduate backgrounds, majors, extracurriculars, research experience, and "
    "career trajectories."
)

TARGET = (
    "Emory University undergraduate alumni who matriculated at top-20 US medical "
    "schools. Focus on LinkedIn profiles, Emory alumni directories, medical school "
    "class profiles, and published interviews."
)

DETAILS = (
    "Collecting profiles of Emory undergrads at top med schools to understand: "
    "(1) What they studied at Emory (major, minor, honors), (2) Research labs and "
    "clinical experience during undergrad, (3) Extracurriculars and leadership roles, "
    "(4) Current stage — which year of med school, residency, or attending."
)

QUALIFICATION = (
    "Must be a verified Emory University undergraduate alumnus/alumna who is currently "
    "enrolled in or graduated from an MD or MD/PhD program at a top-20 US medical school. "
    "PhD-only students are acceptable but HIGHEST PRIORITY is current or recently graduated MD/MD-PhD students. "
    "CRITICAL: Residency does NOT count — if someone got their MD elsewhere and is only "
    "at a top school for residency or fellowship, they do NOT qualify. You must verify "
    "WHERE they earned their MD degree. "
    "CRITICAL: You must CONFIRM Emory undergrad with concrete evidence (LinkedIn, directory, "
    "article). Do not include someone just because they appear in a combined search — you need "
    "a source that explicitly states they attended Emory for undergrad. "
    "REQUIRE LinkedIn verification whenever possible — a university profile page alone is NOT enough "
    "to confirm someone is an MD student (they could be staff, postdoc, or research associate). "
    "LinkedIn shows their actual degree program. Without LinkedIn, need at least TWO other sources "
    "confirming MD/MD-PhD enrollment (e.g. class roster + news article). "
    "Do not include Emory graduate school students — only undergrad."
)

SEARCH_VERTICALS = [
    "Harvard Medical School Emory alumni",
    "Columbia Vagelos College of Physicians Emory alumni",
    "Penn Perelman School of Medicine Emory alumni",
    "Johns Hopkins School of Medicine Emory alumni",
    "Stanford School of Medicine Emory alumni",
    "Yale School of Medicine Emory alumni",
    "UCSF School of Medicine Emory alumni",
    "Duke School of Medicine Emory alumni",
    "WashU School of Medicine Emory alumni",
    "Cornell Weill Medical College Emory alumni",
]


# ── DATABASE ─────────────────────────────────────────────────────────────

def init_db(db_path: str):
    """Create tables if they don't exist."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(200) NOT NULL,
            domain VARCHAR(200),
            industry VARCHAR(100),
            size VARCHAR(50),
            city VARCHAR(100),
            state VARCHAR(100),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255),
            job_title VARCHAR(200),
            lifecycle_stage VARCHAR(50) DEFAULT 'lead',
            prospect_notes TEXT,
            source_url VARCHAR(500),
            company_id INTEGER REFERENCES companies(id),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_name
            ON contacts(first_name, last_name);
    """)
    conn.close()


# ── CLAUDE CLI ───────────────────────────────────────────────────────────

def _get_clean_env():
    return {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}


def call_claude(system_prompt: str, user_prompt: str, timeout: int = 180) -> dict:
    """Call Claude Code CLI. Returns {ok, result, error}."""
    cmd = [
        CLAUDE_CLI, "-p",
        "--output-format", "json",
        "--model", "claude-sonnet-4-6",
        "--system-prompt", system_prompt,
        "--allowedTools", "WebSearch,WebFetch",
    ]
    try:
        proc = subprocess.run(
            cmd, input=user_prompt, capture_output=True, text=True,
            timeout=timeout, env=_get_clean_env(),
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "result": "", "error": f"Timed out after {timeout}s"}
    except FileNotFoundError:
        return {"ok": False, "result": "", "error": "Claude CLI not found"}

    if proc.returncode != 0:
        return {"ok": False, "result": "", "error": f"Exit {proc.returncode}: {proc.stderr.strip()[:300]}"}

    raw = proc.stdout.strip()
    if not raw:
        return {"ok": False, "result": "", "error": "Empty output"}

    try:
        wrapper = json.loads(raw)
        if isinstance(wrapper, dict) and "result" in wrapper:
            if wrapper.get("is_error"):
                return {"ok": False, "result": "", "error": str(wrapper["result"])[:300]}
            return {"ok": True, "result": wrapper["result"], "error": ""}
    except json.JSONDecodeError:
        pass

    return {"ok": True, "result": raw, "error": ""}


# ── JSON PARSING ─────────────────────────────────────────────────────────

def parse_json_from_text(text: str):
    """Extract JSON (array or object) from text that may contain markdown."""
    if not text:
        return None
    text = text.strip()

    try:
        data = json.loads(text)
        if isinstance(data, (list, dict)):
            return data
    except json.JSONDecodeError:
        pass

    for pattern in [r'```json\s*\n(.*?)\n```', r'```\s*\n(.*?)\n```']:
        match = re.search(pattern, text, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                if isinstance(data, (list, dict)):
                    return data
            except json.JSONDecodeError:
                continue

    match = re.search(r'\[[\s\S]*\]', text)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    return None


# ── PROMPTS ──────────────────────────────────────────────────────────────

def build_discovery_system_prompt() -> str:
    return f"""You are a specialized research assistant.

RESEARCH OBJECTIVE:
{DESCRIPTION}

TARGET: {TARGET}
DETAILS: {DETAILS}

QUALIFICATION CRITERIA:
{QUALIFICATION}

CRITICAL RULES — THESE ARE HARD GATES:
- You MUST follow the qualification criteria EXACTLY. If a result does not meet ALL criteria, do NOT include it.
- Every person/organization must be REAL and have a verifiable web presence (LinkedIn, university page, news article, etc.).
- Do NOT fabricate any information. Only report facts you found evidence for.
- When researching individuals, include SPECIFIC details: their background, experience, timeline, and verifiable sources.
- prospect_notes MUST be a detailed summary of the person's background and experience — not a generic description.
- Prefer finding NAMED individuals over anonymous roles/positions. Include the person's actual name when you can find it.

You output structured JSON for CRM import."""


def get_existing_names(db_path: str) -> list[str]:
    """Load all existing contact names from the DB for exclusion."""
    import sqlite3
    if not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT first_name, last_name FROM contacts").fetchall()
    conn.close()
    return [f"{r[0]} {r[1]}" for r in rows]


def get_school_coverage(db_path: str) -> dict[str, int]:
    """Count how many contacts we have per medical school (company)."""
    import sqlite3
    if not os.path.exists(db_path):
        return {}
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT c.name, COUNT(ct.id) FROM companies c LEFT JOIN contacts ct ON ct.company_id = c.id GROUP BY c.id"
    ).fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


VERTICAL_TO_DB_KEYWORDS = {
    "Harvard Medical School Emory alumni": ["harvard"],
    "Columbia Vagelos College of Physicians Emory alumni": ["columbia", "vagelos"],
    "Penn Perelman School of Medicine Emory alumni": ["penn", "perelman", "pennsylvania"],
    "Johns Hopkins School of Medicine Emory alumni": ["johns hopkins", "hopkins"],
    "Stanford School of Medicine Emory alumni": ["stanford"],
    "Yale School of Medicine Emory alumni": ["yale"],
    "UCSF School of Medicine Emory alumni": ["ucsf"],
    "Duke School of Medicine Emory alumni": ["duke"],
    "WashU School of Medicine Emory alumni": ["washu", "washington university", "wustl"],
    "Cornell Weill Medical College Emory alumni": ["cornell", "weill"],
}


def pick_verticals_for_run(db_path: str, count: int = 3) -> list[str]:
    """Pick the least-covered schools to search this run."""
    coverage = get_school_coverage(db_path)

    # Score each vertical: lower coverage = higher priority
    scored = []
    for vertical in SEARCH_VERTICALS:
        keywords = VERTICAL_TO_DB_KEYWORDS.get(vertical, [vertical.split(" Emory")[0].split()[0].lower()])
        hits = sum(v for k, v in coverage.items()
                   if any(kw in k.lower() for kw in keywords))
        scored.append((hits, vertical))

    # Sort by coverage (ascending) — least covered schools first
    scored.sort(key=lambda x: x[0])
    picked = [v for _, v in scored[:count]]

    # If all schools have some coverage, also add creative search angles
    if scored and scored[0][0] > 0:
        picked.append(
            "Emory University pre-med alumni who matched into top medical schools — "
            "search Match Day announcements, Emory alumni magazine, Emory pre-med advising spotlights, "
            "student newspaper profiles, and department newsletters"
        )

    return picked


def build_discovery_prompt(industry_group: str, num_leads: int, region: str = "United States",
                           existing_names: list[str] | None = None) -> str:
    exclusion_block = ""
    if existing_names:
        names_list = ", ".join(existing_names)
        exclusion_block = f"""

ALREADY FOUND — DO NOT INCLUDE THESE PEOPLE (we already have them in our database):
{names_list}

You MUST find DIFFERENT people. If your search returns any of the above names, skip them and keep searching for new individuals."""

    return f"""Research and find {num_leads} REAL, VERIFIED individuals/organizations matching this category: {industry_group}
Region/scope: {region}

Target: {TARGET}
Details: {DETAILS}

QUALIFICATION CRITERIA — HARD REQUIREMENTS (every result MUST meet ALL of these):
{QUALIFICATION}
{exclusion_block}

SEARCH STRATEGY:
- Search LinkedIn for specific people matching the criteria
- Search university alumni directories, department pages, and "Where Are They Now" pages
- Search Google for "{industry_group}" with relevant keywords
- Cross-reference multiple sources to verify each result
- Only include results where you found concrete evidence
- Try DIFFERENT search queries than obvious ones — dig deeper into class profiles, student org pages, research lab member lists, residency match announcements

For each result, the company_name field should be the ORGANIZATION/INSTITUTION name (e.g. the medical school).
The suggested_title should describe the PERSON's role or status, using their FULL LEGAL NAME (e.g. "Jerry William Allen" not "J.W. Allen", "Robert Smith" not "Rob Smith"). Never use initials or abbreviations for names.
prospect_notes MUST be a DETAILED summary (4-6 sentences) of the person's background, experience, education timeline, and achievements.

Return a JSON array in a ```json code block with these fields:
company_name, domain, industry, size, city, state, suggested_title, prospect_notes

Where:
- company_name: the institution or organization name
- domain: just the domain (e.g. "example.edu"), not a full URL
- size: employee range like "11-50" or "201-500"
- suggested_title: the person's current role or status
- prospect_notes: DETAILED background summary — education history, research interests, career path, achievements. Be specific."""


VERIFY_SYSTEM = (
    "You are a person verification assistant. Given a person description, verify "
    "they are real by finding any credible source — LinkedIn, news articles, "
    "university directories, publications, etc. Do ONE web search and return the "
    "result. Be fast."
)


def build_verify_prompt(title: str, org: str, notes: str) -> str:
    return f"""Verify this person is real:
Role: {title} at {org}
Background: {notes[:200]}

Do ONE search for "{title}" "{org}" — check LinkedIn, news articles, university pages, publications, or any credible source.

Return JSON in a ```json block:
{{"found": true/false, "source_url": "https://...", "first_name": "...", "last_name": "...", "title": "...", "confidence": "high/medium/low"}}

- "source_url" must be the URL where you found evidence of this person.
- IMPORTANT: Use the person's full legal first name, NOT initials or abbreviations (e.g. "Jerry William" not "J.W.", "Robert" not "Rob").
- confidence is "high" if you found a direct profile/directory listing, "medium" if mentioned in an article or publication.
If not found in one search, return {{"found": false}}."""


# ── VALIDATION & DEDUP ───────────────────────────────────────────────────

def validate_lead(lead: dict) -> str | None:
    """Return error string if lead is invalid, None if OK."""
    if not isinstance(lead, dict):
        return f"Not a dict: {type(lead)}"
    required = {"company_name", "domain", "industry", "prospect_notes"}
    missing = required - set(lead.keys())
    if missing:
        return f"Missing: {', '.join(sorted(missing))}"
    for f in ["company_name", "industry", "prospect_notes"]:
        v = lead.get(f, "")
        if not v or not isinstance(v, str) or len(v.strip()) < 2:
            return f"'{f}' is empty or too short"
    domain = lead.get("domain", "")
    if "/" in domain:
        domain = domain.split("/")[0]
        lead["domain"] = domain
    if not DOMAIN_RE.match(domain):
        return f"Invalid domain: '{domain}'"
    if len(lead.get("prospect_notes", "")) < 20:
        return "Prospect notes too short"
    return None


def dedup_leads(leads: list[dict]) -> list[dict]:
    """Deduplicate leads by (org, title, notes[:80])."""
    seen = set()
    unique = []
    for lead in leads:
        domain = lead.get("domain", "").lower().strip()
        if "/" in domain:
            domain = domain.split("/")[0]
            lead["domain"] = domain
        key = (
            lead.get("company_name", "").lower().strip(),
            lead.get("suggested_title", "").lower().strip(),
            lead.get("prospect_notes", "")[:80].lower().strip(),
        )
        if key not in seen:
            seen.add(key)
            unique.append(lead)
    return unique


# ── IMPORT ───────────────────────────────────────────────────────────────

def import_leads(leads: list[dict], db_path: str) -> tuple[int, int, int, list[str]]:
    """Import verified leads into SQLite. Returns (imported, skipped, rejected, details)."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    imported = 0
    skipped = 0
    rejected = 0
    details = []

    for lead in leads:
        name = lead.get("company_name", "?")

        err = validate_lead(lead)
        if err:
            details.append(f"{name}: {err}")
            rejected += 1
            continue

        enrichment = lead.get("_enrichment", {})
        if not enrichment.get("found"):
            details.append(f"{name}: No verified person found")
            rejected += 1
            continue

        first_name = enrichment["first_name"]
        last_name = enrichment["last_name"]
        title = enrichment.get("title", lead.get("suggested_title", ""))
        source = enrichment.get("source_url", enrichment.get("source", ""))
        confidence = enrichment.get("confidence", "low")

        # Global name dedup — the core protection against duplicates across runs
        existing = conn.execute(
            "SELECT id FROM contacts WHERE first_name = ? AND last_name = ?",
            (first_name, last_name),
        ).fetchone()
        if existing:
            skipped += 1
            continue

        # Find or create company
        company = conn.execute(
            "SELECT id FROM companies WHERE name = ? OR domain = ?",
            (lead["company_name"], lead["domain"]),
        ).fetchone()
        if company:
            company_id = company["id"]
        else:
            cursor = conn.execute(
                "INSERT INTO companies (name, domain, industry, size, city, state) VALUES (?, ?, ?, ?, ?, ?)",
                (lead["company_name"], lead["domain"], lead["industry"],
                 lead.get("size"), lead.get("city"), lead.get("state")),
            )
            company_id = cursor.lastrowid

        email = enrichment.get("email", "")
        if not email or not EMAIL_RE.match(email):
            email = f"{first_name.lower()}.{last_name.lower()}@{lead['domain']}"

        # Email collision fallback
        if conn.execute("SELECT id FROM contacts WHERE email = ?", (email,)).fetchone():
            h = hashlib.md5(lead["prospect_notes"][:200].encode()).hexdigest()[:8]
            email = f"{first_name.lower()}.{last_name.lower()}-{h}@{lead['domain']}"
            if conn.execute("SELECT id FROM contacts WHERE email = ?", (email,)).fetchone():
                skipped += 1
                continue

        prospect_notes = lead["prospect_notes"]
        if source:
            prospect_notes += f"\n[Source: {source}]"

        is_verified = confidence in ("high", "medium") and bool(source)
        if confidence == "medium":
            title = f"[MEDIUM] {title}"
        elif confidence == "low":
            title = f"[Unverified] {title}"

        try:
            conn.execute(
                """INSERT INTO contacts (first_name, last_name, email, job_title,
                   lifecycle_stage, prospect_notes, source_url, company_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (first_name, last_name, email, title,
                 "verified" if is_verified else "lead",
                 prospect_notes, source, company_id),
            )
            imported += 1
        except sqlite3.IntegrityError:
            # UNIQUE INDEX caught a duplicate that slipped past the query check
            skipped += 1

    conn.commit()
    conn.close()
    return imported, skipped, rejected, details


# ── MAIN PIPELINE ────────────────────────────────────────────────────────

def run_search(num_leads: int = 10, max_workers: int = 4, db_path: str = "emory_med.db",
               region: str = "United States", dry_run: bool = False):
    """Run the full discovery -> verification -> import pipeline."""
    init_db(db_path)

    logger.info("Starting Emory med school search: %d leads, %d workers", num_leads, max_workers)

    # Load existing contacts so we can tell Claude to skip them
    existing_names = get_existing_names(db_path)
    if existing_names:
        logger.info("Excluding %d existing contacts from search", len(existing_names))

    # Pick the least-covered schools to focus this run
    verticals = pick_verticals_for_run(db_path, count=min(max_workers, 3))
    leads_per_vertical = max(3, (num_leads + len(verticals) - 1) // len(verticals))

    discovery_system = build_discovery_system_prompt()
    all_leads = []

    logger.info("Phase 1: Discovery — %d schools, %d leads each", len(verticals), leads_per_vertical)
    for v in verticals:
        logger.info("  Targeting: %s", v[:80])

    if dry_run:
        logger.info("DRY RUN — skipping Claude CLI calls")
        return 0, 0, 0

    # Run sequentially — avoids timeout issues in cloud environments
    for vertical in verticals:
        prompt = build_discovery_prompt(vertical, leads_per_vertical, region, existing_names)
        logger.info("Searching: %s", vertical[:60])
        result = call_claude(discovery_system, prompt, 600)
        if not result["ok"]:
            logger.warning("Discovery failed for %s: %s", vertical[:60], result["error"])
            continue
        parsed = parse_json_from_text(result["result"])
        if isinstance(parsed, list):
            all_leads.extend(parsed)
            logger.info("Got %d leads from '%s'", len(parsed), vertical[:60])

    if not all_leads:
        logger.error("No leads found from any worker")
        return 0, 0, 0

    # Dedup discovery results
    unique_leads = dedup_leads(all_leads)
    if len(unique_leads) > num_leads:
        unique_leads = unique_leads[:num_leads]
    logger.info("Phase 1 complete: %d unique leads (from %d raw)", len(unique_leads), len(all_leads))

    # Phase 2: Verification (sequential to avoid cloud timeouts)
    logger.info("Phase 2: Verifying %d people...", len(unique_leads))

    verified = 0
    failed = 0
    for i, lead in enumerate(unique_leads):
        title = lead.get("suggested_title", "")
        org = lead.get("company_name", "")
        notes = lead.get("prospect_notes", "")
        prompt = build_verify_prompt(title, org, notes)
        logger.info("Verifying %d/%d: %s", i + 1, len(unique_leads), title[:50])
        result = call_claude(VERIFY_SYSTEM, prompt, 120)
        if result["ok"]:
            parsed = parse_json_from_text(result["result"])
            if isinstance(parsed, dict) and parsed.get("found"):
                unique_leads[i]["_enrichment"] = {
                    "found": True,
                    "first_name": parsed.get("first_name", ""),
                    "last_name": parsed.get("last_name", ""),
                    "title": parsed.get("title", ""),
                    "email": "",
                    "source_url": parsed.get("source_url", ""),
                    "confidence": parsed.get("confidence", "medium"),
                }
                verified += 1
            else:
                failed += 1
        else:
            failed += 1

    logger.info("Phase 2 complete: %d verified, %d failed", verified, failed)

    # Phase 3: Import
    logger.info("Phase 3: Importing to %s...", db_path)
    imported, skipped, rejected, details = import_leads(unique_leads, db_path)

    logger.info("Done: %d imported, %d skipped (dupes), %d rejected", imported, skipped, rejected)
    if details:
        for d in details[:10]:
            logger.info("  %s", d)

    return imported, skipped, rejected


def main():
    parser = argparse.ArgumentParser(description="Emory Med School Finder")
    parser.add_argument("--num-leads", type=int, default=10, help="Number of leads to find")
    parser.add_argument("--max-workers", type=int, default=4, help="Parallel workers (max 4)")
    parser.add_argument("--db-path", default="emory_med.db", help="SQLite database path")
    parser.add_argument("--region", default="United States", help="Search region")
    parser.add_argument("--dry-run", action="store_true", help="Skip Claude CLI calls")
    args = parser.parse_args()

    imported, skipped, rejected = run_search(
        num_leads=args.num_leads,
        max_workers=args.max_workers,
        db_path=args.db_path,
        region=args.region,
        dry_run=args.dry_run,
    )

    # Print summary for the scheduled agent to see
    print(f"\n=== SEARCH COMPLETE ===")
    print(f"Imported: {imported}")
    print(f"Skipped (duplicates): {skipped}")
    print(f"Rejected: {rejected}")

    # Commit and push the updated DB so results persist across scheduled runs
    if imported > 0 and not args.dry_run:
        _commit_and_push(args.db_path, imported)


def _commit_and_push(db_path: str, imported: int):
    """Commit the updated database and push to origin."""
    try:
        subprocess.run(["git", "add", db_path], check=True, capture_output=True)
        msg = f"Add {imported} new Emory med school leads"
        subprocess.run(
            ["git", "commit", "-m", msg],
            check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "push"],
            check=True, capture_output=True,
        )
        logger.info("Pushed updated DB to GitHub (%d new leads)", imported)
    except subprocess.CalledProcessError as e:
        logger.warning("Git push failed: %s", e.stderr.decode()[:200] if e.stderr else str(e))


if __name__ == "__main__":
    main()
