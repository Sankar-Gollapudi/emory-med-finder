# Emory Med School Finder

Finds Emory University undergrad alumni who went to top-20 US medical schools.

## For scheduled runs (YOU ARE THE SEARCH ENGINE)

Do NOT run search.py — it uses subprocess calls that don't work in this environment.
Instead, YOU do the searching directly using your WebSearch and WebFetch tools, then import results.

### Step 1: Check what we need

```bash
python3 -c "
from search import get_existing_names, get_school_coverage, pick_verticals_for_run
names = get_existing_names('emory_med.db')
print(f'Existing contacts: {len(names)}')
for n in names: print(f'  - {n}')
print()
verticals = pick_verticals_for_run('emory_med.db', count=3)
print('Schools to search this run:')
for v in verticals: print(f'  - {v}')
"
```

### Step 2: Search the web yourself

For each school listed above, use WebSearch and WebFetch to find Emory University undergraduate alumni who went to that medical school. Requirements:
- Must be a verified Emory University UNDERGRADUATE alumnus (not grad school)
- Must be at a top-20 US medical school
- Must have a verifiable web presence (LinkedIn, university page, news article)
- Use their FULL LEGAL NAME (not initials or nicknames)
- Skip anyone already in the existing contacts list
- STRONGLY PREFER recent graduates and current students (Emory class of 2018-2026, currently in med school or recently matched into residency)
- Faculty and attendings who graduated Emory 10+ years ago are LOWER PRIORITY — only include them if you can't find current students

Search strategies (prioritize recent sources):
- LinkedIn: "Emory University" + "[med school name]" + "MD candidate" or "medical student"
- Match Day announcements from the last 3 years (2024, 2025, 2026) — these list students by name and where they matched
- Google: "Emory" + "[med school name]" + "class of 2025" or "class of 2026" or "class of 2027" or "class of 2028"
- Medical school class profile pages and student directories
- Emory pre-med advising spotlights and Emory alumni magazine features on recent grads
- AMCAS/residency match result PDFs (e.g. "VUSM_Class of 2026 Match Results")
- Student org pages, research lab member lists that show current trainees
- Emory Wheel (student newspaper) articles about students accepted to med school

### Step 3: Save results and import

Write results to a JSON file, then import:

```bash
cat > results.json << 'EOF'
[
  {
    "company_name": "Medical School Name",
    "domain": "medschool.edu",
    "industry": "Medical Education",
    "size": "1001-5000",
    "city": "City",
    "state": "ST",
    "suggested_title": "Full Name - Role at Med School",
    "prospect_notes": "4-6 sentences about their background, Emory undergrad details, med school details, research interests.",
    "_enrichment": {
      "found": true,
      "first_name": "First",
      "last_name": "Last",
      "title": "Role at Med School",
      "email": "",
      "source_url": "https://linkedin.com/in/...",
      "confidence": "high"
    }
  }
]
EOF
python3 import_results.py results.json
```

The import script handles dedup (by first_name + last_name), validation, and auto git push.

## For local/EC2 runs

search.py still works locally where the claude CLI is available:
```bash
python3 search.py --num-leads 10 --max-workers 4
```

## Database

- `emory_med.db` — SQLite with `companies` and `contacts` tables
- UNIQUE INDEX on contacts(first_name, last_name) prevents duplicates
- Results accumulate across runs
