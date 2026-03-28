# Emory Med School Finder

Standalone CLI that finds Emory University undergrad alumni who went to top-20 US medical schools.

## How to run

```bash
python3 search.py --num-leads 10 --max-workers 4
```

This runs a 3-phase pipeline:
1. **Discovery** — parallel Claude CLI calls search the web for Emory→med school alumni
2. **Verification** — each person is verified via a quick web search
3. **Import** — verified leads are stored in `emory_med.db` with dedup by (first_name, last_name)

Results accumulate across runs. The UNIQUE INDEX on (first_name, last_name) prevents duplicates.

## For scheduled runs

The scheduled agent should run:
```bash
python3 search.py --num-leads 10
```

Results are printed to stdout at the end. The database is the source of truth.
