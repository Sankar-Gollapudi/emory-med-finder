"""Tests for the Emory med school finder."""
import json
import os
import sqlite3
import tempfile

import pytest

import search


@pytest.fixture
def db_path(tmp_path):
    """Create a fresh DB and return its path."""
    path = str(tmp_path / "test.db")
    search.init_db(path)
    return path


@pytest.fixture
def sample_leads():
    """Fixture data mimicking Claude discovery output."""
    return [
        {
            "company_name": "Harvard Medical School",
            "domain": "hms.harvard.edu",
            "industry": "Medical Education",
            "size": "1001-5000",
            "city": "Boston",
            "state": "MA",
            "suggested_title": "John Smith - MD/PhD Student",
            "prospect_notes": "John Smith graduated from Emory University in 2021 with a BS in Biology and a minor in Chemistry. He conducted research in Dr. Jane Doe's immunology lab for three years. He is currently a second-year MD/PhD student at Harvard Medical School studying neuroscience.",
        },
        {
            "company_name": "Stanford School of Medicine",
            "domain": "med.stanford.edu",
            "industry": "Medical Education",
            "size": "1001-5000",
            "city": "Stanford",
            "state": "CA",
            "suggested_title": "Jane Doe - MD Candidate",
            "prospect_notes": "Jane Doe graduated from Emory University in 2022 with a BA in Neuroscience and Behavioral Biology. She was president of the Emory Pre-Med Society and volunteered at Grady Hospital. She is now a first-year MD student at Stanford School of Medicine.",
        },
    ]


@pytest.fixture
def sample_verifications():
    """Fixture data mimicking Claude verification output."""
    return [
        {
            "found": True,
            "first_name": "John",
            "last_name": "Smith",
            "title": "MD/PhD Student at Harvard Medical School",
            "source_url": "https://linkedin.com/in/johnsmith",
            "confidence": "high",
        },
        {
            "found": True,
            "first_name": "Jane",
            "last_name": "Doe",
            "title": "MD Candidate at Stanford School of Medicine",
            "source_url": "https://linkedin.com/in/janedoe",
            "confidence": "medium",
        },
    ]


class TestInitDb:
    def test_creates_tables(self, db_path):
        conn = sqlite3.connect(db_path)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "companies" in tables
        assert "contacts" in tables
        conn.close()


class TestDedup:
    def test_dedup_by_person_key(self):
        leads = [
            {"company_name": "HMS", "suggested_title": "John Smith", "prospect_notes": "A" * 100},
            {"company_name": "HMS", "suggested_title": "John Smith", "prospect_notes": "A" * 100},
            {"company_name": "HMS", "suggested_title": "Jane Doe", "prospect_notes": "B" * 100},
        ]
        result = search.dedup_leads(leads)
        assert len(result) == 2

    def test_dedup_different_people_same_org(self):
        leads = [
            {"company_name": "HMS", "suggested_title": "John Smith", "prospect_notes": "Background A" * 10},
            {"company_name": "HMS", "suggested_title": "Jane Doe", "prospect_notes": "Background B" * 10},
        ]
        result = search.dedup_leads(leads)
        assert len(result) == 2


class TestValidateLead:
    def test_valid_lead(self, sample_leads):
        assert search.validate_lead(sample_leads[0]) is None

    def test_missing_domain(self):
        lead = {"company_name": "HMS", "industry": "Medical", "prospect_notes": "A" * 30}
        assert search.validate_lead(lead) is not None

    def test_short_notes(self):
        lead = {"company_name": "HMS", "domain": "hms.edu", "industry": "Medical", "prospect_notes": "Short"}
        assert search.validate_lead(lead) is not None

    def test_invalid_domain(self):
        lead = {"company_name": "HMS", "domain": "not a domain", "industry": "Medical", "prospect_notes": "A" * 30}
        assert search.validate_lead(lead) is not None

    def test_strips_url_from_domain(self):
        lead = {"company_name": "HMS", "domain": "hms.harvard.edu/some/path", "industry": "Medical", "prospect_notes": "A" * 30}
        assert search.validate_lead(lead) is None
        assert lead["domain"] == "hms.harvard.edu"


class TestImportLeads:
    def test_imports_verified_leads(self, db_path, sample_leads, sample_verifications):
        for i, lead in enumerate(sample_leads):
            lead["_enrichment"] = sample_verifications[i]

        imported, skipped, rejected, details = search.import_leads(sample_leads, db_path)
        assert imported == 2
        assert skipped == 0
        assert rejected == 0

        conn = sqlite3.connect(db_path)
        contacts = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
        companies = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        assert contacts == 2
        assert companies == 2
        conn.close()

    def test_rejects_unverified_leads(self, db_path, sample_leads):
        # No _enrichment attached
        imported, skipped, rejected, details = search.import_leads(sample_leads, db_path)
        assert imported == 0
        assert rejected == 2

    def test_dedup_on_reimport(self, db_path, sample_leads, sample_verifications):
        for i, lead in enumerate(sample_leads):
            lead["_enrichment"] = sample_verifications[i]

        search.import_leads(sample_leads, db_path)
        # Import same leads again
        imported, skipped, rejected, details = search.import_leads(sample_leads, db_path)
        assert imported == 0
        assert skipped == 2

    def test_high_confidence_verified_stage(self, db_path, sample_leads, sample_verifications):
        sample_leads[0]["_enrichment"] = sample_verifications[0]  # high confidence
        search.import_leads([sample_leads[0]], db_path)

        conn = sqlite3.connect(db_path)
        stage = conn.execute("SELECT lifecycle_stage FROM contacts").fetchone()[0]
        assert stage == "verified"
        conn.close()

    def test_email_hash_fallback_on_collision(self, db_path, sample_verifications):
        lead1 = {
            "company_name": "HMS", "domain": "hms.harvard.edu", "industry": "Medical",
            "suggested_title": "John Smith - Student", "prospect_notes": "Background A" * 10,
            "_enrichment": {**sample_verifications[0]},
        }
        lead2 = {
            "company_name": "HMS", "domain": "hms.harvard.edu", "industry": "Medical",
            "suggested_title": "John Smith Jr - Student", "prospect_notes": "Background B" * 10,
            "_enrichment": {
                "found": True, "first_name": "John", "last_name": "Smith",
                "title": "Student", "source_url": "https://example.com", "confidence": "high",
            },
        }
        # First import succeeds normally, second should use hash fallback
        # but name dedup will catch it since same first+last name
        imported, skipped, _, _ = search.import_leads([lead1], db_path)
        assert imported == 1
        imported2, skipped2, _, _ = search.import_leads([lead2], db_path)
        assert skipped2 == 1  # deduped by name


class TestParseJson:
    def test_plain_json_array(self):
        result = search.parse_json_from_text('[{"key": "val"}]')
        assert result == [{"key": "val"}]

    def test_json_in_code_block(self):
        text = 'Here are the results:\n```json\n[{"key": "val"}]\n```\nDone.'
        result = search.parse_json_from_text(text)
        assert result == [{"key": "val"}]

    def test_json_object(self):
        result = search.parse_json_from_text('{"found": true}')
        assert result == {"found": True}

    def test_returns_none_on_garbage(self):
        assert search.parse_json_from_text("not json at all") is None

    def test_returns_none_on_empty(self):
        assert search.parse_json_from_text("") is None
