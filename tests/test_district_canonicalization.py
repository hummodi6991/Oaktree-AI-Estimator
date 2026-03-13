"""Tests for district label canonicalization in the expansion advisor.

Covers:
- _canonicalize_district_label helper
- Target district normalization at search creation
- Canonical fields on candidate payloads
"""
from __future__ import annotations

import pytest

from app.services.expansion_advisor import (
    _canonicalize_district_label,
    _normalize_candidate_payload,
)
from app.services.aqar_district_match import normalize_district_key


# ---------------------------------------------------------------------------
# _canonicalize_district_label
# ---------------------------------------------------------------------------

class TestCanonicalizeDistrictLabel:
    """Unit tests for the _canonicalize_district_label helper."""

    def test_none_input(self):
        result = _canonicalize_district_label(None)
        assert result["district_key"] is None
        assert result["district_name_ar"] is None
        assert result["district_name_en"] is None
        assert result["district_display"] is None

    def test_empty_string(self):
        result = _canonicalize_district_label("")
        assert result["district_key"] is None
        assert result["district_display"] is None

    def test_whitespace_only(self):
        result = _canonicalize_district_label("   ")
        assert result["district_key"] is None
        assert result["district_display"] is None

    def test_clean_arabic_without_lookup(self):
        """Clean Arabic district name should pass through."""
        result = _canonicalize_district_label("الملقا")
        assert result["district_key"] == normalize_district_key("الملقا")
        assert result["district_name_ar"] == "الملقا"
        assert result["district_name_en"] is None
        assert result["district_display"] == "الملقا"

    def test_with_lookup_hit(self):
        """Canonical lookup should provide English label."""
        lookup = {
            normalize_district_key("الملقا"): {
                "label_ar": "الملقا",
                "label_en": "Al Malqa",
            }
        }
        result = _canonicalize_district_label("الملقا", lookup)
        assert result["district_name_ar"] == "الملقا"
        assert result["district_name_en"] == "Al Malqa"
        assert result["district_display"] == "الملقا"  # Arabic preferred

    def test_with_lookup_prefers_canonical_ar(self):
        """Lookup arabic label should be used even if raw string is a variant."""
        key = normalize_district_key("حي الملقا")
        lookup = {
            key: {
                "label_ar": "الملقا",
                "label_en": "Al Malqa",
            }
        }
        result = _canonicalize_district_label("حي الملقا", lookup)
        assert result["district_name_ar"] == "الملقا"
        assert result["district_display"] == "الملقا"

    def test_arabic_diacritics_normalized(self):
        """District with أ/إ should normalize to ا in the key."""
        result = _canonicalize_district_label("الأولى")
        key = result["district_key"]
        assert "أ" not in key  # normalized away

    def test_display_fallback_to_key(self):
        """When only the key is derivable, display should show the key."""
        result = _canonicalize_district_label("some_key_value")
        assert result["district_display"] == "some_key_value"


# ---------------------------------------------------------------------------
# _normalize_candidate_payload with district fields
# ---------------------------------------------------------------------------

class TestNormalizeCandidateDistrictFields:
    """Ensure _normalize_candidate_payload injects canonical district fields."""

    def test_adds_canonical_fields(self):
        candidate = {
            "district": "العليا",
            "gate_status_json": {},
            "gate_reasons_json": {"passed": [], "failed": [], "unknown": []},
            "feature_snapshot_json": {},
            "score_breakdown_json": {},
        }
        result = _normalize_candidate_payload(candidate)
        assert "district_key" in result
        assert "district_name_ar" in result
        assert "district_name_en" in result
        assert "district_display" in result
        assert result["district_display"] == "العليا"

    def test_with_lookup_enriches_english(self):
        key = normalize_district_key("العليا")
        lookup = {
            key: {"label_ar": "العليا", "label_en": "Al Olaya"},
        }
        candidate = {
            "district": "العليا",
            "gate_status_json": {},
            "gate_reasons_json": {},
            "feature_snapshot_json": {},
            "score_breakdown_json": {},
        }
        result = _normalize_candidate_payload(candidate, lookup)
        assert result["district_name_en"] == "Al Olaya"
        assert result["district_display"] == "العليا"

    def test_no_double_canonicalize(self):
        """If district_display already exists, don't re-compute."""
        candidate = {
            "district": "SomeRawValue",
            "district_display": "Already Canonical",
            "district_key": "already_key",
            "district_name_ar": "canonical",
            "district_name_en": "Canonical",
            "gate_status_json": {},
            "gate_reasons_json": {},
            "feature_snapshot_json": {},
            "score_breakdown_json": {},
        }
        result = _normalize_candidate_payload(candidate)
        assert result["district_display"] == "Already Canonical"

    def test_none_district_produces_none_display(self):
        candidate = {
            "district": None,
            "gate_status_json": {},
            "gate_reasons_json": {},
            "feature_snapshot_json": {},
            "score_breakdown_json": {},
        }
        result = _normalize_candidate_payload(candidate)
        assert result["district_display"] is None
        assert result["district_key"] is None


# ---------------------------------------------------------------------------
# Target district normalization (API-level)
# ---------------------------------------------------------------------------

class TestTargetDistrictNormalization:
    """Validate that normalize_district_key canonicalizes user input."""

    def test_garbled_target_normalizes(self):
        """A garbled input that normalizes to a valid key should resolve."""
        # Simulates: user selected a district that has حي prefix
        raw = "حي الملقا"
        norm = normalize_district_key(raw)
        assert norm == normalize_district_key("الملقا")

    def test_deduplication(self):
        """Duplicate target districts should collapse to one entry."""
        inputs = ["الملقا", "حي الملقا", "الملقا"]
        seen: set[str] = set()
        canonical: list[str] = []
        for td_raw in inputs:
            norm = normalize_district_key(td_raw)
            if norm and norm not in seen:
                seen.add(norm)
                canonical.append(norm)
        assert len(canonical) == 1
        assert canonical[0] == normalize_district_key("الملقا")

    def test_empty_input_skipped(self):
        norm = normalize_district_key("")
        assert norm == ""
