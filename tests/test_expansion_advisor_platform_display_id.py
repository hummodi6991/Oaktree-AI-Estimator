"""Unit tests for the PR6 ``platform`` / ``display_id`` candidate-dict fields.

These exercise pure Python (no live PostgreSQL):

  - ``_strip_platform_prefix`` is imported and tested directly.
  - The ``platform`` / ``display_id`` derivation is replicated from the
    inline candidate-dict construction in
    ``app/services/expansion_advisor.py`` (the same dict literal that
    overrides ``source_type`` to ``commercial_unit``). The candidate dict is
    assembled deep inside the DB-bound search function, so the derivation
    rule is mirrored here rather than invoking that function. Keep this
    mirror in sync if the derivation changes.
"""

import pytest

from app.services.expansion_advisor import _strip_platform_prefix


def _derive_candidate_fields(row: dict) -> dict:
    """Mirror of the ``platform`` / ``display_id`` keys built in the
    ``app.services.expansion_advisor`` candidate-dict construction."""
    return {
        "platform": (
            row.get("source_type")
            if row.get("commercial_unit_id")
            and row.get("source_type") in ("aqar", "bayut")
            else None
        ),
        "display_id": _strip_platform_prefix(
            row.get("source_id") or row.get("commercial_unit_id")
        ),
    }


def test_candidate_dict_emits_platform_for_aqar_row():
    row = {"source_type": "aqar", "commercial_unit_id": "12345", "source_id": "12345"}
    out = _derive_candidate_fields(row)
    assert out["platform"] == "aqar"
    assert out["display_id"] == "12345"


def test_candidate_dict_emits_platform_for_bayut_row():
    row = {
        "source_type": "bayut",
        "commercial_unit_id": "bayut:87825483",
        "source_id": "bayut:87825483",
    }
    out = _derive_candidate_fields(row)
    assert out["platform"] == "bayut"
    assert out["display_id"] == "87825483"


def test_candidate_dict_platform_null_for_non_listing_tier():
    row = {"source_type": "arcgis_parcel", "commercial_unit_id": None, "source_tier": 3}
    out = _derive_candidate_fields(row)
    assert out["platform"] is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("bayut:42", "42"),
        ("aqar-123", "aqar-123"),
        ("foo:bar:baz", "bar:baz"),
        (None, None),
        ("", ""),
    ],
)
def test_strip_platform_prefix_helper(value, expected):
    assert _strip_platform_prefix(value) == expected
