"""Tests for scripts/diagnostics/generate_brand_alias_candidates.py.

Coverage:

1. Classifier regression — representative chain_keys map to the expected
   (canonical_brand_id, en, ar, confidence) tuples.
2. Confidence-tier coverage — synthetic rows land in the expected tier.
3. Normalizer agreement — the Python `_normalize_chain_name` agrees with
   the SQL fragment `_CHAIN_NAME_NORM_SQL` used by the candidate
   generator, so the alias_keys emitted match what the production matcher
   computes at runtime.

These tests do NOT touch the production DB. The few cases that need to
exercise the write path mock the cursor with hand-rolled tuples.
"""

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path

import pytest

from app.ingest.expansion_advisor_competitors import (
    _CHAIN_NAME_NORM_SQL,
    _normalize_chain_name,
)

SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "diagnostics"
    / "generate_brand_alias_candidates.py"
)


def _load_module():
    """Import the script as a module so we can call its helpers."""
    spec = importlib.util.spec_from_file_location(
        "generate_brand_alias_candidates",
        SCRIPT_PATH,
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["generate_brand_alias_candidates"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def gen():
    return _load_module()


# ---------------------------------------------------------------------------
# 1. Classifier regression
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "chain_key,expected_id,expected_en,expected_ar,expected_conf",
    [
        ("starbucks", "starbucks", "Starbucks", "ستاربكس", "high"),
        ("mcdonald s", "mcdonalds", "McDonald's", "ماكدونالدز", "high"),
        ("kfc", "kfc", "KFC", "كنتاكي", "high"),
        ("albaik", "albaik", "Albaik", "البيك", "high"),
        ("al baik", "albaik", "Albaik", "البيك", "high"),
        ("herfy", "herfy", "Herfy", "هرفي", "high"),
        ("costa coffee", "costa_coffee", "Costa Coffee", "كوستا كوفي", "high"),
        ("ستاربكس", "starbucks", "Starbucks", "ستاربكس", "high"),
        ("البيك", "albaik", "Albaik", "البيك", "high"),
        ("dr cafe", "dr_cafe", "Dr. CAFE", "دكتور كيف", "high"),
    ],
)
def test_classify_known_chains(
    gen,
    chain_key,
    expected_id,
    expected_en,
    expected_ar,
    expected_conf,
):
    cid, en, ar, conf, _notes = gen.classify(chain_key, [], poi_count=10)
    assert cid == expected_id
    assert en == expected_en
    assert ar == expected_ar
    assert conf == expected_conf


@pytest.mark.parametrize(
    "chain_key,expected_id",
    [
        ("al romansiah", "al_romansiah"),
        ("najd village", "najd_village"),
        ("caribou coffee shop", "caribou_coffee"),  # substring match
        ("mama noura riyadh", "mama_noura"),
        ("الرومانسية", "al_romansiah"),  # Arabic-only match
        ("الطازج", "al_tazaj"),
    ],
)
def test_classify_extended_chains(gen, chain_key, expected_id):
    cid, _, _, conf, _ = gen.classify(chain_key, [], poi_count=10)
    assert cid == expected_id
    assert conf == "high"


def test_known_chains_no_persian_codepoints(gen):
    """Every Arabic string in KNOWN_CHAINS must be in the Arabic block."""
    for pattern, (cid, en, ar) in gen.KNOWN_CHAINS:
        for ch in pattern:
            if ord(ch) >= 0x0600:  # Arabic-ish ranges
                assert 0x0600 <= ord(ch) <= 0x06FF, (
                    f"Pattern '{pattern}' contains non-Arabic-block "
                    f"codepoint U+{ord(ch):04X}"
                )
        for ch in ar:
            if ord(ch) >= 0x0600:
                assert 0x0600 <= ord(ch) <= 0x06FF, (
                    f"Arabic display name '{ar}' contains non-Arabic-block "
                    f"codepoint U+{ord(ch):04X}"
                )


def test_classify_non_chain_pattern(gen):
    cid, en, ar, conf, notes = gen.classify(
        "matjar al baqala بقالة المتجر",
        [],
        poi_count=10,
    )
    assert cid == "" and en == "" and ar == ""
    assert conf == "low"
    assert "non-chain pattern" in notes


def test_classify_unknown_low_count(gen):
    # No KNOWN_CHAINS match, no NOT_A_CHAIN match, count < 5 → unknown.
    cid, en, ar, conf, notes = gen.classify(
        "some random brand",
        [],
        poi_count=2,
    )
    assert (cid, en, ar) == ("", "", "")
    assert conf == "unknown"
    assert notes == ""


def test_classify_unknown_high_count_is_medium(gen):
    # No KNOWN_CHAINS, no NOT_A_CHAIN, but count >= 5 → medium.
    cid, en, ar, conf, notes = gen.classify(
        "some random brand",
        [],
        poi_count=12,
    )
    assert (cid, en, ar) == ("", "", "")
    assert conf == "medium"
    assert "manual" in notes


# ---------------------------------------------------------------------------
# 2. Confidence-tier coverage on synthetic rows
# ---------------------------------------------------------------------------


def test_write_candidates_tier_coverage(gen, tmp_path):
    rows = [
        # high (KNOWN_CHAINS match)
        ("starbucks olaya", 30, ["Starbucks Olaya"]),
        ("ستاربكس النخيل", 25, ["ستاربكس النخيل"]),
        ("kfc", 20, ["KFC"]),
        # low (non-chain pattern)
        ("مطعم الشرق", 8, ["مطعم الشرق"]),
        ("the corner kitchen", 6, ["The Corner Kitchen"]),
        # medium (no match, count >= 5)
        ("foo bar grill house", 7, ["Foo Bar Grill House"]),
        ("بيت الخير", 9, ["بيت الخير"]),
        # unknown (no match, count < 5)
        ("xyz random", 2, ["xyz random"]),
        ("abc cafeteria nameless", 3, ["abc cafeteria nameless"]),
        ("zzzz one off", 2, ["zzzz one off"]),
    ]
    out = tmp_path / "candidates.csv"
    summary = gen.write_candidates(rows, str(out))

    assert summary["rows"] == 10
    counts = summary["tier_counts"]
    assert counts.get("high", 0) == 3
    assert counts.get("low", 0) == 2
    assert counts.get("medium", 0) == 2
    assert counts.get("unknown", 0) == 3

    # CSV is well-formed and has the expected header.
    with out.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        body = list(reader)
    assert header == gen.CSV_HEADER
    assert len(body) == 10
    # First row (highest count) should be classified high.
    high_rows = [r for r in body if r[6] == "high"]
    assert len(high_rows) == 3


# ---------------------------------------------------------------------------
# 3. Normalizer agreement — Python mirror matches SQL fragment
# ---------------------------------------------------------------------------

# Pure-Python re-implementation of the SQL fragment, evaluated symbolically.
# We can't actually execute Postgres regex from Python, but we can verify
# that the Python normalizer's outputs match a "trusted" rebuild of the
# same logic from the same character maps. If a future contributor edits
# `_CHAIN_NAME_NORM_SQL` without touching `_normalize_chain_name` (or vice
# versa), the SQL fragment's character maps will drift from the Python
# function and this test fails.


def _expected_python_norm(name: str) -> str:
    """Independent re-derivation of `_normalize_chain_name` semantics."""
    import re as _re

    if not name:
        return ""
    s = name.lower()
    s = s.translate(
        {
            ord("أ"): "ا",
            ord("إ"): "ا",
            ord("آ"): "ا",
            ord("ى"): "ي",
            ord("ـ"): None,
        }
    )
    s = _re.sub(r"[^a-z0-9\s؀-ۿ]", " ", s)
    s = _re.sub(r"\s+", " ", s).strip()
    return s


@pytest.mark.parametrize(
    "raw",
    [
        "Starbucks - Olaya",
        "ستاربكس النخيل",
        "Burger King - برجر كنج",
        "Al-Baik",
        "McDonald's",
        "Dr. CAFE",
        "أبو يوسف",  # alef-hamza-above → alef
        "إفطار",  # alef-hamza-below → alef
        "آيس كريم",  # alef-madda → alef
        "ليلى",  # alef-maksura → ya
        "كافيـه",  # tatweel embedded
    ],
)
def test_normalizer_python_matches_expected(raw):
    assert _normalize_chain_name(raw) == _expected_python_norm(raw)


def test_sql_fragment_uses_same_codepoints_as_python():
    """The SQL fragment must reference the same Arabic codepoints the
    Python normalizer translates. This is the structural lockstep check
    described in the module docstring.
    """
    # SQL uses unicode escapes; verify the four Alef-variants and tatweel
    # are present in the fragment text.
    for code in ("\\u0623", "\\u0625", "\\u0622", "\\u0649", "\\u0640"):
        assert code in _CHAIN_NAME_NORM_SQL, (
            f"SQL normalizer missing codepoint {code} — Python and SQL "
            f"normalizers have drifted apart."
        )
    # SQL's mapped output codepoints.
    for code in ("\\u0627", "\\u064A"):
        assert code in _CHAIN_NAME_NORM_SQL
    # Arabic block in the strip regex.
    assert "\\u0600-\\u06FF" in _CHAIN_NAME_NORM_SQL


def test_candidate_query_uses_production_normalizer(gen):
    """The query string in the generator script must embed the production
    SQL normalizer expression — otherwise the generated alias_keys would
    not match what the runtime matcher computes."""
    # The normalizer fragment, formatted for column `name`, must appear
    # verbatim in the candidate query.
    expected = _CHAIN_NAME_NORM_SQL.format(col="name")
    assert expected in gen.CANDIDATE_QUERY
