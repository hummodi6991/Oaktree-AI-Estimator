#!/usr/bin/env python3
"""Generate brand-alias candidate CSV from restaurant_poi for human review.

Read-only diagnostic. Scans ``restaurant_poi.name``, applies the production
chain-key normalizer (imported from
``app.ingest.expansion_advisor_competitors`` so the two stay in lockstep),
removes keys already present in ``brand_alias``, and emits a dated CSV of
candidate rows for the reviewer (Ahmed).

The script proposes a ``canonical_brand_id`` + bilingual display names for
chains it recognizes via an internal ``KNOWN_CHAINS`` lookup, flags likely
non-chains via ``NOT_A_CHAIN_PATTERNS``, and assigns a confidence tier
(``high`` / ``medium`` / ``low`` / ``unknown``). The output is NEVER merged
into ``data/brand_aliases.csv`` by this script — Ahmed reviews the CSV in
a spreadsheet and the cleaned rows land in a follow-up PR.

Usage (Codespace against production):

    PGHOST=... PGPORT=... PGUSER=... PGPASSWORD=... PGDATABASE=... \\
      python scripts/diagnostics/generate_brand_alias_candidates.py

Output file: ``/tmp/brand_alias_candidates_YYYYMMDD.csv``.

Safe to re-run: the output filename is date-stamped and the script issues
no DB writes.
"""

from __future__ import annotations

import csv
import datetime as _dt
import os
import sys
from collections import Counter

# Import a DB driver lazily so test runners that don't have psycopg
# installed can still import this module to exercise the classifier and
# CSV writer.
_psycopg = None
_DRIVER = None
try:
    import psycopg as _psycopg  # psycopg3

    _DRIVER = "psycopg"
except ImportError:  # pragma: no cover — older envs
    try:
        import psycopg2 as _psycopg  # type: ignore[no-redef]

        _DRIVER = "psycopg2"
    except ImportError:
        _psycopg = None
        _DRIVER = None

# Import directly from the production module so any future change to the
# normalizer or denylist flows through here automatically.
from app.ingest.expansion_advisor_competitors import (  # noqa: E402
    _CHAIN_KEY_DENYLIST,
    _CHAIN_NAME_NORM_SQL,
)

# ---------------------------------------------------------------------------
# Internal classification tables
# ---------------------------------------------------------------------------
# KNOWN_CHAINS: ordered list of (substring_pattern, (id, en, ar)) tuples.
# Substring match against the NORMALIZED chain_key. Longest patterns first
# so e.g. "costa coffee" beats "costa", "pizza hut" beats "pizza".
#
# Bias check: every entry below is either a global brand with confirmed
# Saudi presence, or a well-known KSA / Gulf-region chain. Brands whose
# Saudi operations the author is not 100% sure of are intentionally left
# OUT of KNOWN_CHAINS so they fall through to the medium/unknown bucket.

KNOWN_CHAINS: list[tuple[str, tuple[str, str, str]]] = [
    # --- Coffee chains (global) ---
    ("costa coffee", ("costa_coffee", "Costa Coffee", "كوستا كوفي")),
    ("krispy kreme", ("krispy_kreme", "Krispy Kreme", "كرسبي كريم")),
    ("tim hortons", ("tim_hortons", "Tim Hortons", "تيم هورتنز")),
    ("dunkin", ("dunkin", "Dunkin'", "دانكن")),
    ("starbucks", ("starbucks", "Starbucks", "ستاربكس")),
    ("costa", ("costa_coffee", "Costa Coffee", "كوستا كوفي")),
    # --- Coffee chains (Saudi / Gulf) ---
    ("dr cafe", ("dr_cafe", "Dr. CAFE", "دكتور كيف")),
    ("half million", ("half_million", "Half Million", "هاف ميلين")),
    ("coffee address", ("coffee_address", "Coffee Address", "عنوان القهوة")),
    ("barn s", ("barns", "Barn's", "بارنز")),
    ("barns", ("barns", "Barn's", "بارنز")),
    # --- QSR burgers ---
    ("burger king", ("burger_king", "Burger King", "برجر كنج")),
    ("mcdonald s", ("mcdonalds", "McDonald's", "ماكدونالدز")),
    ("mcdonald", ("mcdonalds", "McDonald's", "ماكدونالدز")),
    ("hardee s", ("hardees", "Hardee's", "هارديز")),
    ("hardees", ("hardees", "Hardee's", "هارديز")),
    ("hardee", ("hardees", "Hardee's", "هارديز")),
    ("five guys", ("five_guys", "Five Guys", "فايف غايز")),
    ("shake shack", ("shake_shack", "Shake Shack", "شيك شاك")),
    # --- QSR chicken ---
    ("texas chicken", ("texas_chicken", "Texas Chicken", "تكساس تشكن")),
    ("popeyes", ("popeyes", "Popeyes", "بوبايز")),
    ("al baik", ("albaik", "Albaik", "البيك")),
    ("albaik", ("albaik", "Albaik", "البيك")),
    ("herfy", ("herfy", "Herfy", "هرفي")),
    ("kudu", ("kudu", "Kudu", "كودو")),
    ("kfc", ("kfc", "KFC", "كنتاكي")),
    # --- Pizza ---
    ("pizza hut", ("pizza_hut", "Pizza Hut", "بيتزا هت")),
    ("papa john", ("papa_johns", "Papa John's", "بابا جونز")),
    ("dominos", ("dominos", "Domino's", "دومينوز")),
    ("domino s", ("dominos", "Domino's", "دومينوز")),
    ("maestro pizza", ("maestro_pizza", "Maestro Pizza", "مايسترو بيتزا")),
    ("little caesars", ("little_caesars", "Little Caesars", "ليتل سيزرز")),
    # --- Subs / sandwiches ---
    ("subway", ("subway", "Subway", "صب واي")),
    ("shawarmer", ("shawarmer", "Shawarmer", "شاورمر")),
    ("kababji", ("kababji", "Kababji", "كبابجي")),
    # --- Desserts / ice cream ---
    ("baskin", ("baskin_robbins", "Baskin-Robbins", "باسكن روبنز")),
    ("dairy queen", ("dairy_queen", "Dairy Queen", "ديري كوين")),
    ("marble slab", ("marble_slab", "Marble Slab", "ماربل سلاب")),
    ("cold stone", ("cold_stone", "Cold Stone Creamery", "كولد ستون")),
    ("cinnabon", ("cinnabon", "Cinnabon", "سينابون")),
    # --- Casual dining ---
    ("texas roadhouse", ("texas_roadhouse", "Texas Roadhouse", "تكساس رود هاوس")),
    (
        "the cheesecake factory",
        ("cheesecake_factory", "The Cheesecake Factory", "ذا تشيز كيك فاكتوري"),
    ),
    (
        "cheesecake factory",
        ("cheesecake_factory", "The Cheesecake Factory", "ذا تشيز كيك فاكتوري"),
    ),
    ("applebee", ("applebees", "Applebee's", "ابلبيز")),
    ("chili s", ("chilis", "Chili's", "تشيليز")),
    ("chilis", ("chilis", "Chili's", "تشيليز")),
    ("tgi friday", ("tgi_fridays", "TGI Fridays", "تي جي اي فرايديز")),
    ("ihop", ("ihop", "IHOP", "اي هوب")),
    ("p f chang", ("pf_changs", "P.F. Chang's", "بي اف تشانغز")),
    # --- Saudi / regional ---
    ("bateel", ("bateel", "Bateel", "بتيل")),
    ("operation falafel", ("operation_falafel", "Operation Falafel", "عملية فلافل")),
    ("paul", ("paul", "Paul", "بول")),  # bakery chain
    ("magnolia", ("magnolia_bakery", "Magnolia Bakery", "ماغنوليا")),
    (
        "le pain quotidien",
        ("le_pain_quotidien", "Le Pain Quotidien", "لو بان كوتيديان"),
    ),
    ("nando", ("nandos", "Nando's", "ناندوز")),
    ("the meat co", ("the_meat_co", "The Meat Co", "ذا ميت كو")),
    ("shrimp anatomy", ("shrimp_anatomy", "Shrimp Anatomy", "شريمب أناتومي")),
    # --- Saudi regional chains (extended) ---
    ("al romansiah", ("al_romansiah", "Al Romansiah", "الرومانسية")),
    ("romansiah", ("al_romansiah", "Al Romansiah", "الرومانسية")),
    ("najd village", ("najd_village", "Najd Village", "قرية نجد")),
    ("naqd village", ("najd_village", "Najd Village", "قرية نجد")),
    ("al tazaj", ("al_tazaj", "Al Tazaj", "الطازج")),
    ("altazaj", ("al_tazaj", "Al Tazaj", "الطازج")),
    ("al tannour", ("al_tannour", "Al Tannour", "التنور")),
    ("section b", ("section_b", "Section B", "سيكشن بي")),
    ("local food", ("local_food", "Local Food", "لوكال فود")),
    ("the butcher", ("the_butcher_shop", "The Butcher Shop", "ذا بوتشر شوب")),
    ("johnny rockets", ("johnny_rockets", "Johnny Rockets", "جوني روكتس")),
    ("steak house", ("the_steak_house", "The Steak House", "ذا ستيك هاوس")),
    ("carluccio", ("carluccios", "Carluccio's", "كارلوتشيوز")),
    ("zaatar w zeit", ("zaatar_w_zeit", "Zaatar W Zeit", "زعتر وزيت")),
    ("caribou", ("caribou_coffee", "Caribou Coffee", "كاريبو كوفي")),
    ("second cup", ("second_cup", "Second Cup", "سيكوند كاب")),
    ("waffle house", ("waffle_house", "Waffle House", "وافل هاوس")),
    ("the burger joint", ("the_burger_joint", "The Burger Joint", "ذا برجر جوينت")),
    ("steak escape", ("steak_escape", "Steak Escape", "ستيك اسكيب")),
    ("kraving", ("kraving", "Kraving", "كرافنغ")),
    ("the breakfast club", ("breakfast_club", "The Breakfast Club", "ذا بريكفاست كلب")),
    ("urth caffe", ("urth_caffe", "Urth Caffé", "ايرث كافيه")),
    ("nozomi", ("nozomi", "Nozomi", "نوزومي")),
    ("buffalo wild wings", ("buffalo_wild_wings", "Buffalo Wild Wings", "بفلو وايلد وينغز")),
    ("buffalo wings", ("buffalo_wild_wings", "Buffalo Wild Wings", "بفلو وايلد وينغز")),
    ("fudruckers", ("fuddruckers", "Fuddruckers", "فدركرز")),
    ("fuddruckers", ("fuddruckers", "Fuddruckers", "فدركرز")),
    ("yum yum", ("yum_yum", "Yum Yum", "يم يم")),
    ("madi", ("madi", "Madi", "مادي")),
    ("al saj", ("al_saj", "Al Saj", "الصاج")),
    ("white robata", ("white_robata", "White Robata", "وايت روباتا")),
    ("burgerizzr", ("burgerizzr", "Burgerizzr", "برجرايزر")),
    ("burgerizer", ("burgerizzr", "Burgerizzr", "برجرايزر")),
    ("lusin", ("lusin", "Lusin", "لوسين")),
    ("the cheese gar", ("cheese_gar", "The Cheese Gar", "ذا تشيز غار")),
    ("mama noura", ("mama_noura", "Mama Noura", "ماما نورة")),
    ("mamanoura", ("mama_noura", "Mama Noura", "ماما نورة")),
    ("camile", ("camile", "Camile", "كاميل")),
    ("nineteen", ("nineteen", "Nineteen", "ناينتين")),
    ("ghaida", ("ghaida", "Ghaida", "غيداء")),
    ("al baba", ("al_baba", "Al Baba", "البابا")),
    ("al wadi", ("al_wadi", "Al Wadi", "الوادي")),
    ("al sham", ("al_sham", "Al Sham", "الشام")),
    ("kabsa", ("kabsa_house", "Kabsa House", "بيت الكبسة")),
    ("makarna", ("makarna", "Makarna", "مكرونة")),
    # --- Arabic-keyed entries (Arabic-only POI names land here) ---
    ("ستاربكس", ("starbucks", "Starbucks", "ستاربكس")),
    ("ستاربوكس", ("starbucks", "Starbucks", "ستاربكس")),
    ("ماكدونالدز", ("mcdonalds", "McDonald's", "ماكدونالدز")),
    ("ماكدونالد", ("mcdonalds", "McDonald's", "ماكدونالدز")),
    ("برجر كنج", ("burger_king", "Burger King", "برجر كنج")),
    ("بيرجر كنج", ("burger_king", "Burger King", "برجر كنج")),
    ("كنتاكي", ("kfc", "KFC", "كنتاكي")),
    ("بيتزا هت", ("pizza_hut", "Pizza Hut", "بيتزا هت")),
    ("دانكن", ("dunkin", "Dunkin'", "دانكن")),
    ("كوستا", ("costa_coffee", "Costa Coffee", "كوستا كوفي")),
    ("البيك", ("albaik", "Albaik", "البيك")),
    ("هرفي", ("herfy", "Herfy", "هرفي")),
    ("كودو", ("kudu", "Kudu", "كودو")),
    ("هارديز", ("hardees", "Hardee's", "هارديز")),
    ("شاورمر", ("shawarmer", "Shawarmer", "شاورمر")),
    ("سب واي", ("subway", "Subway", "صب واي")),
    ("صب واي", ("subway", "Subway", "صب واي")),
    ("بوبايز", ("popeyes", "Popeyes", "بوبايز")),
    ("دومينوز", ("dominos", "Domino's", "دومينوز")),
    ("بابا جونز", ("papa_johns", "Papa John's", "بابا جونز")),
    ("باسكن", ("baskin_robbins", "Baskin-Robbins", "باسكن روبنز")),
    ("سينابون", ("cinnabon", "Cinnabon", "سينابون")),
    ("كرسبي كريم", ("krispy_kreme", "Krispy Kreme", "كرسبي كريم")),
    ("تيم هورتنز", ("tim_hortons", "Tim Hortons", "تيم هورتنز")),
    ("تكساس تشكن", ("texas_chicken", "Texas Chicken", "تكساس تشكن")),
    ("مايسترو بيتزا", ("maestro_pizza", "Maestro Pizza", "مايسترو بيتزا")),
    ("بارنز", ("barns", "Barn's", "بارنز")),
    ("هاف ميلين", ("half_million", "Half Million", "هاف ميلين")),
    ("نصف مليون", ("half_million", "Half Million", "هاف ميلين")),
    ("بتيل", ("bateel", "Bateel", "بتيل")),
    ("ناندوز", ("nandos", "Nando's", "ناندوز")),
    ("ديري كوين", ("dairy_queen", "Dairy Queen", "ديري كوين")),
    ("كبابجي", ("kababji", "Kababji", "كبابجي")),
    ("عنوان القهوة", ("coffee_address", "Coffee Address", "عنوان القهوة")),
    ("دكتور كيف", ("dr_cafe", "Dr. CAFE", "دكتور كيف")),
    ("الرومانسية", ("al_romansiah", "Al Romansiah", "الرومانسية")),
    ("الطازج", ("al_tazaj", "Al Tazaj", "الطازج")),
    ("التنور", ("al_tannour", "Al Tannour", "التنور")),
    ("الصاج", ("al_saj", "Al Saj", "الصاج")),
    ("قرية نجد", ("najd_village", "Najd Village", "قرية نجد")),
    ("ماما نورة", ("mama_noura", "Mama Noura", "ماما نورة")),
    ("ذا بوتشر", ("the_butcher_shop", "The Butcher Shop", "ذا بوتشر شوب")),
    ("كاريبو", ("caribou_coffee", "Caribou Coffee", "كاريبو كوفي")),
    ("زعتر وزيت", ("zaatar_w_zeit", "Zaatar W Zeit", "زعتر وزيت")),
    ("بيت الكبسة", ("kabsa_house", "Kabsa House", "بيت الكبسة")),
    ("الشام", ("al_sham", "Al Sham", "الشام")),
    ("الوادي", ("al_wadi", "Al Wadi", "الوادي")),
]


# Substrings of the normalized chain_key that strongly suggest a non-chain
# (generic descriptor, cuisine type, city/district name, generic noun).
NOT_A_CHAIN_PATTERNS: tuple[str, ...] = (
    # Generic English descriptors
    "kitchen",
    "bakery",
    "restaurants",
    "restaurant",
    "buffet",
    "diner",
    "lounge",
    "snack",
    "catering",
    # Generic Arabic descriptors
    "مطعم",  # restaurant
    "مطبخ",  # kitchen
    "مخبز",  # bakery
    "بقالة",  # grocery
    "بقاله",  # grocery (alt spelling)
    "كافيه",  # cafe
    "كوفي شوب",  # coffee shop
    "وجبات",  # meals
    "حلويات",  # sweets
    "مأكولات",  # foods
    "ماكولات",  # foods (variant)
    "اكلات",  # foods
    # Cuisine descriptors that frequently appear as standalone POI names
    "lebanese",
    "indian",
    "chinese",
    "italian",
    "turkish",
    "yemeni",
    "lubnani",
)


def classify(
    chain_key: str,
    sample_names: list[str],
    poi_count: int,
) -> tuple[str, str, str, str, str]:
    """Return (canonical_brand_id, en, ar, confidence, notes).

    The first three may be empty when the classifier can't propose them.
    """
    # 1. Try the explicit KNOWN_CHAINS lookup (longest substrings first).
    for pattern, (cid, en, ar) in KNOWN_CHAINS:
        if pattern in chain_key:
            return cid, en, ar, "high", ""

    # 2. Non-chain patterns.
    for pattern in NOT_A_CHAIN_PATTERNS:
        if pattern in chain_key:
            return "", "", "", "low", f"matches non-chain pattern '{pattern}'"

    # 3. High count → likely a chain, but unknown — flag medium for naming.
    if poi_count >= 5:
        return "", "", "", "medium", "high count, manual classification needed"

    # 4. Otherwise unknown — Ahmed decides.
    return "", "", "", "unknown", ""


# ---------------------------------------------------------------------------
# Database query
# ---------------------------------------------------------------------------

# Produce the SQL fragment that normalizes restaurant_poi.name. This is the
# same fragment the production matcher uses, so the alias_keys we generate
# match what runtime will look up.
_NORM_EXPR = _CHAIN_NAME_NORM_SQL.format(col="name")
_DENYLIST_SQL = ", ".join(f"'{w}'" for w in _CHAIN_KEY_DENYLIST)

CANDIDATE_QUERY = f"""
WITH norm AS (
    SELECT
        {_NORM_EXPR} AS chain_key,
        name AS sample_name
    FROM restaurant_poi
    WHERE name IS NOT NULL AND name != ''
),
agg AS (
    SELECT
        chain_key,
        COUNT(*) AS poi_count,
        (array_agg(DISTINCT sample_name))[1:5] AS samples
    FROM norm
    WHERE chain_key != ''
      AND chain_key NOT IN ({_DENYLIST_SQL})
      AND chain_key !~ '^[0-9]+$'
    GROUP BY chain_key
)
SELECT a.chain_key, a.poi_count, a.samples
FROM agg a
LEFT JOIN brand_alias ba ON ba.alias_key = a.chain_key
WHERE ba.alias_key IS NULL
  AND a.poi_count >= 2
ORDER BY a.poi_count DESC
LIMIT 600;
"""


# Current canonicalization rate, used only to print the projection table.
# Sourced from production diagnostics (Apr/May 2026).
CURRENT_CANONICALIZATION_RATE = 0.163
CURRENT_CANONICALIZED_POIS = 7476
CURRENT_TOTAL_POIS = 45889


def _connect():
    if _psycopg is None:
        raise RuntimeError(
            "neither psycopg nor psycopg2 is installed in this env. "
            "Install one (or run from a venv that has them)."
        )
    kwargs = {
        "host": os.environ.get("PGHOST"),
        "port": os.environ.get("PGPORT"),
        "user": os.environ.get("PGUSER"),
        "password": os.environ.get("PGPASSWORD"),
        "dbname": os.environ.get("PGDATABASE"),
        "sslmode": os.environ.get("PGSSLMODE"),
    }
    kwargs = {k: v for k, v in kwargs.items() if v}
    return _psycopg.connect(**kwargs)


CSV_HEADER = [
    "chain_key",
    "total_pois",
    "sample_raw_names",
    "proposed_canonical_brand_id",
    "proposed_display_name_en",
    "proposed_display_name_ar",
    "confidence",
    "notes",
]


def _format_samples(samples) -> str:
    if not samples:
        return ""
    # samples comes back as a Python list from both psycopg and psycopg2.
    return " || ".join(str(s) for s in samples if s)


def run(out_path: str) -> dict:
    """Connect, query, classify, write CSV. Returns summary dict."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(CANDIDATE_QUERY)
        rows = cur.fetchall()
    finally:
        conn.close()

    return write_candidates(rows, out_path)


def write_candidates(rows: list, out_path: str) -> dict:
    """Classify each row and write to CSV. Returns summary dict.

    Split out so tests can exercise this without a live DB.
    """
    tier_counts: Counter[str] = Counter()
    tier_pois: Counter[str] = Counter()

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for row in rows:
            chain_key, poi_count, samples = row[0], row[1], row[2]
            cid, en, ar, conf, notes = classify(
                chain_key,
                samples or [],
                poi_count,
            )
            writer.writerow(
                [
                    chain_key,
                    poi_count,
                    _format_samples(samples),
                    cid,
                    en,
                    ar,
                    conf,
                    notes,
                ]
            )
            tier_counts[conf] += 1
            tier_pois[conf] += poi_count

    return {
        "rows": len(rows),
        "tier_counts": dict(tier_counts),
        "tier_pois": dict(tier_pois),
        "out_path": out_path,
    }


def _print_summary(summary: dict) -> None:
    rows = summary["rows"]
    counts = summary["tier_counts"]
    pois = summary["tier_pois"]
    total_potential = sum(pois.values())

    print(f"Generated {rows} candidate aliases:")
    for tier in ("high", "medium", "low", "unknown"):
        c = counts.get(tier, 0)
        p = pois.get(tier, 0)
        print(f"  {tier + ' confidence:':<18} {c:>4}  →  covers {p:>7,} POIs")

    print(
        f"Total potential POI coverage gain: {total_potential:,} POIs "
        f"({(total_potential / CURRENT_TOTAL_POIS) * 100:+.1f}%)"
    )
    print(
        f"Current canonicalization rate:    "
        f"{CURRENT_CANONICALIZATION_RATE * 100:.1f}%"
    )
    high_gain = pois.get("high", 0)
    med_gain = pois.get("medium", 0)
    proj_high = (CURRENT_CANONICALIZED_POIS + high_gain) / CURRENT_TOTAL_POIS
    proj_high_med = (
        CURRENT_CANONICALIZED_POIS + high_gain + med_gain
    ) / CURRENT_TOTAL_POIS
    print(f"Projected if all `high` adopted:  {proj_high * 100:.1f}%")
    print(f"Projected if `high`+`medium`:     {proj_high_med * 100:.1f}%")
    print(f"Output written to {summary['out_path']}")


def main() -> int:
    out_path = (
        f"/tmp/brand_alias_candidates_" f"{_dt.date.today().strftime('%Y%m%d')}.csv"
    )
    try:
        summary = run(out_path)
    except Exception as exc:
        print(f"ERROR: candidate generation failed: {exc}", file=sys.stderr)
        return 2
    _print_summary(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
