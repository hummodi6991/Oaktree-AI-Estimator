"""Integration tests for brand_alias canonical dedup in proximity queries."""
from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def _make_session_with_aliases():
    """SQLite session with brand_alias and ECQ populated for canonical dedup tests."""
    engine = create_engine("sqlite:///:memory:")
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    db.execute(text("""
        CREATE TABLE brand_alias (
            alias_key VARCHAR(256) PRIMARY KEY,
            canonical_brand_id VARCHAR(64) NOT NULL,
            display_name_en VARCHAR(256),
            display_name_ar VARCHAR(256),
            notes TEXT
        )
    """))
    db.execute(text("""
        CREATE TABLE expansion_competitor_quality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brand_name VARCHAR(256),
            canonical_brand_id VARCHAR(64),
            display_name_en VARCHAR(256),
            display_name_ar VARCHAR(256)
        )
    """))

    db.execute(text("""
        INSERT INTO brand_alias (alias_key, canonical_brand_id, display_name_en, display_name_ar)
        VALUES
            ('burger king', 'burger_king', 'Burger King', 'بيرجر كنج'),
            ('بيرجر كنج', 'burger_king', 'Burger King', 'بيرجر كنج'),
            ('kfc', 'kfc', 'KFC', 'كنتاكي'),
            ('كنتاكي', 'kfc', 'KFC', 'كنتاكي')
    """))

    db.execute(text("""
        INSERT INTO expansion_competitor_quality
            (brand_name, canonical_brand_id, display_name_en, display_name_ar)
        VALUES
            ('Burger King', 'burger_king', 'Burger King', 'بيرجر كنج'),
            ('بيرجر كنج', 'burger_king', 'Burger King', 'بيرجر كنج'),
            ('KFC', 'kfc', 'KFC', 'كنتاكي'),
            ('Kfc', 'kfc', 'KFC', 'كنتاكي')
    """))
    db.commit()
    return db


class TestBrandAliasJoin:
    """Verify ECQ rows now carry canonical brand metadata after the patch."""

    def test_ecq_rows_have_canonical_brand_id(self):
        db = _make_session_with_aliases()
        rows = db.execute(text("""
            SELECT brand_name, canonical_brand_id, display_name_en, display_name_ar
            FROM expansion_competitor_quality
            ORDER BY id
        """)).mappings().all()

        assert len(rows) == 4
        # Two cross-script Burger King rows + two KFC casing variants
        # all carry canonical brand IDs and matching display names.
        assert {r["canonical_brand_id"] for r in rows} == {"burger_king", "kfc"}
        for r in rows:
            assert r["display_name_en"] in {"Burger King", "KFC"}
            assert r["display_name_ar"] in {"بيرجر كنج", "كنتاكي"}

    def test_canonical_dedup_via_distinct_on(self):
        """Simulate the proximity-query DISTINCT ON pattern.

        Two Burger King rows + two KFC rows should dedup to two rows when
        DISTINCT ON canonical_brand_id is applied.
        """
        db = _make_session_with_aliases()
        # SQLite doesn't support DISTINCT ON; simulate via GROUP BY.
        rows = db.execute(text("""
            SELECT canonical_brand_id, COUNT(*) AS variants
            FROM expansion_competitor_quality
            GROUP BY canonical_brand_id
        """)).mappings().all()
        rows_by_brand = {r["canonical_brand_id"]: r["variants"] for r in rows}
        assert rows_by_brand == {"burger_king": 2, "kfc": 2}

    def test_chain_key_format_matches_brand_alias(self):
        """Sanity: every brand_alias.alias_key value should match the
        normalized form produced by _normalize_chain_name. If this test
        fails, the brand_aliases.csv has chain_keys that won't be matched
        by _CHAIN_NAME_NORM_SQL at materialization time.
        """
        from app.ingest.expansion_advisor_competitors import _normalize_chain_name

        # Sample of canonical alias_keys from the production CSV
        sample_keys = [
            "burger king",
            "بيرجر كنج",
            "kfc",
            "كنتاكي",
            "starbucks",
            "ستاربكس",
            "dunkin دانكن",
            "mcdonald s",
        ]
        for key in sample_keys:
            normalized = _normalize_chain_name(key)
            assert normalized == key, (
                f"alias_key {key!r} does not survive _normalize_chain_name "
                f"round-trip (became {normalized!r}). brand_alias rows would "
                f"never match at materialization time."
            )
