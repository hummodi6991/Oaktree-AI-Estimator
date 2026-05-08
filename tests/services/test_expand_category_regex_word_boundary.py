"""Regression tests for the word-bounded shape of ``_expand_category``'s regex.

The regex is consumed by both the ``restaurant_poi`` (category + name) and
``delivery_source_record`` (category_raw + cuisine_raw) competitor predicates
in ``_bulk_enrich_competitors`` / ``_build_candidate_sql`` /
``_build_candidate_sql_no_district``. Without word boundaries the predicate
matches substrings (e.g. "burger" inside "hamburgerville") and inflates the
competitor count; with PostgreSQL POSIX ``\\m`` (left) and ``\\M`` (right)
word boundaries it matches whole tokens only.

These tests exercise the in-process Python construction — no DB required.
PostgreSQL evaluates the same string at runtime against rp.name etc.
"""

import re

import pytest

from app.services.expansion_advisor import _expand_category


def _to_python_regex(pg_regex: str) -> re.Pattern:
    """Translate the PostgreSQL POSIX regex shape to a Python regex.

    PostgreSQL uses ``\\m`` / ``\\M`` for word-start / word-end boundaries;
    Python uses ``\\b`` for both. The translation is sufficient for the
    boundary semantics we want to assert here.
    """
    py = pg_regex.replace(r"\m", r"\b").replace(r"\M", r"\b")
    return re.compile(py, re.IGNORECASE)


class TestWordBoundaryShape:
    def test_burger_regex_starts_with_left_boundary(self):
        assert _expand_category("burger")["regex"].startswith(r"\m(")

    def test_burger_regex_ends_with_right_boundary(self):
        assert _expand_category("burger")["regex"].endswith(r")\M")

    def test_unknown_category_also_word_bounded(self):
        regex = _expand_category("UNKNOWN_CATEGORY_XYZ")["regex"]
        assert regex.startswith(r"\m(")
        assert regex.endswith(r")\M")

    def test_burger_inner_alternation_preserved(self):
        regex = _expand_category("burger")["regex"]
        # Tokens from the alias map: burger | hamburger | برجر
        for token in ("burger", "hamburger", "برجر"):
            assert token in regex


class TestSemanticsAgainstSamplePOINames:
    """Validate that the regex produced for ``burger`` matches the realistic
    miscategorized-as-international names called out in the diagnosis without
    over-matching unrelated tokens.
    """

    def setup_method(self):
        self.pattern = _to_python_regex(_expand_category("burger")["regex"])

    @pytest.mark.parametrize(
        "name",
        [
            "Burger King",
            "Hardee's Burger",
            "Hamburger House",
            "برجر العم",
        ],
    )
    def test_matches_burger_venue_names(self, name: str):
        assert self.pattern.search(name.lower()) is not None

    @pytest.mark.parametrize(
        "name",
        [
            "Pizza Hut",
            "Starbucks Coffee",
            "Al Romansiah",
            "Shawarma House",
        ],
    )
    def test_does_not_match_other_cuisines(self, name: str):
        assert self.pattern.search(name.lower()) is None


class TestPipelineWiring:
    """The competitor predicate must read ``:category_regex`` (already bound
    in the SQL params) at all three call sites — keep this wiring locked
    so a future refactor does not silently regress to category-only equality.
    """

    def test_bulk_competitor_path_uses_regex_predicate_on_rp(self):
        from app.services import expansion_advisor as ea
        import inspect

        src = inspect.getsource(ea._bulk_enrich_competitors)
        assert "lower(rp.category) ~* :category_regex" in src
        assert "lower(rp.name) ~* :category_regex" in src

    def test_candidate_sql_competitor_lateral_uses_regex_predicate_on_rp(self):
        from app.services import expansion_advisor as ea
        import inspect

        # Both _build_candidate_sql and _build_candidate_sql_no_district
        # are nested helpers inside run_expansion_search.
        src = inspect.getsource(ea.run_expansion_search)
        occurrences_cat = src.count("lower(rp.category) ~* :category_regex")
        occurrences_name = src.count("lower(rp.name) ~* :category_regex")
        assert occurrences_cat >= 2, f"expected >=2, got {occurrences_cat}"
        assert occurrences_name >= 2, f"expected >=2, got {occurrences_name}"
