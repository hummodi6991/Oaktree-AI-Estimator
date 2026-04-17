"""Unit tests for ``_expand_category_terms`` category expansion.

The realized-demand aggregation builds its SQL ``LIKE`` filter from the
terms returned here, so the expanded set must contain the raw strings
that actually appear in ``expansion_delivery_rating_history.cuisine_raw``
(English bucket names *and* Arabic cuisine tags). No DB access — these
are pure assertions on the in-memory term-expansion map.
"""

from app.services.expansion_advisor import _expand_category_terms


def test_cafe_expands_to_coffee():
    assert "coffee" in _expand_category_terms("cafe")


def test_chicken_includes_traditional_and_arabic_tag():
    terms = _expand_category_terms("chicken")
    assert "traditional" in terms
    assert "دجاج" in terms


def test_arabic_includes_traditional_and_arabic_tag():
    terms = _expand_category_terms("arabic")
    assert "traditional" in terms
    assert "عربي" in terms


def test_shawarma_includes_traditional_and_arabic_tag():
    terms = _expand_category_terms("shawarma")
    assert "traditional" in terms
    assert "شاورما" in terms


def test_grills_includes_arabic_tag():
    assert "مشويات" in _expand_category_terms("grills")


def test_burger_includes_international_and_original():
    terms = _expand_category_terms("burger")
    assert "international" in terms
    assert "burger" in terms


def test_saudi_arabic_alias_resolves_to_traditional_and_grills():
    # Reverse lookup: raw Arabic term → canonical "arabic" → its bucket list.
    terms = _expand_category_terms("سعودي")
    assert "traditional" in terms
    assert "grills" in terms


def test_unknown_category_falls_back_to_international():
    assert "international" in _expand_category_terms("UNKNOWN_CATEGORY_XYZ")
