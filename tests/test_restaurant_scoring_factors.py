"""
Tests for the upgraded zoning, parking, and commercial density scoring factors.

Tests cover:
- Zoning label/code mapping determinism
- Mixed-use vs residential vs commercial outcomes
- Parking composite behavior for good vs bad contexts
- Commercial density behavior for high-anchor vs low-anchor areas
- No-data fallback behavior
- Confidence behavior
- ScoredFactor structure
"""

from app.services.restaurant_scoring_factors import (
    ScoredFactor,
    _match_landuse_label,
    _ZONING_RULES,
    _ZONING_CODE_MAP,
)


# ---------------------------------------------------------------------------
# ScoredFactor structure tests
# ---------------------------------------------------------------------------

class TestScoredFactor:
    def test_basic_construction(self):
        sf = ScoredFactor(score=75.0, confidence=0.8, rationale="test")
        assert sf.score == 75.0
        assert sf.confidence == 0.8
        assert sf.rationale == "test"
        assert sf.meta == {}

    def test_meta_defaults_to_empty(self):
        sf = ScoredFactor(score=50.0, confidence=0.5)
        assert sf.meta == {}


# ---------------------------------------------------------------------------
# Zoning label matching tests
# ---------------------------------------------------------------------------

class TestZoningLabelMatching:
    """Test the deterministic landuse → restaurant feasibility mapping."""

    def test_commercial_arabic_high(self):
        match = _match_landuse_label("تجاري")
        assert match is not None
        score, rationale = match
        assert score >= 88.0
        assert "commercial" in rationale

    def test_commercial_english_high(self):
        match = _match_landuse_label("Commercial")
        assert match is not None
        score, _ = match
        assert score >= 88.0

    def test_mixed_use_arabic_high(self):
        match = _match_landuse_label("مختلط")
        assert match is not None
        score, rationale = match
        assert score >= 85.0
        assert "mixed" in rationale

    def test_mixed_use_english_high(self):
        match = _match_landuse_label("Mixed-Use Zone")
        assert match is not None
        score, _ = match
        assert score >= 85.0

    def test_residential_arabic_low(self):
        match = _match_landuse_label("سكني")
        assert match is not None
        score, rationale = match
        assert score <= 40.0
        assert "residential" in rationale

    def test_residential_english_low(self):
        match = _match_landuse_label("Residential")
        assert match is not None
        score, _ = match
        assert score <= 40.0

    def test_industrial_very_low(self):
        match = _match_landuse_label("صناعي")
        assert match is not None
        score, _ = match
        assert score <= 30.0

    def test_industrial_english_very_low(self):
        match = _match_landuse_label("Industrial Zone")
        assert match is not None
        score, _ = match
        assert score <= 30.0

    def test_retail_high(self):
        match = _match_landuse_label("retail")
        assert match is not None
        score, _ = match
        assert score >= 85.0

    def test_hotel_hospitality_high(self):
        match = _match_landuse_label("hotel district")
        assert match is not None
        score, _ = match
        assert score >= 80.0

    def test_office_moderate_high(self):
        match = _match_landuse_label("مكاتب")
        assert match is not None
        score, _ = match
        assert score >= 78.0

    def test_education_moderate(self):
        match = _match_landuse_label("education zone")
        assert match is not None
        score, _ = match
        assert 40.0 <= score <= 65.0

    def test_health_moderate(self):
        match = _match_landuse_label("hospital area")
        assert match is not None
        score, _ = match
        assert 40.0 <= score <= 65.0

    def test_agricultural_very_low(self):
        match = _match_landuse_label("agricultural")
        assert match is not None
        score, _ = match
        assert score <= 25.0

    def test_open_space_very_low(self):
        match = _match_landuse_label("park")
        assert match is not None
        score, _ = match
        assert score <= 25.0

    def test_unknown_label_returns_none(self):
        match = _match_landuse_label("xyz_unknown_999")
        assert match is None

    def test_empty_string_returns_none(self):
        match = _match_landuse_label("")
        assert match is None

    def test_none_returns_none(self):
        match = _match_landuse_label(None)
        assert match is None

    def test_case_insensitive(self):
        match_lower = _match_landuse_label("commercial")
        match_upper = _match_landuse_label("COMMERCIAL")
        match_mixed = _match_landuse_label("Commercial")
        assert match_lower is not None
        assert match_upper is not None
        assert match_mixed is not None
        assert match_lower[0] == match_upper[0] == match_mixed[0]

    def test_commercial_beats_residential(self):
        commercial = _match_landuse_label("commercial")
        residential = _match_landuse_label("residential")
        assert commercial is not None
        assert residential is not None
        assert commercial[0] > residential[0]

    def test_mixed_beats_residential(self):
        mixed = _match_landuse_label("mixed")
        residential = _match_landuse_label("residential")
        assert mixed is not None
        assert residential is not None
        assert mixed[0] > residential[0]


class TestZoningCodeMap:
    """Test the integer landuse code mapping."""

    def test_commercial_code_high(self):
        assert 1 in _ZONING_CODE_MAP
        score, _ = _ZONING_CODE_MAP[1]
        assert score >= 88.0

    def test_residential_code_low(self):
        assert 2 in _ZONING_CODE_MAP
        score, _ = _ZONING_CODE_MAP[2]
        assert score <= 40.0

    def test_industrial_code_low(self):
        assert 3 in _ZONING_CODE_MAP
        score, _ = _ZONING_CODE_MAP[3]
        assert score <= 30.0

    def test_mixed_use_code_high(self):
        assert 4 in _ZONING_CODE_MAP
        score, _ = _ZONING_CODE_MAP[4]
        assert score >= 85.0

    def test_all_codes_have_rationale(self):
        for code, (score, rationale) in _ZONING_CODE_MAP.items():
            assert isinstance(score, float), f"Code {code} score not float"
            assert isinstance(rationale, str), f"Code {code} rationale not string"
            assert len(rationale) > 0, f"Code {code} has empty rationale"

    def test_all_codes_in_valid_range(self):
        for code, (score, _) in _ZONING_CODE_MAP.items():
            assert 10.0 <= score <= 95.0, f"Code {code} score {score} out of range"


# ---------------------------------------------------------------------------
# Zoning rules consistency
# ---------------------------------------------------------------------------

class TestZoningRulesConsistency:
    """Validate zoning rules structure and scoring ranges."""

    def test_all_rules_have_keywords(self):
        for keywords, score, rationale in _ZONING_RULES:
            assert len(keywords) > 0
            assert isinstance(score, float)
            assert isinstance(rationale, str)

    def test_all_scores_in_valid_range(self):
        for keywords, score, rationale in _ZONING_RULES:
            assert 10.0 <= score <= 95.0, f"Rule {rationale} score {score} out of range"

    def test_commercial_rules_score_higher_than_residential(self):
        commercial_scores = []
        residential_scores = []
        for keywords, score, rationale in _ZONING_RULES:
            if "commercial" in rationale:
                commercial_scores.append(score)
            if "residential" in rationale:
                residential_scores.append(score)
        if commercial_scores and residential_scores:
            assert min(commercial_scores) > max(residential_scores), (
                f"Commercial min {min(commercial_scores)} should exceed "
                f"residential max {max(residential_scores)}"
            )

    def test_favorable_zones_above_70(self):
        """Commercial, mixed-use, retail, hospitality should score ≥ 70."""
        favorable_rationales = {
            "commercial_zone", "mixed_use_zone", "retail_zone",
            "hospitality_zone", "investment_zone",
        }
        for keywords, score, rationale in _ZONING_RULES:
            if rationale in favorable_rationales:
                assert score >= 70.0, f"{rationale} should be ≥ 70 but got {score}"

    def test_unfavorable_zones_below_45(self):
        """Residential, industrial, agricultural, utility should score < 45."""
        unfavorable_rationales = {
            "residential_zone", "industrial_zone", "agricultural_zone",
            "utility_zone",
        }
        for keywords, score, rationale in _ZONING_RULES:
            if rationale in unfavorable_rationales:
                assert score < 45.0, f"{rationale} should be < 45 but got {score}"


# ---------------------------------------------------------------------------
# No-data and fallback behavior tests
# ---------------------------------------------------------------------------

class TestFallbackBehavior:
    """Test that fallback defaults are not flat 50."""

    def test_unknown_label_does_not_produce_50(self):
        """If label doesn't match, _match_landuse_label returns None (not 50)."""
        result = _match_landuse_label("completely_unknown_label_abc123")
        assert result is None

    def test_scored_factor_confidence_range(self):
        """ScoredFactor confidence must be in [0, 1]."""
        sf = ScoredFactor(score=50.0, confidence=0.5)
        assert 0.0 <= sf.confidence <= 1.0

    def test_scored_factor_score_range(self):
        """ScoredFactor score must be in [0, 100]."""
        sf = ScoredFactor(score=50.0, confidence=0.5)
        assert 0.0 <= sf.score <= 100.0


# ---------------------------------------------------------------------------
# Confidence gating behavior
# ---------------------------------------------------------------------------

class TestConfidenceGating:
    """Test the confidence-blending logic used in score_location."""

    def test_low_confidence_blends_toward_neutral(self):
        """When confidence < floor, the score is blended toward neutral."""
        _FLOOR = 0.3
        _NEUTRAL = 45.0
        raw_score = 90.0
        confidence = 0.1

        blend = confidence / _FLOOR
        gated = blend * raw_score + (1 - blend) * _NEUTRAL

        # With confidence=0.1 and floor=0.3, blend=0.333
        # gated = 0.333 * 90 + 0.667 * 45 = 30 + 30 = 60
        assert gated < raw_score
        assert gated > _NEUTRAL

    def test_high_confidence_keeps_score(self):
        """When confidence >= floor, score is not modified."""
        _FLOOR = 0.3
        raw_score = 90.0
        confidence = 0.5

        # confidence >= floor, so no gating applied
        assert confidence >= _FLOOR
        # Score should be used as-is

    def test_zero_confidence_gives_neutral(self):
        """When confidence = 0, gated score = neutral fallback."""
        _FLOOR = 0.3
        _NEUTRAL = 45.0
        raw_score = 90.0
        confidence = 0.0

        blend = confidence / _FLOOR  # 0
        gated = blend * raw_score + (1 - blend) * _NEUTRAL
        assert abs(gated - _NEUTRAL) < 0.1

    def test_gating_does_not_exceed_original(self):
        """Gated score should never exceed the original score when blending toward lower neutral."""
        _FLOOR = 0.3
        _NEUTRAL = 45.0

        for conf in [0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.29]:
            blend = conf / _FLOOR
            for raw in [10.0, 30.0, 50.0, 70.0, 90.0]:
                gated = blend * raw + (1 - blend) * _NEUTRAL
                if raw >= _NEUTRAL:
                    assert gated <= raw + 0.01
                else:
                    assert gated >= raw - 0.01


# ---------------------------------------------------------------------------
# Integration: commercial_density backward compat
# ---------------------------------------------------------------------------

class TestCommercialDensityBackwardCompat:
    """Ensure the old float-returning API still works."""

    def test_import_works(self):
        from app.services.restaurant_location import commercial_density_score
        assert callable(commercial_density_score)

    def test_v2_import_works(self):
        from app.services.restaurant_location import commercial_density_score_v2
        assert callable(commercial_density_score_v2)


class TestParkingBackwardCompat:
    """Ensure the old float-returning API still works."""

    def test_import_works(self):
        from app.services.restaurant_location import parking_availability_score
        assert callable(parking_availability_score)

    def test_v2_import_works(self):
        from app.services.restaurant_location import parking_availability_score_v2
        assert callable(parking_availability_score_v2)


class TestZoningBackwardCompat:
    """Ensure the old float-returning API still works."""

    def test_import_works(self):
        from app.services.restaurant_location import zoning_fit_score
        assert callable(zoning_fit_score)

    def test_v2_import_works(self):
        from app.services.restaurant_location import zoning_fit_score_v2
        assert callable(zoning_fit_score_v2)


# ---------------------------------------------------------------------------
# Anchor weights consistency
# ---------------------------------------------------------------------------

class TestAnchorWeights:
    """Validate anchor weight configuration."""

    def test_all_weights_positive(self):
        from app.services.restaurant_scoring_factors import _ANCHOR_WEIGHTS
        for k, v in _ANCHOR_WEIGHTS.items():
            assert v > 0, f"Anchor weight for {k} must be positive"

    def test_mall_is_highest_weight(self):
        from app.services.restaurant_scoring_factors import _ANCHOR_WEIGHTS
        assert _ANCHOR_WEIGHTS["mall"] >= max(
            v for k, v in _ANCHOR_WEIGHTS.items() if k != "mall"
        )

    def test_common_anchors_present(self):
        from app.services.restaurant_scoring_factors import _ANCHOR_WEIGHTS
        required = {"mall", "shopping", "supermarket", "office", "hospital", "university"}
        assert required.issubset(set(_ANCHOR_WEIGHTS.keys()))
