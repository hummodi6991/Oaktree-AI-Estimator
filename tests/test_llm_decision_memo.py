"""Tests for app.services.llm_decision_memo."""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from app.services.llm_decision_memo import (
    COMPONENT_WEIGHTS,
    MemoContext,
    _daily_cost_tracker,
    _format_rent_vs_median,
    _rent_positioning,
    _today_key,
    build_memo_context,
    generate_decision_memo,
    generate_structured_memo,
    render_structured_memo_as_text,
    render_structured_memo_prompt,
)

# ── Fixtures ────────────────────────────────────────────────────────

SAMPLE_BRIEF = {
    "brand_name": "TestCafe",
    "category": "cafe",
    "service_model": "cafe",
    "min_area_m2": 40,
    "max_area_m2": 120,
    "target_area_m2": 80,
    "existing_branches": [
        {"name": "Branch 1", "lat": 24.7, "lon": 46.7, "district": "Olaya"},
    ],
    "brand_profile": {"primary_channel": "dine_in"},
}

SAMPLE_CANDIDATE = {
    "id": "cand-001",
    "parcel_id": "aqar-12345",
    "district_display": "Al Marwah",
    "district": "marwah",
    "area_m2": 65,
    "estimated_annual_rent_sar": 35000,
    "estimated_rent_sar_m2_year": 538,
    "unit_street_width_m": 20,
    "final_score": 82,
    "economics_score": 75,
    "brand_fit_score": 88,
    "demand_score": 70,
    "provider_whitespace_score": 60,
    "listing_quality_score": 72,
    "district_median_rent": 40000,
    "llm_reasoning": "Landlord excludes laundromats, near Hyper Panda.",
}

VALID_LLM_RESPONSE = {
    "headline": "GO: Al Marwah is a strong cafe fit near high footfall.",
    "fit_summary": (
        "Al Marwah offers TestCafe a competitive location with strong foot "
        "traffic. As a cafe operator, TestCafe benefits from proximity to "
        "Hyper Panda and the landlord's preference for quality tenants."
    ),
    "top_reasons_to_pursue": [
        "Strong footfall from adjacent Hyper Panda",
        "Landlord explicitly excludes low-value tenants",
        "Rent 12% below district median",
    ],
    "top_risks": [
        "Street width may limit visibility",
        "No drive-thru capability",
        "Competition from existing cafes in corridor",
    ],
    "recommended_next_action": "Schedule a site visit to confirm storefront visibility from main road.",
    "rent_context": "Annual rent of SAR 35,000 is 12% below the Al Marwah district median of SAR 40,000.",
}


def _make_mock_response(content_dict: dict | str, input_tokens: int = 500, output_tokens: int = 300):
    """Build a mock OpenAI ChatCompletion response."""
    mock = MagicMock()
    if isinstance(content_dict, dict):
        mock.choices = [MagicMock(message=MagicMock(content=json.dumps(content_dict)))]
    else:
        mock.choices = [MagicMock(message=MagicMock(content=content_dict))]
    mock.usage = MagicMock(prompt_tokens=input_tokens, completion_tokens=output_tokens)
    return mock


@pytest.fixture(autouse=True)
def _reset_cost_tracker():
    """Reset the daily cost tracker before each test."""
    _daily_cost_tracker.clear()
    yield
    _daily_cost_tracker.clear()


# ── Tests ───────────────────────────────────────────────────────────


class TestFormatRentVsMedian:
    def test_none_inputs_return_unknown_en(self):
        assert _format_rent_vs_median(None, 40000, "en") == "unknown"
        assert _format_rent_vs_median(35000, None, "en") == "unknown"
        assert _format_rent_vs_median(None, None, "en") == "unknown"

    def test_none_inputs_return_unknown_ar(self):
        assert _format_rent_vs_median(None, 40000, "ar") == "غير معروف"

    def test_in_line_with_median(self):
        assert _format_rent_vs_median(40000, 40000, "en") == "in line with median"
        assert _format_rent_vs_median(40000, 40000, "ar") == "متوافق مع المتوسط"
        # Within 5% threshold
        assert _format_rent_vs_median(41000, 40000, "en") == "in line with median"

    def test_above_median(self):
        result = _format_rent_vs_median(48000, 40000, "en")
        assert "above median" in result
        assert "20%" in result

    def test_below_median(self):
        result = _format_rent_vs_median(35000, 40000, "en")
        assert "below median" in result

    def test_below_median_ar(self):
        result = _format_rent_vs_median(35000, 40000, "ar")
        assert "أقل من المتوسط" in result

    def test_zero_median_returns_unknown(self):
        assert _format_rent_vs_median(35000, 0, "en") == "unknown"


class TestDailyCeilingBlocksCall:
    def test_ceiling_blocks_when_exceeded(self):
        from app.services.llm_decision_memo import DAILY_CEILING_USD

        today = _today_key()
        _daily_cost_tracker[today] = DAILY_CEILING_USD

        with pytest.raises(RuntimeError, match="daily cost ceiling"):
            generate_decision_memo(
                candidate=SAMPLE_CANDIDATE,
                brief=SAMPLE_BRIEF,
                lang="en",
            )


class TestSuccessfulGeneration:
    @patch("app.services.llm_decision_memo._get_client")
    def test_returns_all_fields(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_mock_response(VALID_LLM_RESPONSE)
        mock_get_client.return_value = mock_client

        result = generate_decision_memo(
            candidate=SAMPLE_CANDIDATE,
            brief=SAMPLE_BRIEF,
            lang="en",
        )

        # Shape asserts: all six legacy keys present and correctly typed.
        # Avoids byte-equality against fixture text so the test survives
        # tone iteration on the legacy template if it ever ships.
        for str_key in ("headline", "fit_summary", "recommended_next_action", "rent_context"):
            assert isinstance(result[str_key], str) and result[str_key].strip()
        for list_key in ("top_reasons_to_pursue", "top_risks"):
            assert isinstance(result[list_key], list) and len(result[list_key]) >= 1
            assert all(isinstance(item, str) and item.strip() for item in result[list_key])


class TestMissingFieldFilledGracefully:
    @patch("app.services.llm_decision_memo._get_client")
    def test_missing_list_field_filled_with_empty_list(self, mock_get_client):
        incomplete = {
            "headline": "CONSIDER: Decent spot",
            "fit_summary": "Looks OK for TestCafe.",
            "recommended_next_action": "Visit site.",
            "rent_context": "Rent is reasonable.",
            # top_reasons_to_pursue and top_risks are missing
        }
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_mock_response(incomplete)
        mock_get_client.return_value = mock_client

        result = generate_decision_memo(
            candidate=SAMPLE_CANDIDATE,
            brief=SAMPLE_BRIEF,
            lang="en",
        )

        # Missing list fields fill to empty list, not crash.
        assert result["top_risks"] == []
        assert result["top_reasons_to_pursue"] == []
        # Provided string fields survive the fill-default pass.
        assert isinstance(result["headline"], str) and result["headline"].strip()

    @patch("app.services.llm_decision_memo._get_client")
    def test_missing_string_field_filled_with_dash(self, mock_get_client):
        incomplete = {
            "headline": "GO: Good site",
            "top_reasons_to_pursue": ["reason 1"],
            "top_risks": ["risk 1"],
            # fit_summary, recommended_next_action, rent_context missing
        }
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_mock_response(incomplete)
        mock_get_client.return_value = mock_client

        result = generate_decision_memo(
            candidate=SAMPLE_CANDIDATE,
            brief=SAMPLE_BRIEF,
            lang="en",
        )

        # Missing string fields fill to placeholder so un-updated frontends
        # never see KeyError. Sentinel value is implementation detail; assert
        # presence + non-empty rather than the exact dash glyph.
        for missing_key in ("fit_summary", "recommended_next_action", "rent_context"):
            assert isinstance(result[missing_key], str) and result[missing_key].strip()


class TestInvalidJsonRaises:
    @patch("app.services.llm_decision_memo._get_client")
    def test_non_json_raises_runtime_error(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_mock_response(
            "This is not JSON at all, just plain text."
        )
        mock_get_client.return_value = mock_client

        with pytest.raises(RuntimeError, match="invalid JSON"):
            generate_decision_memo(
                candidate=SAMPLE_CANDIDATE,
                brief=SAMPLE_BRIEF,
                lang="en",
            )


class TestArabicLangUsesArabicTemplate:
    @patch("app.services.llm_decision_memo._get_client")
    def test_arabic_prompt_contains_arabic_text(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_mock_response(VALID_LLM_RESPONSE)
        mock_get_client.return_value = mock_client

        generate_decision_memo(
            candidate=SAMPLE_CANDIDATE,
            brief=SAMPLE_BRIEF,
            lang="ar",
        )

        # Capture the prompt sent to the mock client. Heuristic check: the
        # legacy Arabic template contains Arabic script in the system turn
        # (not a specific phrase that would couple the test to wording).
        call_args = mock_client.chat.completions.create.call_args
        messages = call_args.kwargs.get("messages") or call_args[1].get("messages", [])
        prompt_text = messages[0]["content"]

        # At least one Arabic-script character must appear so we know the
        # locale switch fired.
        assert any("؀" <= ch <= "ۿ" for ch in prompt_text), (
            "Arabic prompt template did not emit Arabic-script content"
        )


# ── Structured memo (Phase 1) ───────────────────────────────────────


VALID_STRUCTURED_RESPONSE = {
    "headline_recommendation": "recommend — 15% below-median rent with strong delivery pull",
    "ranking_explanation": (
        "occupancy_economics contributed 27.0 out of 30 and brand_fit 8.8 out of 11, "
        "driving rank 2 of 12. Access_visibility at 6.5 of 10 was the only sub-median "
        "component and the gap to rank 1 is narrow."
    ),
    "key_evidence": [
        {"signal": "annual rent", "value": "SAR 480,000/yr", "implication": "15% below Al Olaya median", "polarity": "positive"},
        {"signal": "realized demand 30d", "value": "1,400 orders", "implication": "7.8x district median", "polarity": "positive"},
    ],
    "risks": [
        {"risk": "street width 8 m limits drive-thru", "mitigation": "curbside handoff via Keeta"},
    ],
    "comparison": "Matches Peer A on rent and beats Peer B on realized demand by 3x.",
    "bottom_line": "Take it — the rent alone justifies the deal.",
    # PR #3: typed advisory sections (Design 2 — backend assembles numeric
    # fields, LLM writes summary + thesis prose). Numeric fields are echoed
    # from the typed payload; summary / thesis are LLM-authored.
    "property_overview": {
        "summary": "120 m² unit on 8 m street; listed 14 days ago in Al Olaya.",
        "area_m2": 120,
        "frontage_width_m": 8,
        "street_type": "secondary",
        "parking_evidence": "shared",
        "visibility_score": 65,
        "listing_age_days": 14,
        "vacancy_status": "vacant",
    },
    "financial_framing": {
        "summary": "SAR 480,000/yr cheaper than about 78% of 14 district comparables.",
        "thesis": (
            "Rent is the spine of the case at this site. SAR 480,000/yr is decisively "
            "below the SAR 560,000 district median across 14 peer listings — a SAR 80k/yr "
            "cushion that compounds across a five-year lease. The cheaper-than-78% reading "
            "is district-scoped, supporting the underwriting on a same-catchment basis."
        ),
        "annual_rent_sar": 480000,
        "comparable_median_annual_rent_sar": 560000,
        "rent_percentile_vs_comparables": 0.22,
        "comparable_n": 14,
        "comparable_scope": "district",
        "spread_to_median_sar": -80000,
    },
    "market_context": {
        "summary": "1,400 orders/30d realized across 6 branches with rising district momentum.",
        "demand_thesis": (
            "Demand is observable, not modelled. 1,400 orders over the trailing 30 days "
            "across 6 active branches in the district is meaningful evidence the category "
            "trades. District momentum reads rising on the 30-day window, which supports "
            "the underwriting on a 36-month horizon."
        ),
        "population_reach": 35000,
        "district_momentum": "rising",
        "realized_demand_30d": 1400,
        "realized_demand_branches": 6,
        "delivery_listing_count": 22,
    },
    "competitive_landscape": {
        "summary": "Peer A and Peer B operate within 500 m; rank 2 sits cheaper than ~53% of comparables.",
        "saturation_thesis": (
            "Two named chains operate within 500 m — Peer A and Peer B — confirming the "
            "category trades and raising the bar on differentiation. Rank 2 in this search "
            "sits cheaper than ~53% of comparables and at access/visibility score "
            "of 71/100, materially weaker than this site on both axes."
        ),
        "top_chains": [
            {"display_name_en": "Peer A", "display_name_ar": None, "branch_count": 2, "nearest_distance_m": 180},
            {"display_name_en": "Peer B", "display_name_ar": None, "branch_count": 1, "nearest_distance_m": 320},
        ],
        "comparable_competitors": [
            {"id": "comp-1", "name": "Peer A", "score": 0.78},
            {"id": "comp-2", "name": "Peer B", "score": 0.71},
        ],
        "next_candidate_summary": {
            "rank": 2,
            "candidate_id": "cand-rank-2",
            "district": "Al Olaya",
            "annual_rent_sar": 510000,
            "rent_percentile_vs_comparables": 0.47,
            "access_visibility_score": 71,
        },
    },
}


BASE_STRUCTURED_CANDIDATE = {
    "id": "cand-structured-1",
    "parcel_id": "parcel-9",
    "rank_position": 2,
    "feature_snapshot_json": {
        "district": "Al Olaya",
        "district_display": "العليا",
        "area_m2": 120,
        "estimated_annual_rent_sar": 480000,
        "district_median_rent": 560000,
        "unit_street_width_m": 8,
    },
    "score_breakdown_json": {
        "occupancy_economics": 90,
        "listing_quality": 70,
        "brand_fit": 80,
        "competition_whitespace": 60,
        "demand_potential": 75,
        "access_visibility": 65,
        "landlord_signal": 55,
        "delivery_demand": 50,
        "confidence": 85,
    },
    "gate_status_json": [
        {"gate": "zoning_fit_pass", "verdict": "pass", "reason": "C-2 allowed"},
        {"gate": "rent_reasonable", "verdict": "pass", "reason": "15% below median"},
    ],
    "comparable_competitors_json": [
        {"name": "Peer A", "district": "Al Olaya"},
        {"name": "Peer B", "district": "Al Olaya"},
    ],
}

BASE_STRUCTURED_BRIEF = {
    "brand_name": "BurgerCo",
    "category": "QSR",
    "service_model": "qsr",
    "min_area_m2": 100,
    "max_area_m2": 200,
    "target_area_m2": 120,
}


def _mock_client_returning(content, input_tokens: int = 400, output_tokens: int = 220):
    """Build a mocked OpenAI client whose .chat.completions.create returns a canned reply."""
    client = MagicMock()
    client.chat.completions.create.return_value = _make_mock_response(
        content, input_tokens=input_tokens, output_tokens=output_tokens
    )
    return client


class TestBuildMemoContextContributionsMath:
    """Step 8, test 9."""

    def test_contributions_equal_weight_times_score_for_all_nine(self):
        ctx = build_memo_context(
            candidate=BASE_STRUCTURED_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        scores = BASE_STRUCTURED_CANDIDATE["score_breakdown_json"]
        contributions = ctx.score_breakdown["contributions"]
        # All nine components represented
        assert set(contributions.keys()) == set(COMPONENT_WEIGHTS.keys())
        for comp, weight in COMPONENT_WEIGHTS.items():
            expected = round(weight * scores[comp], 3)
            assert contributions[comp] == expected, f"{comp}: got {contributions[comp]}, want {expected}"
        # Spot-check the headline number the prompt expects to see.
        # 2026-05-07 rebalance: occupancy_economics weight 0.30 → 0.262924,
        # so contribution at score=90 = round(90 * 0.262924, 3) = 23.663.
        assert contributions["occupancy_economics"] == 23.663
        # Weights sub-dict carried through for the LLM
        assert ctx.score_breakdown["weights"] == dict(COMPONENT_WEIGHTS)


# ── PR #3: typed advisory-section assembly ──────────────────────────


def _make_full_advisory_candidate() -> dict:
    """Candidate with every advisory-section input populated."""
    return {
        "id": "cand-advisory-1",
        "parcel_id": "parcel-7",
        "rank_position": 1,
        "feature_snapshot_json": {
            "area_m2": 180,
            "unit_street_width_m": 24,
            "access_visibility_score": 82,
            "estimated_annual_rent_sar": 432000,
            "comparable_median_annual_rent_sar": 542000,
            "comparable_n": 14,
            "comparable_source_label": "district_type",
            "population_reach": 41000,
            "district_momentum": {
                "momentum_score": 72,
                "sample_floor_applied": False,
            },
            "realized_demand_30d": 380,
            "realized_demand_branches": 6,
            "delivery_listing_count": 22,
            "candidate_location": {
                "is_vacant": True,
            },
            "listing_age": {"created_days": 64},
            "brand_presence": {
                "top_chains": [
                    {
                        "display_name_en": "Peer Chain A",
                        "display_name_ar": None,
                        "branch_count": 2,
                        "nearest_distance_m": 180,
                    },
                    {
                        "display_name_en": "Peer Chain B",
                        "display_name_ar": None,
                        "branch_count": 1,
                        "nearest_distance_m": 320,
                    },
                ],
            },
        },
        "score_breakdown_json": {
            "occupancy_economics": 85,
            "listing_quality": 70,
            "brand_fit": 78,
            "competition_whitespace": 60,
            "demand_potential": 80,
            "access_visibility": 82,
            "landlord_signal": 60,
            "delivery_demand": 70,
            "confidence": 80,
            "economics_detail": {"rent_burden": {"percentile": 0.28}},
            "inputs": {"parking_evidence_band": "moderate"},
        },
        "comparable_competitors_json": [
            {"id": "comp-1", "name": "Peer Chain A", "score": 0.78},
            {"id": "comp-2", "name": "Peer Chain B", "score": 0.71},
        ],
    }


def _make_brief() -> dict:
    return {
        "brand_name": "BurgerCo",
        "category": "QSR",
        "service_model": "qsr",
    }


class TestBuildMemoAdvisorySections:
    """PR #3: backend deterministically assembles typed advisory sections."""

    def test_full_data_populates_all_sections(self):
        from app.services.llm_decision_memo import build_memo_advisory_sections
        ctx = build_memo_context(
            candidate=_make_full_advisory_candidate(),
            brief=_make_brief(),
            lang="en",
            next_candidate_summary={
                "rank": 2,
                "candidate_id": "cand-rank-2",
                "district": "Al Olaya",
                "annual_rent_sar": 488000,
                "rent_percentile_vs_comparables": 0.47,
                "access_visibility_score": 71,
            },
        )
        sections = build_memo_advisory_sections(ctx)
        assert set(sections.keys()) == {
            "property_overview",
            "financial_framing",
            "market_context",
            "competitive_landscape",
        }
        assert sections["property_overview"]["area_m2"] == 180
        assert sections["property_overview"]["frontage_width_m"] == 24
        assert sections["property_overview"]["visibility_score"] == 82
        assert sections["property_overview"]["listing_age_days"] == 64
        assert sections["property_overview"]["vacancy_status"] == "vacant"
        # parking_evidence band "moderate" collapses to "shared"
        assert sections["property_overview"]["parking_evidence"] == "shared"
        # summary / thesis fields are LEFT EMPTY for the LLM to fill
        assert sections["property_overview"]["summary"] == ""

        assert sections["financial_framing"]["annual_rent_sar"] == 432000.0
        assert sections["financial_framing"]["comparable_median_annual_rent_sar"] == 542000.0
        assert sections["financial_framing"]["rent_percentile_vs_comparables"] == 0.28
        assert sections["financial_framing"]["comparable_n"] == 14
        assert sections["financial_framing"]["comparable_scope"] == "district"
        # spread = 432_000 - 542_000 = -110_000
        assert sections["financial_framing"]["spread_to_median_sar"] == -110000.0
        assert sections["financial_framing"]["thesis"] == ""

        assert sections["market_context"]["population_reach"] == 41000
        assert sections["market_context"]["district_momentum"] == "rising"
        assert sections["market_context"]["realized_demand_30d"] == 380
        assert sections["market_context"]["realized_demand_branches"] == 6
        assert sections["market_context"]["delivery_listing_count"] == 22
        assert sections["market_context"]["demand_thesis"] == ""

        cl = sections["competitive_landscape"]
        assert len(cl["top_chains"]) == 2
        assert cl["top_chains"][0]["display_name_en"] == "Peer Chain A"
        assert len(cl["comparable_competitors"]) == 2
        assert cl["next_candidate_summary"] is not None
        assert cl["next_candidate_summary"]["rank"] == 2
        assert cl["saturation_thesis"] == ""

    def test_missing_comparable_returns_null_financial_fields(self):
        from app.services.llm_decision_memo import build_memo_advisory_sections
        cand = _make_full_advisory_candidate()
        # Remove comparable rent context (simulates pre-PR-#1 backfill state)
        cand["feature_snapshot_json"].pop("comparable_median_annual_rent_sar", None)
        cand["feature_snapshot_json"].pop("comparable_n", None)
        cand["feature_snapshot_json"].pop("comparable_source_label", None)
        ctx = build_memo_context(candidate=cand, brief=_make_brief(), lang="en")
        sections = build_memo_advisory_sections(ctx)
        ff = sections["financial_framing"]
        # Annual rent is still populated (it lives on the listing itself)
        assert ff["annual_rent_sar"] == 432000.0
        # Comparable-derived fields all collapse to None — never zero,
        # never a default. This is the structural fix for the v4.1 leak.
        assert ff["comparable_median_annual_rent_sar"] is None
        assert ff["comparable_n"] is None
        assert ff["comparable_scope"] is None
        assert ff["spread_to_median_sar"] is None

    def test_no_rank_2_returns_null_next_candidate_summary(self):
        from app.services.llm_decision_memo import build_memo_advisory_sections
        # next_candidate_summary defaults to None when not passed in
        ctx = build_memo_context(
            candidate=_make_full_advisory_candidate(),
            brief=_make_brief(),
            lang="en",
        )
        sections = build_memo_advisory_sections(ctx)
        assert sections["competitive_landscape"]["next_candidate_summary"] is None

    def test_empty_brand_presence_returns_empty_top_chains(self):
        from app.services.llm_decision_memo import build_memo_advisory_sections
        cand = _make_full_advisory_candidate()
        cand["feature_snapshot_json"]["brand_presence"] = {"top_chains": []}
        ctx = build_memo_context(candidate=cand, brief=_make_brief(), lang="en")
        sections = build_memo_advisory_sections(ctx)
        assert sections["competitive_landscape"]["top_chains"] == []

    def test_scope_inference_from_source_label(self):
        from app.services.llm_decision_memo import _scope_from_source_label
        assert _scope_from_source_label("district_type") == "district"
        assert _scope_from_source_label("district_band") == "district"
        assert _scope_from_source_label("city_band_type") == "city_band"
        assert _scope_from_source_label("city_anything") == "city"
        assert _scope_from_source_label(None) is None
        assert _scope_from_source_label("") is None
        assert _scope_from_source_label("nonsense") is None

    def test_spread_to_median_sign(self):
        from app.services.llm_decision_memo import build_memo_advisory_sections
        # Above-median rent — positive spread
        cand = _make_full_advisory_candidate()
        cand["feature_snapshot_json"]["estimated_annual_rent_sar"] = 600000
        cand["feature_snapshot_json"]["comparable_median_annual_rent_sar"] = 542000
        ctx = build_memo_context(candidate=cand, brief=_make_brief(), lang="en")
        assert build_memo_advisory_sections(ctx)["financial_framing"]["spread_to_median_sar"] == 58000.0

        # Below-median rent — negative spread
        cand["feature_snapshot_json"]["estimated_annual_rent_sar"] = 432000
        cand["feature_snapshot_json"]["comparable_median_annual_rent_sar"] = 542000
        ctx = build_memo_context(candidate=cand, brief=_make_brief(), lang="en")
        assert build_memo_advisory_sections(ctx)["financial_framing"]["spread_to_median_sar"] == -110000.0

        # Either input null — spread null
        cand["feature_snapshot_json"].pop("comparable_median_annual_rent_sar", None)
        ctx = build_memo_context(candidate=cand, brief=_make_brief(), lang="en")
        assert build_memo_advisory_sections(ctx)["financial_framing"]["spread_to_median_sar"] is None


class TestGenerateStructuredMemoHappyPath:
    """Step 8, test 1 — service level."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_returns_parsed_dict_with_six_keys(self, mock_get_client):
        mock_get_client.return_value = _mock_client_returning(VALID_STRUCTURED_RESPONSE)

        ctx = build_memo_context(
            candidate=BASE_STRUCTURED_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        result = generate_structured_memo(ctx)

        assert isinstance(result, dict)
        expected_keys = {
            "headline_recommendation",
            "ranking_explanation",
            "key_evidence",
            "risks",
            "comparison",
            "bottom_line",
        }
        assert set(result.keys()) >= expected_keys
        assert result["bottom_line"] == VALID_STRUCTURED_RESPONSE["bottom_line"]


class TestGenerateStructuredMemoMalformedJsonFallback:
    """Step 8, test 2."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_non_json_returns_none(self, mock_get_client, caplog):
        mock_get_client.return_value = _mock_client_returning(
            "this is not json at all, just prose"
        )

        ctx = build_memo_context(
            candidate=BASE_STRUCTURED_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        with caplog.at_level("WARNING"):
            result = generate_structured_memo(ctx)

        assert result is None
        assert any("JSON parse failed" in rec.message for rec in caplog.records)


class TestGenerateStructuredMemoMissingKeyFallback:
    """Step 8, test 3."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_missing_bottom_line_returns_none(self, mock_get_client, caplog):
        incomplete = {k: v for k, v in VALID_STRUCTURED_RESPONSE.items() if k != "bottom_line"}
        mock_get_client.return_value = _mock_client_returning(incomplete)

        ctx = build_memo_context(
            candidate=BASE_STRUCTURED_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        with caplog.at_level("WARNING"):
            result = generate_structured_memo(ctx)

        assert result is None
        assert any("missing keys" in rec.message for rec in caplog.records)


class TestGenerateStructuredMemoExceptionFallback:
    """Step 8, test 4."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_timeout_returns_none_no_raise(self, mock_get_client, caplog):
        client = MagicMock()
        client.chat.completions.create.side_effect = TimeoutError("llm timed out")
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=BASE_STRUCTURED_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        with caplog.at_level("WARNING"):
            # Must not raise
            result = generate_structured_memo(ctx)

        assert result is None
        assert any("OpenAI call failed" in rec.message for rec in caplog.records)


class TestRenderPromptRealizedDemandPresent:
    """Step 8, test 5."""

    def test_user_message_contains_realized_demand_numbers(self):
        cand = dict(BASE_STRUCTURED_CANDIDATE)
        cand["realized_demand_30d"] = 1400
        cand["realized_demand_branches"] = 8
        cand["realized_demand_district_median"] = 180

        ctx = build_memo_context(candidate=cand, brief=BASE_STRUCTURED_BRIEF, lang="en")
        messages = render_structured_memo_prompt(ctx)
        user_content = messages[1]["content"]

        # Serialized numbers appear verbatim
        assert "1400" in user_content
        assert '"branch_count": 8' in user_content or "\"branch_count\":8" in user_content
        assert "180" in user_content
        # System prompt was upgraded with the REALIZED DEMAND addendum.
        assert "REALIZED DEMAND" in messages[0]["content"]


class TestRenderPromptFailedGate:
    """Step 8, test 6.

    A genuinely failed gate still triggers the GATE FAILURE addendum, and
    the base rule sections are always present in the system prompt so the
    LLM sees HARD RULES + GATE LANGUAGE RULES regardless of input.
    """

    def test_failed_gate_triggers_failure_addendum_and_base_rule_sections(self):
        cand = dict(BASE_STRUCTURED_CANDIDATE)
        # Realistic production shape: gate_status_json is the flat
        # raw-keyed bool/None map; gate_reasons_json is the bucketed
        # humanized view. PR #2b's blocking/advisory split resolves the
        # raw key (zoning_fit_pass) from gate_status_json.
        cand["gate_status_json"] = {
            "overall_pass": False,
            "zoning_fit_pass": False,
            "economics_pass": True,
        }
        cand["gate_reasons_json"] = {
            "passed": ["economics"],
            "failed": ["zoning fit"],
            "unknown": [],
            "explanations": {"zoning fit": "C-2 not allowed on this parcel"},
        }

        ctx = build_memo_context(candidate=cand, brief=BASE_STRUCTURED_BRIEF, lang="en")
        messages = render_structured_memo_prompt(ctx)
        system_content = messages[0]["content"]

        # Base rule sections present in the v2 humanized prompt.
        assert "HARD RULES" in system_content
        assert "GATE LANGUAGE RULES" in system_content

        # Failure-language is still permitted/encouraged for a genuinely
        # failed gate, so the GATE FAILURE situational addendum fires.
        assert "GATE FAILURE" in system_content
        assert "zoning fit" in system_content


class TestRenderPromptArabicLocale:
    """Step 8, test 7."""

    def test_arabic_locale_adds_msa_instruction(self):
        ctx = build_memo_context(
            candidate=BASE_STRUCTURED_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="ar",
        )
        messages = render_structured_memo_prompt(ctx)
        system_content = messages[0]["content"]

        assert "Modern Standard Arabic" in system_content
        assert ctx.locale == "ar"


class TestGenerateStructuredMemoFlagOff:
    """Step 8, test 8."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_flag_off_returns_none_without_calling_client(self, mock_get_client, monkeypatch):
        # IMPORTANT: patch the settings binding the function actually reads,
        # not app.core.config.settings. Other tests (test_config_settings,
        # test_parcel_table_overrides) reload app.core.config during the
        # suite, which creates a fresh settings singleton — but
        # llm_decision_memo.settings was bound at its own import time and
        # still references the original instance.
        import app.services.llm_decision_memo as memo_mod
        monkeypatch.setattr(memo_mod.settings, "EXPANSION_MEMO_STRUCTURED_ENABLED", False)

        ctx = build_memo_context(
            candidate=BASE_STRUCTURED_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        result = generate_structured_memo(ctx)

        assert result is None
        mock_get_client.assert_not_called()


# ── Endpoint integration (happy path + persistence + ceiling) ───────


class _DummyRow:
    def __init__(self, values):
        self._values = values

    def __getitem__(self, i):
        return self._values[i]


class _DummyResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _DummyDB:
    """In-memory DB stub supporting the two SQL statements the endpoint uses."""

    def __init__(self, preload_row=None):
        self.executed: list[tuple[str, dict]] = []
        self.committed = False
        self.rolled_back = False
        self._preload_row = preload_row
        self.persisted: dict | None = None

    def execute(self, stmt, params=None):
        sql_text = stmt.text if hasattr(stmt, "text") else str(stmt)
        self.executed.append((sql_text, dict(params or {})))
        if "SELECT" in sql_text:
            return _DummyResult(self._preload_row)
        if "UPDATE" in sql_text:
            self.persisted = dict(params or {})
            return _DummyResult(None)
        return _DummyResult(None)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def _endpoint_client(db):
    from fastapi.testclient import TestClient
    from app.db.deps import get_db
    from app.main import app

    def override_get_db():
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app, raise_server_exceptions=False)


class TestDecisionMemoEndpointHappyPathPersists:
    """Step 8, test 1 — endpoint persists both columns on structured success."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_endpoint_persists_memo_text_and_memo_json(self, mock_get_client):
        mock_get_client.return_value = _mock_client_returning(VALID_STRUCTURED_RESPONSE)

        db = _DummyDB(preload_row=None)  # cache miss
        client = _endpoint_client(db)

        payload = {
            "candidate": BASE_STRUCTURED_CANDIDATE,
            "brief": BASE_STRUCTURED_BRIEF,
            "lang": "en",
            "search_id": "search-1",
            "parcel_id": "parcel-9",
        }
        resp = client.post("/v1/expansion-advisor/decision-memo", json=payload)
        assert resp.status_code == 200, resp.text
        body = resp.json()

        assert body["cached"] is False
        assert body["memo_json"] is not None
        assert body["memo_json"]["bottom_line"] == VALID_STRUCTURED_RESPONSE["bottom_line"]
        assert isinstance(body["memo_text"], str) and body["memo_text"].startswith("## Headline Recommendation")

        # Persisted with both columns populated
        assert db.persisted is not None
        assert db.persisted["sid"] == "search-1"
        assert db.persisted["pid"] == "parcel-9"
        assert isinstance(db.persisted["txt"], str)
        persisted_json = json.loads(db.persisted["j"])
        assert persisted_json["bottom_line"] == VALID_STRUCTURED_RESPONSE["bottom_line"]
        assert db.committed is True


class TestDecisionMemoEndpointMalformedFallsBackToLegacy:
    """Step 8, test 2 — endpoint persists only text on legacy fallback."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_malformed_falls_back_and_persists_only_text(self, mock_get_client):
        # Structured returns non-JSON; legacy then called — we stub both via
        # a side_effect list on the SAME mocked client.
        structured_bad = _make_mock_response("not json at all")
        legacy_good = _make_mock_response(VALID_LLM_RESPONSE)
        client_mock = MagicMock()
        client_mock.chat.completions.create.side_effect = [structured_bad, legacy_good]
        mock_get_client.return_value = client_mock

        db = _DummyDB(preload_row=None)
        api = _endpoint_client(db)

        payload = {
            "candidate": BASE_STRUCTURED_CANDIDATE,
            "brief": BASE_STRUCTURED_BRIEF,
            "lang": "en",
            "search_id": "search-1",
            "parcel_id": "parcel-9",
        }
        resp = api.post("/v1/expansion-advisor/decision-memo", json=payload)
        assert resp.status_code == 200, resp.text
        body = resp.json()

        assert body["memo_json"] is None
        assert isinstance(body["memo_text"], str) and body["memo_text"]
        # Legacy-shape memo under "memo" (not structured)
        assert body["memo"]["headline"] == VALID_LLM_RESPONSE["headline"]

        # Persistence: text yes, JSON null
        assert db.persisted is not None
        assert db.persisted["txt"]
        assert db.persisted["j"] is None


class TestDecisionMemoEndpointCeilingStillReturns503:
    """Ceiling-breach path consistency — the 503 contract must hold."""

    def test_ceiling_breach_returns_503(self):
        db = _DummyDB(preload_row=None)
        api = _endpoint_client(db)

        # Pin the tracker above the ceiling so both structured AND legacy
        # short-circuit on _check_daily_ceiling(). Structured returns None
        # (fallback), legacy raises RuntimeError, endpoint → 503.
        today = _today_key()
        _daily_cost_tracker[today] = 999.0
        try:
            payload = {
                "candidate": BASE_STRUCTURED_CANDIDATE,
                "brief": BASE_STRUCTURED_BRIEF,
                "lang": "en",
                "search_id": "search-1",
                "parcel_id": "parcel-9",
            }
            resp = api.post("/v1/expansion-advisor/decision-memo", json=payload)
        finally:
            _daily_cost_tracker.clear()

        assert resp.status_code == 503


class TestDecisionMemoEndpointMemoIsLegacyShape:
    """Backward-compat contract: response['memo'] is always a legacy-shape
    dict so un-updated frontends reading memo.headline never crash.
    """

    @patch("app.services.llm_decision_memo._get_client")
    def test_structured_path_memo_has_all_six_legacy_keys_populated(self, mock_get_client):
        mock_get_client.return_value = _mock_client_returning(VALID_STRUCTURED_RESPONSE)

        db = _DummyDB(preload_row=None)
        api = _endpoint_client(db)

        payload = {
            "candidate": BASE_STRUCTURED_CANDIDATE,
            "brief": BASE_STRUCTURED_BRIEF,
            "lang": "en",
            "search_id": "search-legacy-shape",
            "parcel_id": "parcel-9",
        }
        resp = api.post("/v1/expansion-advisor/decision-memo", json=payload)
        assert resp.status_code == 200, resp.text
        memo = resp.json()["memo"]

        # All six legacy keys present and non-empty
        for key in (
            "headline",
            "fit_summary",
            "top_reasons_to_pursue",
            "top_risks",
            "recommended_next_action",
            "rent_context",
        ):
            assert key in memo, f"missing legacy key: {key}"
            assert memo[key], f"legacy key empty: {key}"

        # Headline maps from headline_recommendation
        assert memo["headline"] == VALID_STRUCTURED_RESPONSE["headline_recommendation"]
        # fit_summary maps from ranking_explanation
        assert memo["fit_summary"] == VALID_STRUCTURED_RESPONSE["ranking_explanation"]
        # recommended_next_action maps from bottom_line
        assert memo["recommended_next_action"] == VALID_STRUCTURED_RESPONSE["bottom_line"]

        # Lists are non-empty when source key_evidence / risks are non-empty
        assert isinstance(memo["top_reasons_to_pursue"], list)
        assert len(memo["top_reasons_to_pursue"]) == len(VALID_STRUCTURED_RESPONSE["key_evidence"])
        assert memo["top_reasons_to_pursue"][0] == VALID_STRUCTURED_RESPONSE["key_evidence"][0]["implication"]

        assert isinstance(memo["top_risks"], list)
        assert len(memo["top_risks"]) == len(VALID_STRUCTURED_RESPONSE["risks"])
        assert memo["top_risks"][0] == VALID_STRUCTURED_RESPONSE["risks"][0]["risk"]

        # rent_context is the documented placeholder
        assert memo["rent_context"] == "—"


class TestRenderStructuredMemoAsTextSmoke:
    def test_text_renderer_uses_ten_section_headers(self):
        # PR #3: ten section headers — six legacy + four typed advisory
        # sections (property_overview / financial_framing / market_context /
        # competitive_landscape).
        out = render_structured_memo_as_text(VALID_STRUCTURED_RESPONSE, "en")
        for header in (
            "## Headline Recommendation",
            "## Ranking Explanation",
            "## Key Evidence",
            "## Risks",
            "## Comparison",
            "## Bottom Line",
            "## Property Overview",
            "## Financial Framing",
            "## Market Context",
            "## Competitive Landscape",
        ):
            assert header in out


# ── Phase 1 hotfix: evidence polarity routing in the legacy shim ─────

VALID_STRUCTURED_RESPONSE_WITH_POLARITY = {
    "headline_recommendation": "recommend with reservations — rent is attractive but competition is heavy",
    "ranking_explanation": (
        "occupancy_economics contributed 26.0 out of 30 and competition_whitespace "
        "only 4.0 out of 10, netting rank 3 of 12."
    ),
    "key_evidence": [
        {
            "signal": "annual rent",
            "value": "SAR 480,000/yr",
            "implication": "15% below Al Olaya median",
            "polarity": "positive",
        },
        {
            "signal": "competitor density",
            "value": "3 QSR within 400 m",
            "implication": "3 competitors limit market share",
            "polarity": "negative",
        },
        {
            "signal": "street width",
            "value": "12 m",
            "implication": "adequate but not drive-thru capable",
            "polarity": "neutral",
        },
    ],
    "risks": [
        {"risk": "landlord wants 3-year escalator", "mitigation": "negotiate cap at CPI+2%"},
    ],
    "comparison": "Matches Peer A on rent; trails Peer B on competition exposure.",
    "bottom_line": "Worth a site visit but push hard on the escalator.",
}


class TestStructuredToLegacyShapeEvidencePolarity:
    """Phase 1 hotfix: polarity-aware routing of key_evidence into
    top_reasons_to_pursue vs top_risks in _structured_to_legacy_shape."""

    def test_mixed_polarity_routes_positives_and_neutrals_to_reasons_and_negatives_to_risks(self):
        from app.api.expansion_advisor import _structured_to_legacy_shape

        memo = _structured_to_legacy_shape(VALID_STRUCTURED_RESPONSE_WITH_POLARITY)

        positive_impl = "15% below Al Olaya median"
        neutral_impl = "adequate but not drive-thru capable"
        negative_impl = "3 competitors limit market share"
        explicit_risk = "landlord wants 3-year escalator"

        # top_reasons_to_pursue: positive first, then neutral — order matters.
        assert memo["top_reasons_to_pursue"] == [positive_impl, neutral_impl]
        assert memo["top_reasons_to_pursue"].index(positive_impl) < memo["top_reasons_to_pursue"].index(neutral_impl)
        assert negative_impl not in memo["top_reasons_to_pursue"]

        # top_risks: explicit risk first, then negative implication — order matters.
        assert memo["top_risks"] == [explicit_risk, negative_impl]
        assert memo["top_risks"].index(explicit_risk) < memo["top_risks"].index(negative_impl)

    def test_missing_polarity_is_backward_compat_and_treated_as_neutral(self):
        """Cached structured memos generated before this hotfix have no
        polarity field; the shim must route every implication to
        top_reasons_to_pursue (same behavior as before the hotfix) and must
        not raise."""
        from app.api.expansion_advisor import _structured_to_legacy_shape

        legacy_cached_memo = {
            "headline_recommendation": "recommend — below-median rent",
            "ranking_explanation": "occupancy_economics drove the rank.",
            "key_evidence": [
                {"signal": "rent", "value": "SAR 400k/yr", "implication": "12% below median"},
                {"signal": "footfall", "value": "high", "implication": "adjacent to Hyper Panda"},
                {"signal": "frontage", "value": "10 m", "implication": "decent visibility"},
            ],
            "risks": [{"risk": "permit delay", "mitigation": "start early"}],
            "comparison": "Matches peers.",
            "bottom_line": "Pursue.",
        }

        memo = _structured_to_legacy_shape(legacy_cached_memo)

        assert memo["top_reasons_to_pursue"] == [
            "12% below median",
            "adjacent to Hyper Panda",
            "decent visibility",
        ]
        assert memo["top_risks"] == ["permit delay"]

    def test_all_negative_evidence_falls_back_to_ranking_explanation(self):
        from app.api.expansion_advisor import _structured_to_legacy_shape

        ranking_explanation = (
            "access_visibility was the only component above median at 6.8 of 10; "
            "every other component trailed peers, leaving rank 11 of 12 with a "
            "final score that sits two full points below the recommend threshold."
        )
        all_negative_memo = {
            "headline_recommendation": "decline — every signal except frontage is weak",
            "ranking_explanation": ranking_explanation,
            "key_evidence": [
                {"signal": "rent", "value": "SAR 700k/yr", "implication": "22% above median", "polarity": "negative"},
                {"signal": "competition", "value": "5 QSR within 300 m", "implication": "saturated corridor", "polarity": "negative"},
            ],
            "risks": [{"risk": "parking fails municipal minimum", "mitigation": None}],
            "comparison": "Trails all peers.",
            "bottom_line": "Skip.",
        }

        memo = _structured_to_legacy_shape(all_negative_memo)

        # top_reasons_to_pursue falls back to ranking_explanation truncated to 200 chars.
        assert len(memo["top_reasons_to_pursue"]) == 1
        assert memo["top_reasons_to_pursue"][0] == ranking_explanation[:200]
        assert len(memo["top_reasons_to_pursue"][0]) <= 200

        # top_risks contains every negative implication plus the explicit risk.
        assert "parking fails municipal minimum" in memo["top_risks"]
        assert "22% above median" in memo["top_risks"]
        assert "saturated corridor" in memo["top_risks"]
        # Explicit risk comes before negative implications.
        assert memo["top_risks"][0] == "parking fails municipal minimum"

    def test_malformed_evidence_items_are_skipped_not_crashed(self):
        from app.api.expansion_advisor import _structured_to_legacy_shape

        malformed_memo = {
            "headline_recommendation": "recommend — rent edge",
            "ranking_explanation": "occupancy_economics drove the rank.",
            "key_evidence": [
                {"signal": "rent", "value": "SAR 400k/yr", "implication": "below median", "polarity": "positive"},
                "not a dict, should be skipped",
                {"signal": "frontage", "value": "10 m"},  # missing implication, should be skipped
                None,  # not a dict, should be skipped
                {"signal": "footfall", "value": "high", "implication": "busy corridor", "polarity": "neutral"},
            ],
            "risks": [
                {"risk": "permit delay", "mitigation": "start early"},
                "not a dict",
                {"mitigation": "no risk key"},  # missing risk, should be skipped
                None,
            ],
            "comparison": "Matches peers.",
            "bottom_line": "Pursue.",
        }

        memo = _structured_to_legacy_shape(malformed_memo)

        # Only the two well-formed evidence items made it through.
        assert memo["top_reasons_to_pursue"] == ["below median", "busy corridor"]
        # Only the one well-formed risk made it through.
        assert memo["top_risks"] == ["permit delay"]
        # Function returned a valid legacy-shape dict (all six keys present).
        for key in (
            "headline",
            "fit_summary",
            "top_reasons_to_pursue",
            "top_risks",
            "recommended_next_action",
            "rent_context",
        ):
            assert key in memo


# ── Tri-state gate fix (parking_pass=null / Aqar listings) ──────────

# Production-shape fixture mirroring the #1-ranked candidate of search
# 34eda4f9-5704-4645-b408-1cf6a3b8db5e: parking unknown, all other gates
# pass, overall_pass=null, final_rank=1, final_score=80.
PRODUCTION_UNKNOWN_PARKING_CANDIDATE = {
    "id": "aqar-listing-1",
    "parcel_id": "aqar-listing-1",
    "final_rank": 1,
    "final_score": 80,
    "economics_score": 82,
    "cannibalization_score": 40,
    "feature_snapshot_json": {
        "district": "Al Olaya",
        "district_display": "العليا",
        "area_m2": 120,
        "estimated_annual_rent_sar": 480000,
        "district_median_rent": 560000,
    },
    "score_breakdown_json": {
        "occupancy_economics": 82,
        "listing_quality": 75,
        "brand_fit": 78,
        "competition_whitespace": 70,
        "demand_potential": 80,
        "access_visibility": 72,
        "landlord_signal": 60,
        "delivery_demand": 65,
        "confidence": 70,
    },
    "gate_status_json": {
        "zoning_fit_pass": True,
        "area_fit_pass": True,
        "frontage_access_pass": True,
        "parking_pass": None,
        "district_pass": True,
        "cannibalization_pass": True,
        "delivery_market_pass": True,
        "economics_pass": True,
        "overall_pass": None,
    },
    "gate_reasons_json": {
        "passed": [
            "zoning fit", "area fit", "frontage/access",
            "district", "cannibalization", "delivery market", "economics",
        ],
        "failed": [],
        "unknown": ["parking"],
        "thresholds": {},
        "explanations": {
            "parking_pass": "Parking context is not available for Aqar listings — cannot evaluate.",
        },
    },
    "top_risks_json": ["Parking could not be verified from current data."],
    "comparable_competitors_json": [],
}


class TestCoerceGateVerdictsTriState:
    """Tri-state preservation: gate_status_json.parking_pass = null must
    produce verdict='unknown', NOT verdict='fail'."""

    def test_flat_gate_status_null_becomes_unknown(self):
        from app.services.llm_decision_memo import _coerce_gate_verdicts
        raw = {
            "zoning_fit_pass": True,
            "parking_pass": None,
            "area_fit_pass": False,
            "overall_pass": None,
        }
        out = _coerce_gate_verdicts(raw)
        by_name = {e["gate"]: e["verdict"] for e in out}
        assert by_name["parking_pass"] == "unknown"
        assert by_name["zoning_fit_pass"] == "pass"
        assert by_name["area_fit_pass"] == "fail"
        # overall_pass is a roll-up, not a gate — should not appear as a row.
        assert "overall_pass" not in by_name

    def test_bucketed_gate_reasons_is_authoritative(self):
        from app.services.llm_decision_memo import _coerce_gate_verdicts
        raw = {
            "passed": ["zoning fit", "economics"],
            "failed": [],
            "unknown": ["parking"],
            "explanations": {
                "parking_pass": "Parking context is not available for Aqar listings — cannot evaluate.",
            },
        }
        out = _coerce_gate_verdicts(raw)
        by_name = {e["gate"]: e for e in out}
        # Authoritative bucket arrays drive verdicts; no gibberish from
        # iterating top-level keys like "passed"/"failed"/"unknown".
        assert by_name["parking"]["verdict"] == "unknown"
        assert "could not be verified" in by_name["parking"]["reason"] or \
               "not available" in by_name["parking"]["reason"]
        assert by_name["zoning fit"]["verdict"] == "pass"
        assert "passed" not in by_name and "failed" not in by_name \
               and "unknown" not in by_name


class TestBuildMemoContextTriStateAnchors:
    """build_memo_context must plumb tri-state buckets + the deterministic
    anchors (overall_pass, final_rank, final_score, deterministic_verdict)
    into MemoContext."""

    def test_production_candidate_populates_unknown_bucket_and_anchors(self):
        ctx = build_memo_context(
            candidate=PRODUCTION_UNKNOWN_PARKING_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        # Tri-state buckets.
        unknown_names = [e["name"] for e in ctx.gate_buckets["unknown"]]
        assert "parking" in unknown_names
        # Explanation survives plumbing (humanized lookup).
        parking_entry = next(e for e in ctx.gate_buckets["unknown"] if e["name"] == "parking")
        assert "not available" in parking_entry["explanation"] \
               or "cannot evaluate" in parking_entry["explanation"]
        assert ctx.gate_buckets["failed"] == []
        assert len(ctx.gate_buckets["passed"]) >= 6
        # Deterministic anchors.
        assert ctx.overall_pass is None
        assert ctx.final_rank == 1
        assert ctx.final_score == 80
        # final_score=80, economics=82, cannib=40 → "go".
        assert ctx.deterministic_verdict == "go"


class TestRenderPromptUnknownGateAddendum:
    """Production case: parking is unknown, no gate failed. The prompt must
    NOT emit the old 'decline due to failure' instruction, and MUST emit
    the UNKNOWN GATES situational addendum."""

    def test_unknown_gate_addendum_replaces_failure_addendum(self):
        ctx = build_memo_context(
            candidate=PRODUCTION_UNKNOWN_PARKING_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        messages = render_structured_memo_prompt(ctx)
        system_content = messages[0]["content"]

        # Base rule sections present in the v2 humanized prompt.
        assert "HARD RULES" in system_content
        assert "GATE LANGUAGE RULES" in system_content
        # Unknown gates surfaced with the right framing.
        assert "UNKNOWN GATES" in system_content
        assert "parking" in system_content
        assert "could not be verified" in system_content \
               or "not evaluable" in system_content
        # Old 'GATE FAILURE' addendum must NOT appear (nothing failed).
        assert "GATE FAILURE" not in system_content

    def test_user_payload_carries_deterministic_anchors(self):
        ctx = build_memo_context(
            candidate=PRODUCTION_UNKNOWN_PARKING_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        messages = render_structured_memo_prompt(ctx)
        user_payload = json.loads(messages[1]["content"])
        assert user_payload["overall_pass"] is None
        assert user_payload["final_rank"] == 1
        assert user_payload["final_score"] == 80
        assert user_payload["deterministic_verdict"] == "go"
        assert "gates" in user_payload
        assert [e["name"] for e in user_payload["gates"]["unknown"]] == ["parking"]
        assert user_payload["gates"]["failed"] == []


# ── Memo-text assertions: prompt-rule compliance against a mock LLM ──
#
# These tests mock the OpenAI client to return a memo crafted to respect
# the new GATE LANGUAGE / HEADLINE / SELF-CONSISTENCY rules, and assert
# that what the pipeline produces survives _structured_to_legacy_shape and
# our wording rules for the production fixture (parking=unknown, rank 1,
# score 80) and for a genuinely failing candidate (over-correction guard).

_DECLINE_RE = r"decline|reject|not viable|disqualif"
_DECLINE_BOTTOM_RE = r"not viable|decline|should not proceed|disqualif"
_PARKING_BAD_RE = r"\bfail|\bfailing\b|\bfailed\b|inadequate|insufficient parking"
_PARKING_UNKNOWN_RE = r"could not be verified|not evaluable|unavailable|not available"
_CONCERN_LANG_RE = r"concern|caution|risk|weak|decline|not recommend"


# PR #3: minimal stub for the four typed advisory sections so the
# validators added in v5 accept these wording-rule fixtures unchanged.
# The sections are intentionally thin — these tests assert prose
# behavior, not numeric assembly — so summary/thesis are short,
# numeric fields are None, and bullets are empty.
_MINIMAL_ADVISORY_SECTIONS = {
    "property_overview": {
        "summary": "Top-ranked candidate; site quality is acceptable for the format.",
        "area_m2": None,
        "frontage_width_m": None,
        "street_type": None,
        "parking_evidence": None,
        "visibility_score": None,
        "listing_age_days": None,
        "vacancy_status": None,
    },
    "financial_framing": {
        "summary": "Comparable rent context not available for this listing.",
        "thesis": "Comparable rent context not available for this listing — the rent thesis rests on absolute pricing alone.",
        "annual_rent_sar": None,
        "comparable_median_annual_rent_sar": None,
        "rent_percentile_vs_comparables": None,
        "comparable_n": None,
        "comparable_scope": None,
        "spread_to_median_sar": None,
    },
    "market_context": {
        "summary": "Realized demand data not available for this catchment.",
        "demand_thesis": "Realized demand data not available for this catchment.",
        "population_reach": None,
        "district_momentum": None,
        "realized_demand_30d": None,
        "realized_demand_branches": None,
        "delivery_listing_count": None,
    },
    "competitive_landscape": {
        "summary": "No named competitors or peer candidates within the data window for this site.",
        "saturation_thesis": "No named competitors or peer candidates within the data window for this site.",
        "top_chains": [],
        "comparable_competitors": [],
        "next_candidate_summary": None,
    },
}


_PRODUCTION_MEMO_COMPLIANT = {
    "headline_recommendation": "Recommend pursuing — strong economics and top rank with parking noted as unverifiable.",
    "ranking_explanation": (
        "occupancy_economics contributed 24.6 out of 30 and brand_fit 8.6 out of 11, "
        "driving rank 1 with a final_score of 80."
    ),
    "key_evidence": [
        {"signal": "final_score", "value": "80/100",
         "implication": "top-ranked candidate in this search", "polarity": "positive"},
        {"signal": "parking", "value": "unknown",
         "implication": "could not be verified from current data (Aqar listings do not carry parking signal)",
         "polarity": "neutral"},
    ],
    "risks": [
        {"risk": "Parking could not be verified from current data.",
         "mitigation": "Site visit to confirm on-street / building parking."},
    ],
    "comparison": "Comfortably ahead of rank 2 on economics.",
    "bottom_line": "Proceed with a site visit to close the parking data gap.",
    **_MINIMAL_ADVISORY_SECTIONS,
}


_OVER_CORRECTION_CANDIDATE = {
    "id": "weak-cand-1",
    "parcel_id": "weak-1",
    "final_rank": 12,
    "final_score": 45,
    "economics_score": 40,
    "cannibalization_score": 80,
    "feature_snapshot_json": {
        "district": "Edge District",
        "area_m2": 60,
        "estimated_annual_rent_sar": 900000,
        "district_median_rent": 400000,
    },
    "score_breakdown_json": {
        "occupancy_economics": 30,
        "listing_quality": 40,
        "brand_fit": 50,
        "competition_whitespace": 40,
        "demand_potential": 45,
        "access_visibility": 50,
        "landlord_signal": 30,
        "delivery_demand": 40,
        "confidence": 50,
    },
    "gate_status_json": {
        "zoning_fit_pass": True,
        "area_fit_pass": True,
        "economics_pass": False,
        "overall_pass": False,
    },
    "gate_reasons_json": {
        "passed": ["zoning fit", "area fit"],
        "failed": ["economics"],
        "unknown": [],
        "thresholds": {},
        "explanations": {
            "economics_pass": "Economics score below minimum threshold.",
        },
    },
    "comparable_competitors_json": [],
}


_OVER_CORRECTION_MEMO_COMPLIANT = {
    "headline_recommendation": "Decline — economics gate fails and rent sits well above the district median.",
    "ranking_explanation": (
        "occupancy_economics contributed only 9.0 out of 30 and landlord_signal 2.4 out of 8, "
        "driving rank 12 with a final_score of 45."
    ),
    "key_evidence": [
        {"signal": "annual rent", "value": "SAR 900,000/yr",
         "implication": "125% above district median — a clear economics concern",
         "polarity": "negative"},
        {"signal": "economics gate", "value": "failed",
         "implication": "deterministic threshold not met",
         "polarity": "negative"},
    ],
    "risks": [
        {"risk": "Economics gate failure indicates the deal is not viable at current rent.",
         "mitigation": "Renegotiate rent or walk."},
    ],
    "comparison": "Worse than every shortlisted peer on economics.",
    "bottom_line": "Do not proceed without a material rent reduction — current terms are not viable.",
    **_MINIMAL_ADVISORY_SECTIONS,
}


class TestMemoWordingComplianceProductionFixture:
    """Assertions (a), (b), (c) against the production unknown-parking
    fixture: mock the LLM to return a compliant memo and verify that
    headline/bottom_line/parking-language rules are satisfied."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_compliant_memo_passes_wording_rules(self, mock_get_client, monkeypatch):
        monkeypatch.setattr(
            "app.services.llm_decision_memo.settings.EXPANSION_MEMO_STRUCTURED_ENABLED",
            True,
            raising=False,
        )
        _daily_cost_tracker.clear()

        mock_get_client.return_value = _mock_client_returning(
            json.dumps(_PRODUCTION_MEMO_COMPLIANT)
        )
        ctx = build_memo_context(
            candidate=PRODUCTION_UNKNOWN_PARKING_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        memo = generate_structured_memo(ctx)
        assert memo is not None

        import re
        # (a) headline does NOT decline / reject / disqualify.
        assert not re.search(_DECLINE_RE, memo["headline_recommendation"], re.I)
        # (b) bottom_line does NOT contain decline / not viable / disqualify.
        assert not re.search(_DECLINE_BOTTOM_RE, memo["bottom_line"], re.I)
        # (c) Any parking sentence uses unknown-language, not failure-language.
        parking_text = " ".join(
            s for s in [
                memo["headline_recommendation"],
                memo["ranking_explanation"],
                memo["bottom_line"],
                memo["comparison"],
            ] + [e.get("implication", "") for e in memo["key_evidence"]]
              + [r.get("risk", "") for r in memo["risks"]]
            if s and "parking" in s.lower()
        )
        if parking_text:
            assert re.search(_PARKING_UNKNOWN_RE, parking_text, re.I)
            assert not re.search(_PARKING_BAD_RE, parking_text, re.I)


class TestMemoWordingOverCorrectionFixture:
    """Guards against the prompt becoming toothless when a gate genuinely
    fails — decline / concern language IS permitted for overall_pass=false."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_failing_candidate_can_use_decline_language(self, mock_get_client, monkeypatch):
        monkeypatch.setattr(
            "app.services.llm_decision_memo.settings.EXPANSION_MEMO_STRUCTURED_ENABLED",
            True,
            raising=False,
        )
        _daily_cost_tracker.clear()

        mock_get_client.return_value = _mock_client_returning(
            json.dumps(_OVER_CORRECTION_MEMO_COMPLIANT)
        )
        ctx = build_memo_context(
            candidate=_OVER_CORRECTION_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        memo = generate_structured_memo(ctx)
        assert memo is not None

        import re
        # Concern / decline language IS permitted somewhere in the memo.
        joined = " ".join([
            memo["headline_recommendation"],
            memo["ranking_explanation"],
            memo["bottom_line"],
            memo["comparison"],
        ])
        assert re.search(_CONCERN_LANG_RE, joined, re.I)

        # Anchors reflect the failing case.
        assert ctx.overall_pass is False
        assert ctx.deterministic_verdict == "caution"


# ---------------------------------------------------------------------------
# Phase 4 — memo / rerank whitelist split.
#
# The whitelist used to be a single tuple imported by both the memo and
# the rerank LLM call paths. Phase 4 widens the memo's narrative surface
# with `listing_age` + `district_momentum` while keeping the rerank
# signal surface constant. These tests pin the split so a future edit
# cannot quietly hand rerank access to Phase 3a/3b dict keys (which
# would double-count signals already folded into _listing_quality_score).
# ---------------------------------------------------------------------------


def test_memo_whitelist_includes_listing_age_and_district_momentum():
    from app.services.llm_decision_memo import _MEMO_WHITELIST

    assert "listing_age" in _MEMO_WHITELIST
    assert "district_momentum" in _MEMO_WHITELIST


def test_rerank_whitelist_excludes_memo_only_keys():
    from app.services.llm_decision_memo import _RERANK_WHITELIST

    assert "listing_age" not in _RERANK_WHITELIST
    assert "district_momentum" not in _RERANK_WHITELIST


def test_memo_whitelist_is_superset_of_rerank_whitelist():
    """The memo whitelist is the rerank whitelist plus the two Phase 4
    keys and the PR #1 advisory-grade comparable-rent keys. Any
    addition to the rerank set must also appear in the memo set by
    construction, otherwise a signal the rerank LLM uses would not be
    visible to the memo LLM."""
    from app.services.llm_decision_memo import _MEMO_WHITELIST, _RERANK_WHITELIST

    assert set(_RERANK_WHITELIST).issubset(set(_MEMO_WHITELIST))
    assert set(_MEMO_WHITELIST) - set(_RERANK_WHITELIST) == {
        "listing_age",
        "district_momentum",
        "comparable_median_annual_rent_sar",
        "comparable_n",
        "comparable_source_label",
        "demand_generator_index",
        "demand_score_source",
    }


def test_memo_whitelist_includes_comparable_rent_keys():
    """PR #1 plumbed comparable-rent context into feature_snapshot_json.
    Pin the whitelist so the memo LLM can read these new keys."""
    from app.services.llm_decision_memo import _MEMO_WHITELIST

    assert "comparable_median_annual_rent_sar" in _MEMO_WHITELIST
    assert "comparable_n" in _MEMO_WHITELIST
    assert "comparable_source_label" in _MEMO_WHITELIST


def test_memo_whitelist_includes_demand_engine_keys():
    """PR-E — the demand-generator evidence must survive the 4,000-char
    snapshot truncation so dg_index memos can cite the engine that scored
    Demand Strength. Memo-only: rerank's signal surface stays constant."""
    from app.services.llm_decision_memo import _MEMO_WHITELIST, _RERANK_WHITELIST

    assert "demand_generator_index" in _MEMO_WHITELIST
    assert "demand_score_source" in _MEMO_WHITELIST
    assert "demand_generator_index" not in _RERANK_WHITELIST
    assert "demand_score_source" not in _RERANK_WHITELIST


# ---------------------------------------------------------------------------
# PR-E — engine-aware demand evidence: payload plumbing + prompt rules.
# ---------------------------------------------------------------------------


_DG_INDEX_SNAPSHOT_FIELDS = {
    "demand_score_source": "dg_index",
    "demand_generator_index": {
        "composite_0_100": 74.2,
        "weights_version": "dg-v1",
        "radius_m": 3500,
        "population_reach": 248000,
        "pop_radius_m": 1500,
        "population_local_reach": 41250,
        "osm_generators": {
            "offices": 120,
            "malls_retail": 14,
            "transit": 9,
            "mosques": 22,
            "schools": 17,
            "hospitals": 3,
            "hotels": 6,
        },
        "building_floors_proxy_sum": 18432.5,
        "fnb_review_weighted_density": 18400.0,
        "fnb_venue_count": 86,
        "subscores": {
            "population": 61.0,
            "osm_generators": 70.5,
            "building_floors": 66.2,
            "fnb_review_weighted": 81.3,
        },
    },
}


class TestSerializePayloadDemandEngine:
    """PR-E: dg_index evidence reaches the memo LLM — including through
    the whitelist-truncation path — and absent fields stay absent."""

    def _user_content(self, snapshot_extra: dict) -> str:
        cand = dict(BASE_STRUCTURED_CANDIDATE)
        cand["feature_snapshot_json"] = {
            **BASE_STRUCTURED_CANDIDATE["feature_snapshot_json"],
            **snapshot_extra,
        }
        ctx = build_memo_context(candidate=cand, brief=BASE_STRUCTURED_BRIEF, lang="en")
        messages = render_structured_memo_prompt(ctx)
        return messages[1]["content"]

    def test_payload_includes_dg_fields_when_present(self):
        user_content = self._user_content(dict(_DG_INDEX_SNAPSHOT_FIELDS))
        assert '"demand_score_source": "dg_index"' in user_content
        assert '"composite_0_100": 74.2' in user_content
        assert '"fnb_review_weighted_density": 18400.0' in user_content
        assert '"building_floors_proxy_sum": 18432.5' in user_content
        assert '"population_local_reach": 41250' in user_content

    def test_truncation_path_retains_dg_fields(self):
        # Oversize the snapshot past _FEATURE_SNAPSHOT_SOFT_LIMIT so the
        # whitelist filter fires; the dg evidence must survive it while the
        # junk key is dropped.
        from app.services.llm_decision_memo import _FEATURE_SNAPSHOT_SOFT_LIMIT

        junk = {"non_whitelisted_blob": "x" * (_FEATURE_SNAPSHOT_SOFT_LIMIT + 100)}
        user_content = self._user_content({**_DG_INDEX_SNAPSHOT_FIELDS, **junk})
        assert "non_whitelisted_blob" not in user_content
        assert '"demand_score_source": "dg_index"' in user_content
        assert '"composite_0_100": 74.2' in user_content
        assert '"fnb_review_weighted_density": 18400.0' in user_content

    def test_absent_dg_fields_leave_payload_unchanged(self):
        user_content = self._user_content({})
        assert "demand_score_source" not in user_content
        assert "demand_generator_index" not in user_content


class TestDemandEnginePromptRules:
    """PR-E: string-pins on the system prompt for the engine conditional,
    the rent-positioning phrase mandate, and the AR glossary entries."""

    def test_en_prompt_carries_engine_conditional(self):
        from app.services.llm_decision_memo import _compose_structured_system_prompt

        en = _compose_structured_system_prompt("en")
        assert "feature_snapshot.demand_score_source" in en
        assert '"dg_index"' in en
        assert "demand-generator composite" in en
        assert "demand_generator_index.fnb_review_weighted_density" in en
        assert "demand_generator_index.osm_generators" in en
        assert "demand_generator_index.building_floors_proxy_sum" in en
        assert "demand_generator_index.population_local_reach" in en
        # population reach is the anchor ONLY for pop_score / legacy rows.
        assert '"pop_score" or the field is absent (legacy rows)' in en
        # Example C caveat: dg_index memos must not anchor demand on
        # population reach.
        assert "MUST NOT be the demand anchor" in en
        # Thin-market fallback is engine-dependent.
        assert "What it leans on next is engine-dependent" in en

    def test_en_prompt_mandates_phrase_en_verbatim_copy(self):
        from app.services.llm_decision_memo import _compose_structured_system_prompt

        en = _compose_structured_system_prompt("en")
        assert "{zone, pct_value, scope, phrase_en, phrase_ar}" in en
        assert "COPY phrase_en" in en
        assert "percentile RANK" in en
        # The anti-anchoring worked inversion from the production defect.
        assert "A listing at rank 0.09 is cheaper than about 91%" in en
        assert "comparables, not 9%" in en

    def test_ar_prompt_carries_dg_glossary_and_units(self):
        from app.services.llm_decision_memo import _compose_structured_system_prompt

        ar = _compose_structured_system_prompt("ar")
        # Rule 7 glossary — fixed Arabic terms (PR-D card vocabulary).
        for term in [
            "مركب مولدات الطلب",
            "كتلة تقييمات المطاعم والمقاهي",
            "مولدات الرحلات",
            "الكثافة العمرانية",
        ]:
            assert term in ar, f"AR dg glossary missing: {term}"
        # Rule 8 unit tokens for the new evidence values.
        assert "تقييماً موزوناً" in ar
        assert "مولد رحلات" in ar
        assert "طابقاً (مؤشر تقريبي)" in ar

    def test_ar_prompt_mandates_phrase_ar_verbatim_copy(self):
        from app.services.llm_decision_memo import _compose_structured_system_prompt

        ar = _compose_structured_system_prompt("ar")
        assert "rent_positioning.phrase_ar" in ar
        assert "انسخ العبارة الجاهزة" in ar
        # The worked anti-inversion example: rank 0.09 → cheaper than ~91%.
        assert "أرخص من حوالي 91% من المقارنات" in ar

    def test_ar_dg_glossary_absent_from_en_prompt(self):
        from app.services.llm_decision_memo import _compose_structured_system_prompt

        en = _compose_structured_system_prompt("en")
        for term in [
            "مركب مولدات الطلب",
            "كتلة تقييمات المطاعم والمقاهي",
            "مولدات الرحلات",
            "الكثافة العمرانية",
        ]:
            assert term not in en, f"AR dg glossary leaked into EN prompt: {term}"


def test_feature_snapshot_whitelist_alias_points_at_memo_whitelist():
    """Back-compat alias for existing memo call sites. A future edit
    that shifts the alias target (e.g. to _RERANK_WHITELIST) would
    silently strip listing_age / district_momentum from the memo
    fallback path at _serialize_context_for_user_message — pin it."""
    from app.services.llm_decision_memo import (
        _FEATURE_SNAPSHOT_WHITELIST,
        _MEMO_WHITELIST,
    )

    assert _FEATURE_SNAPSHOT_WHITELIST is _MEMO_WHITELIST


# ---------------------------------------------------------------------------
# Headline-validity post-validate-and-retry layer (Bugs 1, 2, 3 fix).
# ---------------------------------------------------------------------------

# Imported lazily inside tests where they're used so the module stays
# importable even if these helpers are renamed in the future. These
# fixtures intentionally piggyback on the existing _MINIMAL_ADVISORY_SECTIONS
# block so the shape passes _advisory_section_invalid_reason.

_HEADLINE_RETRY_BASE_BODY = {
    "ranking_explanation": (
        "occupancy_economics contributed 24.6 out of 30 and brand_fit 8.6 out of 11, "
        "driving rank 1 with a final_score of 80."
    ),
    "key_evidence": [
        {"signal": "final_score", "value": "80/100",
         "implication": "top-ranked candidate in this search", "polarity": "positive"},
    ],
    "risks": [
        {"risk": "Market growth signal weak.", "mitigation": "Monitor district momentum quarterly."},
    ],
    "comparison": "Comfortably ahead of rank 2 on economics.",
    "bottom_line": "Proceed with a site visit to confirm on-the-ground conditions.",
    **_MINIMAL_ADVISORY_SECTIONS,
}


def _memo_with_headline(headline: str) -> dict:
    return {"headline_recommendation": headline, **_HEADLINE_RETRY_BASE_BODY}


_RANK1_ADVISORY_FAILURE_CANDIDATE = {
    "id": "advisory-rank1",
    "parcel_id": "advisory-rank1",
    "final_rank": 1,
    "final_score": 80.0,
    "economics_score": 75,
    "cannibalization_score": 40,
    "feature_snapshot_json": {
        "district": "Al Olaya",
        "area_m2": 120,
        "estimated_annual_rent_sar": 480000,
        "district_median_rent": 560000,
    },
    "score_breakdown_json": {
        "occupancy_economics": 80,
        "listing_quality": 70,
        "brand_fit": 78,
        "competition_whitespace": 65,
        "demand_potential": 80,
        "access_visibility": 72,
        "landlord_signal": 60,
        "delivery_demand": 65,
        "confidence": 70,
    },
    "gate_status_json": {
        "zoning_fit_pass": True,
        "area_fit_pass": True,
        "frontage_access_pass": True,
        "parking_pass": True,
        "district_pass": True,
        "cannibalization_pass": True,
        "delivery_market_pass": True,
        "economics_pass": True,
        "radiance_growth_pass": False,
        "overall_pass": True,
    },
    "gate_reasons_json": {
        # Humanized labels — production wire shape after
        # ``_normalize_gate_reasons`` → ``_humanize_gate_list``.
        "passed": [
            "zoning fit", "area fit", "frontage/access",
            "parking", "district", "cannibalization",
            "delivery market", "economics",
        ],
        "failed": ["Market growth signal"],
        "unknown": [],
        "blocking_failures": [],
        "advisory_failures": ["radiance_growth_pass"],
        "thresholds": {},
        "explanations": {
            "radiance_growth_pass": "Advisory market-growth signal — does not block.",
        },
    },
    "comparable_competitors_json": [],
}


_RANK1_ALL_PASS_CANDIDATE = {
    "id": "rank1-all-pass",
    "parcel_id": "rank1-all-pass",
    "final_rank": 1,
    "final_score": 80.0,
    "economics_score": 75,
    "cannibalization_score": 40,
    "feature_snapshot_json": {
        "district": "Al Olaya",
        "area_m2": 120,
        "estimated_annual_rent_sar": 480000,
        "district_median_rent": 560000,
    },
    "score_breakdown_json": _RANK1_ADVISORY_FAILURE_CANDIDATE["score_breakdown_json"],
    "gate_status_json": {
        "zoning_fit_pass": True,
        "area_fit_pass": True,
        "frontage_access_pass": True,
        "parking_pass": True,
        "district_pass": True,
        "cannibalization_pass": True,
        "delivery_market_pass": True,
        "economics_pass": True,
        "overall_pass": True,
    },
    "gate_reasons_json": {
        # Humanized labels — production wire shape after
        # ``_normalize_gate_reasons`` → ``_humanize_gate_list``.
        "passed": [
            "zoning fit", "area fit", "frontage/access",
            "parking", "district", "cannibalization",
            "delivery market", "economics",
        ],
        "failed": [],
        "unknown": [],
        "blocking_failures": [],
        "advisory_failures": [],
        "thresholds": {},
        "explanations": {},
    },
    "comparable_competitors_json": [],
}


_CONSIDER_BAND_CANDIDATE = {
    "id": "consider-band",
    "parcel_id": "consider-band",
    "final_rank": 4,
    "final_score": 65.0,
    "economics_score": 50,
    "cannibalization_score": 60,
    "feature_snapshot_json": {
        "district": "Al Naseem",
        "area_m2": 110,
        "estimated_annual_rent_sar": 420000,
        "district_median_rent": 420000,
    },
    "score_breakdown_json": {
        "occupancy_economics": 60,
        "listing_quality": 55,
        "brand_fit": 60,
        "competition_whitespace": 50,
        "demand_potential": 65,
        "access_visibility": 60,
        "landlord_signal": 50,
        "delivery_demand": 50,
        "confidence": 60,
    },
    "gate_status_json": {
        "zoning_fit_pass": True,
        "area_fit_pass": True,
        "economics_pass": True,
        "overall_pass": True,
    },
    "gate_reasons_json": {
        "passed": ["zoning_fit_pass", "area_fit_pass", "economics_pass"],
        "failed": [],
        "unknown": [],
        "blocking_failures": [],
        "advisory_failures": [],
        "thresholds": {},
        "explanations": {},
    },
    "comparable_competitors_json": [],
}


_BLOCKING_FAILURE_CANDIDATE = {
    "id": "blocking-fail",
    "parcel_id": "blocking-fail",
    "final_rank": 8,
    "final_score": 50.0,
    "economics_score": 35,
    "cannibalization_score": 70,
    "feature_snapshot_json": {
        "district": "Edge District",
        "area_m2": 60,
        "estimated_annual_rent_sar": 900000,
        "district_median_rent": 400000,
    },
    "score_breakdown_json": {
        "occupancy_economics": 30,
        "listing_quality": 40,
        "brand_fit": 50,
        "competition_whitespace": 40,
        "demand_potential": 45,
        "access_visibility": 50,
        "landlord_signal": 30,
        "delivery_demand": 40,
        "confidence": 50,
    },
    "gate_status_json": {
        "zoning_fit_pass": False,
        "area_fit_pass": True,
        "economics_pass": False,
        "overall_pass": False,
    },
    "gate_reasons_json": {
        # Humanized labels — production wire shape after
        # ``_normalize_gate_reasons`` → ``_humanize_gate_list``.
        "passed": ["area fit"],
        "failed": ["zoning fit", "economics"],
        "unknown": [],
        "blocking_failures": ["zoning_fit_pass"],
        "advisory_failures": ["economics_pass"],
        "thresholds": {},
        "explanations": {
            "zoning_fit_pass": "Zoning class disallows F&B.",
            "economics_pass": "Economics score below minimum threshold.",
        },
    },
    "comparable_competitors_json": [],
}


def _enable_structured_memo(monkeypatch):
    monkeypatch.setattr(
        "app.services.llm_decision_memo.settings.EXPANSION_MEMO_STRUCTURED_ENABLED",
        True,
        raising=False,
    )
    _daily_cost_tracker.clear()


def _two_response_client(first_content, second_content):
    """Mock client whose first .create() returns ``first_content`` and
    whose second returns ``second_content``. Both are dicts (will be
    JSON-encoded by _make_mock_response)."""
    client = MagicMock()
    client.chat.completions.create.side_effect = [
        _make_mock_response(first_content),
        _make_mock_response(second_content),
    ]
    return client


class TestHeadlineRetryAdvisoryFailureNotDecline:
    """Bug 1 — advisory-only gate failure must not produce a Decline.

    LLM returns "Decline ..." on both attempts; the post-validate layer
    rewrites the headline locally so the user never sees a contradicting
    recommendation."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_decline_on_advisory_failure_is_rewritten(self, mock_get_client, monkeypatch, caplog):
        _enable_structured_memo(monkeypatch)
        bad_first = _memo_with_headline("Decline — market growth signal fails")
        bad_second = _memo_with_headline("Decline — market growth gate did not pass")
        mock_get_client.return_value = _two_response_client(bad_first, bad_second)

        ctx = build_memo_context(
            candidate=_RANK1_ADVISORY_FAILURE_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        with caplog.at_level("WARNING"):
            memo = generate_structured_memo(ctx)

        assert memo is not None
        # Final headline starts with "Recommend" (rank-1, score >= 70, no
        # blocking failures → guaranteed Recommend).
        assert memo["headline_recommendation"].lower().startswith("recommend")
        assert not memo["headline_recommendation"].lower().startswith("recommend with reservations")
        # The retry was attempted (client called twice).
        assert mock_get_client.return_value.chat.completions.create.call_count == 2


class TestHeadlineRetryRank1ConfabulatedGateFailure:
    """Bug 3 — rank-1 with empty failed_gates must not Decline citing
    fabricated failures (e.g., "decline due to failed parking access")."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_confabulated_decline_is_rewritten(self, mock_get_client, monkeypatch, caplog):
        _enable_structured_memo(monkeypatch)
        bad_first = _memo_with_headline("Decline due to failed parking access")
        bad_second = _memo_with_headline("Decline — parking fails on-site")
        mock_get_client.return_value = _two_response_client(bad_first, bad_second)

        ctx = build_memo_context(
            candidate=_RANK1_ALL_PASS_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        with caplog.at_level("WARNING"):
            memo = generate_structured_memo(ctx)

        assert memo is not None
        assert memo["headline_recommendation"].lower().startswith("recommend")
        assert not memo["headline_recommendation"].lower().startswith("recommend with reservations")
        assert mock_get_client.return_value.chat.completions.create.call_count == 2


class TestHeadlineRetryConsiderPrefixBanned:
    """Bug 2 — headlines starting with "consider " violate the format rule
    and must be rewritten to start with one of the three allowed prefixes."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_consider_prefix_is_rewritten(self, mock_get_client, monkeypatch, caplog):
        _enable_structured_memo(monkeypatch)
        bad_first = _memo_with_headline(
            "consider due to moderate competition and acceptable economics"
        )
        bad_second = _memo_with_headline(
            "Consider — moderate competition and acceptable economics"
        )
        mock_get_client.return_value = _two_response_client(bad_first, bad_second)

        ctx = build_memo_context(
            candidate=_CONSIDER_BAND_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        with caplog.at_level("WARNING"):
            memo = generate_structured_memo(ctx)

        assert memo is not None
        headline_lower = memo["headline_recommendation"].lower()
        # Not rank-1 high-score, not overall_pass=False — falls through to
        # the soft-yes safety-net rewrite.
        assert headline_lower.startswith("recommend with reservations")
        assert mock_get_client.return_value.chat.completions.create.call_count == 2


class TestHeadlineLocalRewriteNullsBody:
    """Bug-fix invariant — when the local-rewrite safety net fires
    (both LLM attempts violate the format rules), the body fields
    must be cleared so they cannot contradict the rewritten headline."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_local_rewrite_empties_body_fields(self, mock_get_client, monkeypatch):
        _enable_structured_memo(monkeypatch)
        bad_first = _memo_with_headline("Decline — market growth signal fails")
        bad_second = _memo_with_headline("Decline — market growth gate did not pass")
        mock_get_client.return_value = _two_response_client(bad_first, bad_second)

        ctx = build_memo_context(
            candidate=_RANK1_ADVISORY_FAILURE_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        assert memo["headline_recommendation"].lower().startswith("recommend")
        # Body fields must be cleared so they cannot contradict the
        # rewritten headline.
        assert memo["ranking_explanation"] == ""
        assert memo["key_evidence"] == []
        assert memo["risks"] == []
        assert memo["comparison"] == ""
        assert memo["bottom_line"] == ""

    @patch("app.services.llm_decision_memo._get_client")
    def test_happy_path_retry_keeps_body(self, mock_get_client, monkeypatch):
        """Control: when the retry succeeds, the body is preserved
        from the retry response (not nulled). Null-out only happens
        on the local-rewrite path."""
        _enable_structured_memo(monkeypatch)
        bad_first = _memo_with_headline("consider due to moderate signals")
        good_second = _memo_with_headline(
            "Recommend — strong economics in a stable district."
        )
        mock_get_client.return_value = _two_response_client(bad_first, good_second)

        ctx = build_memo_context(
            candidate=_RANK1_ALL_PASS_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        assert memo["headline_recommendation"].lower().startswith("recommend")
        # Retry succeeded → body from retry is preserved.
        assert memo["ranking_explanation"] != ""
        assert memo["key_evidence"]


class TestHeadlineNoRetryWhenOverallPassFalseDecline:
    """A genuine "Decline" headline on an overall_pass=False candidate
    passes the validity check on the first try; no retry occurs."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_valid_decline_passes_through_no_retry(self, mock_get_client, monkeypatch):
        _enable_structured_memo(monkeypatch)
        good = _memo_with_headline(
            "Decline — zoning gate fails for this listing's land-use class."
        )
        client = MagicMock()
        client.chat.completions.create.return_value = _make_mock_response(good)
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=_BLOCKING_FAILURE_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        assert memo["headline_recommendation"].lower().startswith("decline")
        assert client.chat.completions.create.call_count == 1


class TestHeadlineNoRetryWhenAllGatesPassRecommend:
    """A valid "Recommend" headline on a clean candidate passes through
    unchanged with a single LLM call."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_valid_recommend_passes_through_no_retry(self, mock_get_client, monkeypatch):
        _enable_structured_memo(monkeypatch)
        good = _memo_with_headline(
            "Recommend — strong economics in a stable district with manageable competition."
        )
        client = MagicMock()
        client.chat.completions.create.return_value = _make_mock_response(good)
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=_RANK1_ALL_PASS_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        assert memo["headline_recommendation"].lower().startswith("recommend")
        assert client.chat.completions.create.call_count == 1


# ---------------------------------------------------------------------------
# v12.1 (PR-E2) — dg-index composite evidence enforcement: detector,
# corrective retry, and deterministic injection fallback.
# ---------------------------------------------------------------------------

_DG_SIGNAL_EN = "demand-generator composite"
_DG_SIGNAL_AR = "مركب مولدات الطلب"

# _DG_INDEX_SNAPSHOT_FIELDS (above) carries composite_0_100 = 74.2 → 74.
_DG_RANK1_CANDIDATE = {
    **_RANK1_ALL_PASS_CANDIDATE,
    "id": "dg-rank1",
    "parcel_id": "dg-rank1",
    "feature_snapshot_json": {
        **_RANK1_ALL_PASS_CANDIDATE["feature_snapshot_json"],
        **_DG_INDEX_SNAPSHOT_FIELDS,
    },
}

_POP_SCORE_RANK1_CANDIDATE = {
    **_RANK1_ALL_PASS_CANDIDATE,
    "id": "pop-rank1",
    "parcel_id": "pop-rank1",
    "feature_snapshot_json": {
        **_RANK1_ALL_PASS_CANDIDATE["feature_snapshot_json"],
        "demand_score_source": "pop_score",
    },
}

# dg_index source but no demand_generator_index block (defensive case —
# both enforcement layers must skip).
_DG_SOURCE_NO_BLOCK_CANDIDATE = {
    **_RANK1_ALL_PASS_CANDIDATE,
    "id": "dg-no-block",
    "parcel_id": "dg-no-block",
    "feature_snapshot_json": {
        **_RANK1_ALL_PASS_CANDIDATE["feature_snapshot_json"],
        "demand_score_source": "dg_index",
    },
}


# Composite rounds to 60 — the production collision shape: a non-generator
# score (e.g. dine_in demand_score) can also render "60/100".
_DG_COMPOSITE_60_CANDIDATE = {
    **_RANK1_ALL_PASS_CANDIDATE,
    "id": "dg-comp60",
    "parcel_id": "dg-comp60",
    "feature_snapshot_json": {
        **_RANK1_ALL_PASS_CANDIDATE["feature_snapshot_json"],
        **_DG_INDEX_SNAPSHOT_FIELDS,
        "demand_generator_index": {
            **_DG_INDEX_SNAPSHOT_FIELDS["demand_generator_index"],
            "composite_0_100": 59.82,
        },
    },
}


def _memo_with_dg_row(headline: str, *, signal: str = _DG_SIGNAL_EN,
                      value: str = "74/100") -> dict:
    """A headline-valid memo whose key_evidence includes a composite row."""
    memo = _memo_with_headline(headline)
    memo["key_evidence"] = list(memo["key_evidence"]) + [
        {"signal": signal, "value": value,
         "implication": "venue activity and trip generators carry the catchment",
         "polarity": "positive"},
    ]
    return memo


class TestDgRequiredComposite:
    """Layer gating — the mandate applies only to dg_index candidates
    carrying a numeric composite."""

    def test_dg_index_with_block_returns_rounded_composite(self):
        from app.services.llm_decision_memo import _dg_required_composite

        assert _dg_required_composite(dict(_DG_INDEX_SNAPSHOT_FIELDS)) == 74

    def test_pop_score_returns_none(self):
        from app.services.llm_decision_memo import _dg_required_composite

        snap = {**_DG_INDEX_SNAPSHOT_FIELDS, "demand_score_source": "pop_score"}
        assert _dg_required_composite(snap) is None

    def test_absent_source_returns_none(self):
        from app.services.llm_decision_memo import _dg_required_composite

        snap = {"demand_generator_index": {"composite_0_100": 74.2}}
        assert _dg_required_composite(snap) is None

    def test_dg_index_without_block_returns_none(self):
        from app.services.llm_decision_memo import _dg_required_composite

        assert _dg_required_composite({"demand_score_source": "dg_index"}) is None

    def test_non_numeric_composite_returns_none(self):
        from app.services.llm_decision_memo import _dg_required_composite

        snap = {
            "demand_score_source": "dg_index",
            "demand_generator_index": {"composite_0_100": "74"},
        }
        assert _dg_required_composite(snap) is None


class TestDgEvidenceDetector:
    """Detector — a memo is compliant when any key_evidence row's
    signal/value mentions the EN phrase, the AR Rule-7 term, or a /100
    value equal to the rounded composite."""

    def test_compliant_by_en_signal_phrase(self):
        from app.services.llm_decision_memo import _dg_evidence_invalid_reason

        memo = _memo_with_dg_row("Recommend — x", value="74.2/100")
        assert _dg_evidence_invalid_reason(memo, 74) is None

    def test_compliant_by_ar_signal_term(self):
        from app.services.llm_decision_memo import _dg_evidence_invalid_reason

        memo = _memo_with_dg_row("نوصي — x", signal=_DG_SIGNAL_AR, value="74/100")
        assert _dg_evidence_invalid_reason(memo, 74) is None

    def test_value_match_requires_generator_attribution_en(self):
        from app.services.llm_decision_memo import _dg_evidence_invalid_reason

        # Value-only match WITH generator attribution (EN signal containing
        # "generator") ⇒ compliant.
        memo = _memo_with_dg_row("Recommend — x",
                                 signal="demand generator composite",
                                 value="74/100")
        assert _dg_evidence_invalid_reason(memo, 74) is None

    def test_value_match_requires_generator_attribution_ar(self):
        from app.services.llm_decision_memo import _dg_evidence_invalid_reason

        # Value-only match WITH generator attribution (AR signal containing
        # "مولدات") ⇒ compliant.
        memo = _memo_with_dg_row("نوصي — x", signal="مولدات الطلب",
                                 value="74/100")
        assert _dg_evidence_invalid_reason(memo, 74) is None

    def test_value_match_without_generator_attribution_is_invalid(self):
        from app.services.llm_decision_memo import _dg_evidence_invalid_reason

        # Bare "74/100" with no generator attribution must NOT satisfy the
        # mandate — it collides with composite-dominated scores.
        memo = _memo_with_dg_row("Recommend — x", signal="demand composite",
                                 value="74/100")
        assert _dg_evidence_invalid_reason(memo, 74) is not None

    def test_non_compliant_returns_reason(self):
        from app.services.llm_decision_memo import _dg_evidence_invalid_reason

        memo = _memo_with_headline("Recommend — x")
        reason = _dg_evidence_invalid_reason(memo, 74)
        assert reason is not None
        assert "demand-generator composite" in reason

    def test_other_score_value_does_not_match(self):
        from app.services.llm_decision_memo import _dg_evidence_invalid_reason

        # The base body carries a "80/100" final_score row — it must not
        # satisfy a composite of 74 (unchanged) AND, now that value-only
        # matches require generator attribution, it must not satisfy a
        # composite of 80 either: the row's signal is "final_score", which
        # carries no generator attribution. Flips the v12.1 false-accept.
        memo = _memo_with_headline("Recommend — x")
        assert _dg_evidence_invalid_reason(memo, 74) is not None
        assert _dg_evidence_invalid_reason(memo, 80) is not None

    def test_production_collision_demand_strength_row_is_invalid(self):
        from app.services.llm_decision_memo import _dg_evidence_invalid_reason

        # Regression from production (parcel-6706340 shape): composite 60
        # with a "Demand Strength"/"60/100" row — coincidental collision,
        # no generator attribution ⇒ invalid.
        memo = _memo_with_headline("Recommend — x")
        memo["key_evidence"] = list(memo["key_evidence"]) + [
            {"signal": "Demand Strength", "value": "60/100",
             "implication": "demand looks adequate", "polarity": "positive"},
        ]
        assert _dg_evidence_invalid_reason(memo, 60) is not None


class TestDgEvidenceRetry:
    """Layer 1 — missing composite row triggers the existing one-retry
    corrective loop with the dg preamble."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_missing_row_triggers_retry_and_keeps_llm_row(
        self, mock_get_client, monkeypatch
    ):
        _enable_structured_memo(monkeypatch)
        first = _memo_with_headline("Recommend — strong site, demand from reach")
        second = _memo_with_dg_row("Recommend — strong site, evidenced demand")
        client = _two_response_client(first, second)
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=_DG_RANK1_CANDIDATE, brief=BASE_STRUCTURED_BRIEF, lang="en"
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        assert client.chat.completions.create.call_count == 2
        rows = memo["key_evidence"]
        assert any(_DG_SIGNAL_EN in str(r.get("signal", "")) for r in rows)
        # LLM-authored row, not the deterministic injection.
        assert all(r.get("source") != "deterministic_injection" for r in rows)

    @patch("app.services.llm_decision_memo._get_client")
    def test_retry_preamble_carries_mandate_and_exact_row(
        self, mock_get_client, monkeypatch
    ):
        _enable_structured_memo(monkeypatch)
        first = _memo_with_headline("Recommend — strong site")
        second = _memo_with_dg_row("Recommend — strong site")
        client = _two_response_client(first, second)
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=_DG_RANK1_CANDIDATE, brief=BASE_STRUCTURED_BRIEF, lang="en"
        )
        generate_structured_memo(ctx)

        retry_call = client.chat.completions.create.call_args_list[1]
        retry_messages = retry_call.kwargs.get("messages") or retry_call[1]["messages"]
        preamble = retry_messages[1]["content"]
        assert retry_messages[1]["role"] == "user"
        assert "PREVIOUS RESPONSE WAS REJECTED" in preamble
        assert '"dg_index"' in preamble
        # The exact required row with THIS candidate's composite filled in.
        assert f'"signal": "{_DG_SIGNAL_EN}"' in preamble
        assert '"value": "74/100"' in preamble
        # Body prose must not anchor demand on population reach.
        assert "MUST NOT be presented as the demand anchor" in preamble

    @patch("app.services.llm_decision_memo._get_client")
    def test_pop_score_candidate_never_triggers(self, mock_get_client, monkeypatch):
        _enable_structured_memo(monkeypatch)
        good = _memo_with_headline("Recommend — strong economics, no dg row")
        client = _mock_client_returning(good)
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=_POP_SCORE_RANK1_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        assert client.chat.completions.create.call_count == 1

    @patch("app.services.llm_decision_memo._get_client")
    def test_dg_source_without_block_skips_both_layers(
        self, mock_get_client, monkeypatch
    ):
        _enable_structured_memo(monkeypatch)
        good = _memo_with_headline("Recommend — strong economics, no dg row")
        client = _mock_client_returning(good)
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=_DG_SOURCE_NO_BLOCK_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        assert client.chat.completions.create.call_count == 1
        assert all(
            r.get("source") != "deterministic_injection"
            for r in memo["key_evidence"]
        )

    @patch("app.services.llm_decision_memo._get_client")
    def test_compliant_first_response_no_retry(self, mock_get_client, monkeypatch):
        _enable_structured_memo(monkeypatch)
        good = _memo_with_dg_row("Recommend — evidenced demand")
        client = _mock_client_returning(good)
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=_DG_RANK1_CANDIDATE, brief=BASE_STRUCTURED_BRIEF, lang="en"
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        assert client.chat.completions.create.call_count == 1


class TestDgEvidenceInjectionFallback:
    """Layer 2 — when the retry still lacks the row, the deterministic
    row is injected at position 2 (index 1) before persistence."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_retry_still_missing_injects_en_row_at_position_2(
        self, mock_get_client, monkeypatch
    ):
        _enable_structured_memo(monkeypatch)
        first = _memo_with_headline("Recommend — strong site")
        second = _memo_with_headline("Recommend — strong site again")
        client = _two_response_client(first, second)
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=_DG_RANK1_CANDIDATE, brief=BASE_STRUCTURED_BRIEF, lang="en"
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        assert client.chat.completions.create.call_count == 2
        injected = memo["key_evidence"][1]
        assert injected["signal"] == _DG_SIGNAL_EN
        assert injected["value"] == "74/100"
        assert injected["polarity"] == "positive"
        assert injected["source"] == "deterministic_injection"
        assert injected["implication"]
        # Exactly one injected row.
        assert sum(
            1 for r in memo["key_evidence"]
            if r.get("source") == "deterministic_injection"
        ) == 1

    @patch("app.services.llm_decision_memo._get_client")
    def test_ar_injection_uses_rule7_term_with_latin_digits(
        self, mock_get_client, monkeypatch
    ):
        _enable_structured_memo(monkeypatch)
        first = _memo_with_headline("نوصي — موقع قوي")
        second = _memo_with_headline("نوصي — موقع قوي مجدداً")
        client = _two_response_client(first, second)
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=_DG_RANK1_CANDIDATE, brief=BASE_STRUCTURED_BRIEF, lang="ar"
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        injected = memo["key_evidence"][1]
        assert injected["signal"] == _DG_SIGNAL_AR
        assert injected["value"] == "74/100"  # Latin digits (Rule 8)
        assert injected["source"] == "deterministic_injection"
        # Implication is Arabic with no Eastern-Arabic digits.
        assert any("؀" <= ch <= "ۿ" for ch in injected["implication"])
        assert not any("٠" <= ch <= "٩" for ch in injected["value"])

    @patch("app.services.llm_decision_memo._get_client")
    def test_dg_retry_with_regressed_headline_keeps_first_and_injects(
        self, mock_get_client, monkeypatch
    ):
        """Edge: first response has a valid headline but no dg row; the
        retry produces the row but an invalid headline. The first response
        wins and the row is injected deterministically."""
        _enable_structured_memo(monkeypatch)
        first = _memo_with_headline("Recommend — strong site, valid headline")
        second = _memo_with_dg_row("consider due to mixed signals")
        client = _two_response_client(first, second)
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=_DG_RANK1_CANDIDATE, brief=BASE_STRUCTURED_BRIEF, lang="en"
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        assert memo["headline_recommendation"] == (
            "Recommend — strong site, valid headline"
        )
        assert memo["key_evidence"][1]["source"] == "deterministic_injection"


    @patch("app.services.llm_decision_memo._get_client")
    def test_colliding_value_row_still_retries_then_injects(
        self, mock_get_client, monkeypatch
    ):
        """Production collision (parcel-6706340 shape): composite rounds to
        60 and the first response carries only a coincidental "60/100" row
        with no generator attribution. The detector must NOT accept it —
        the retry fires, the retry is still non-compliant, and the
        deterministic row is injected at index 1 with the marker."""
        _enable_structured_memo(monkeypatch)
        colliding = _memo_with_headline("Recommend — strong site")
        colliding["key_evidence"] = list(colliding["key_evidence"]) + [
            {"signal": "Demand Strength", "value": "60/100",
             "implication": "demand looks adequate", "polarity": "positive"},
        ]
        second = _memo_with_headline("Recommend — strong site again")
        client = _two_response_client(colliding, second)
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=_DG_COMPOSITE_60_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        # Retry fired (the colliding row did not satisfy the mandate).
        assert client.chat.completions.create.call_count == 2
        injected = memo["key_evidence"][1]
        assert injected["signal"] == _DG_SIGNAL_EN
        assert injected["value"] == "60/100"
        assert injected["source"] == "deterministic_injection"


class TestDgInjectionHelper:
    """Direct unit tests on the injection primitive — idempotence and the
    null-out guard."""

    def test_injects_at_index_1_after_rent_anchor(self):
        from app.services.llm_decision_memo import _inject_dg_evidence_row

        memo = {"key_evidence": [
            {"signal": "annual rent", "value": "SAR 480,000/yr",
             "implication": "x", "polarity": "positive"},
            {"signal": "frontage", "value": "24 m corner",
             "implication": "x", "polarity": "positive"},
        ]}
        assert _inject_dg_evidence_row(memo, composite_rounded=74, locale="en")
        assert memo["key_evidence"][0]["signal"] == "annual rent"
        assert memo["key_evidence"][1]["signal"] == _DG_SIGNAL_EN
        assert memo["key_evidence"][2]["signal"] == "frontage"

    def test_idempotent_never_double_injects(self):
        from app.services.llm_decision_memo import _inject_dg_evidence_row

        memo = {"key_evidence": [
            {"signal": "annual rent", "value": "SAR 480,000/yr",
             "implication": "x", "polarity": "positive"},
        ]}
        assert _inject_dg_evidence_row(memo, composite_rounded=74, locale="en")
        assert not _inject_dg_evidence_row(memo, composite_rounded=74, locale="en")
        assert len(memo["key_evidence"]) == 2

    def test_skips_empty_key_evidence_null_out_path(self):
        from app.services.llm_decision_memo import _inject_dg_evidence_row

        memo = {"key_evidence": []}
        assert not _inject_dg_evidence_row(memo, composite_rounded=74, locale="en")
        assert memo["key_evidence"] == []

    def test_polarity_bands(self):
        from app.services.llm_decision_memo import _dg_required_evidence_row

        assert _dg_required_evidence_row(74, "en")["polarity"] == "positive"
        assert _dg_required_evidence_row(60, "en")["polarity"] == "positive"
        assert _dg_required_evidence_row(45, "en")["polarity"] == "neutral"
        assert _dg_required_evidence_row(30, "en")["polarity"] == "negative"


class TestMemoPromptVersionBumpedForV121:
    """The two non-compliant production memos are cached at v12; the bump
    forces regeneration on next view."""

    def test_version_is_v12_1(self):
        from app.services.llm_decision_memo import MEMO_PROMPT_VERSION

        assert MEMO_PROMPT_VERSION == "v12.1-demand-evidence-enforced-2026-06"


class TestRenderPromptAdvisoryFailureNoGateFailureAddendum:
    """Bug 1 prompt-side fix — an advisory-only gate failure must NOT
    inject the "GATE FAILURE ... overall_pass=False" addendum."""

    def test_advisory_failure_uses_softer_addendum(self):
        ctx = build_memo_context(
            candidate=_RANK1_ADVISORY_FAILURE_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        messages = render_structured_memo_prompt(ctx)
        system_content = messages[0]["content"]

        assert "GATE FAILURE" not in system_content
        assert "ADVISORY GATE NOTE" in system_content
        assert "Market growth signal" in system_content


class TestRenderPromptBlockingFailureKeepsGateFailureAddendum:
    """Bug 1 prompt-side fix — a blocking gate failure (zoning) must
    still inject the strong "GATE FAILURE" addendum."""

    def test_blocking_failure_triggers_gate_failure_addendum(self):
        ctx = build_memo_context(
            candidate=_BLOCKING_FAILURE_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="en",
        )
        messages = render_structured_memo_prompt(ctx)
        system_content = messages[0]["content"]

        assert "GATE FAILURE" in system_content
        assert "zoning fit" in system_content


def _pct_from_fraction_js(frac):
    """Reference re-implementation of the frontend ``pctFromFraction``
    (AdvisorySectionCards.tsx:28-39), using JS ``Math.round`` (round-half-up)
    semantics, so the backend helper can be asserted against it byte-for-byte.
    Returns ``(zone, value)`` where ``value`` is the integer shown to the user
    (``None`` for the MID zone, which carries no number)."""
    import math as _math

    clamped = max(0.0, min(1.0, frac))
    pct = int(_math.floor(clamped * 100.0 + 0.5))  # JS Math.round
    if 40 <= pct <= 60:
        return ("mid", None)
    if pct < 40:
        return ("low", 100 - pct)
    return ("high", pct)


class TestRentPositioning:
    """``_rent_positioning`` moves the ``1 − percentile`` inversion out of the
    LLM and MUST mirror the frontend ``pctFromFraction`` exactly (Finding 5)."""

    def test_known_anchor_fractions(self):
        cases = {
            0.28: ("low", 72),
            0.375: ("low", 62),   # production sample 6545795 → cheaper than ~62%
            0.50: ("mid", None),
            0.70: ("high", 70),
        }
        for frac, (zone, value) in cases.items():
            out = _rent_positioning(frac, "district")
            assert out is not None
            assert (out["zone"], out["pct_value"]) == (zone, value), frac
            assert out["scope"] == "district"

    def test_agrees_with_frontend_pct_from_fraction_on_every_fraction(self):
        # Sweep 0..1 at 0.001 resolution — every value (incl. the .5 rounding
        # boundaries where banker's rounding would diverge, e.g. 0.125) must
        # match the JS round-half-up reference.
        for i in range(0, 1001):
            frac = i / 1000.0
            out = _rent_positioning(frac, "city")
            assert (out["zone"], out["pct_value"]) == _pct_from_fraction_js(frac), frac

    def test_banker_rounding_boundary_uses_round_half_up(self):
        # 0.125 → JS Math.round(12.5)=13 → cheaper than 87%; Python round()
        # would give 12 → 88. Confirm we follow the frontend, not round().
        assert _rent_positioning(0.125, "district") == {
            "zone": "low",
            "pct_value": 87,
            "scope": "district",
            "phrase_en": "cheaper than about 87% of district comparables",
            "phrase_ar": "أقل من حوالي 87% من المقارنات في الحي",
        }

    def test_none_percentile_returns_none(self):
        assert _rent_positioning(None, "district") is None

    def test_out_of_range_fractions_are_clamped(self):
        assert _rent_positioning(-0.2, "city")["zone"] == "low"
        assert _rent_positioning(1.7, "city") == {
            "zone": "high",
            "pct_value": 100,
            "scope": "city",
            "phrase_en": "more expensive than about 100% of citywide comparables",
            "phrase_ar": "أعلى من حوالي 100% من المقارنات على مستوى المدينة",
        }

    def test_median_invariant_listing_below_median_is_cheaper_than_over_50pct(self):
        # Invariant: listing rent < median  ⟺  rendered "cheaper than > 50%".
        # Production sample: listing 141.47 < median 164.72, percentile 0.375.
        out = _rent_positioning(0.375, "district")
        assert out["zone"] == "low"
        assert out["pct_value"] > 50

    # ── PR-E Change 2 — pre-rendered phrase (production inversion fix) ──

    def test_below_median_rank_renders_cheapness_in_en_and_ar(self):
        # Production defect: rank 0.09 (cheaper than ~91% of 9 district
        # comparables) was rendered "cheaper than about 9%" — the raw rank
        # slotted into the v11 template. The pre-rendered phrase pins the
        # corrected, internally consistent wording in both locales.
        out = _rent_positioning(0.09, "district")
        assert out["zone"] == "low"
        assert out["pct_value"] == 91
        assert out["phrase_en"] == "cheaper than about 91% of district comparables"
        assert out["phrase_ar"] == "أقل من حوالي 91% من المقارنات في الحي"
        # The contradictory raw-rank rendering must not appear.
        assert "9%" not in out["phrase_en"].replace("91%", "")
        assert "9%" not in out["phrase_ar"].replace("91%", "")

    def test_phrase_number_always_matches_pct_value(self):
        # Phrase and number agree by construction in every non-mid zone.
        for i in range(0, 101):
            out = _rent_positioning(i / 100.0, "district")
            if out["zone"] == "mid":
                assert out["pct_value"] is None
                assert "%" not in out["phrase_en"]
                assert "%" not in out["phrase_ar"]
            else:
                assert f"{out['pct_value']}%" in out["phrase_en"]
                assert f"{out['pct_value']}%" in out["phrase_ar"]
                verb = "cheaper than about" if out["zone"] == "low" else "more expensive than about"
                assert out["phrase_en"].startswith(verb)

    def test_mid_zone_phrase_has_no_percentage(self):
        out = _rent_positioning(0.50, "district")
        assert out["phrase_en"] == "around the median rent of district comparables"
        assert out["phrase_ar"] == "قريب من الإيجار الوسيط بين المقارنات في الحي"

    def test_phrase_scope_labels(self):
        assert "citywide comparables in the same band/type" in _rent_positioning(0.2, "city_band")["phrase_en"]
        assert "المقارنات في نفس النطاق على مستوى المدينة" in _rent_positioning(0.2, "city_band")["phrase_ar"]
        assert _rent_positioning(0.2, "city")["phrase_en"].endswith("citywide comparables")
        # Unrecognized / absent scope makes no scope claim.
        assert _rent_positioning(0.2, None)["phrase_en"] == "cheaper than about 80% of comparables"
        assert _rent_positioning(0.2, None)["phrase_ar"] == "أقل من حوالي 80% من المقارنات"
