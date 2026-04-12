"""Tests for app.services.llm_decision_memo."""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from app.services.llm_decision_memo import (
    _daily_cost_tracker,
    _format_rent_vs_median,
    _today_key,
    generate_decision_memo,
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
        today = _today_key()
        _daily_cost_tracker[today] = 1.00

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

        assert result["headline"] == VALID_LLM_RESPONSE["headline"]
        assert result["fit_summary"] == VALID_LLM_RESPONSE["fit_summary"]
        assert len(result["top_reasons_to_pursue"]) == 3
        assert len(result["top_risks"]) == 3
        assert result["recommended_next_action"] == VALID_LLM_RESPONSE["recommended_next_action"]
        assert result["rent_context"] == VALID_LLM_RESPONSE["rent_context"]


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

        assert result["top_risks"] == []
        assert result["top_reasons_to_pursue"] == []
        assert result["headline"] == "CONSIDER: Decent spot"

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

        assert result["fit_summary"] == "—"
        assert result["recommended_next_action"] == "—"
        assert result["rent_context"] == "—"


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

        # Capture the prompt sent to the mock client
        call_args = mock_client.chat.completions.create.call_args
        messages = call_args.kwargs.get("messages") or call_args[1].get("messages", [])
        prompt_text = messages[0]["content"]

        # The Arabic template contains this string
        assert "موجز العلامة التجارية للمشغّل" in prompt_text
