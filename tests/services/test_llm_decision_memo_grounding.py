"""Regression tests for the StructuredMemo HOW IT COMPARES grounding fix.

Background — Cohort-46 production observed memos with claims like
"lower rent than Dunkin's typical pricing" and "higher price points than
Ron's Burger and Box Master". The candidate-memo response's
``comparable_competitors_json`` carries only ``display_name_en`` /
``display_name_ar``, ``branch_count``, ``nearest_distance_m`` and
``district`` — there is no rent, pricing, margin, AOV, throughput, or
operational data on a named competitor. Any prose claim about a named
competitor's economics is therefore a fabrication.

The fix lives in ``app/services/llm_decision_memo.py``:

  1. A new prohibition rule (rule #6) in CRITICAL OUTPUT FORMAT RULES.
  2. Regrounded ``comparison`` few-shot fields across the voice examples.
  3. An AVOID section enumerating banned phrasings.

These tests verify both the prompt construction (deterministic, always
runs) and the live LLM behaviour (opt-in via ``RUN_LLM_GROUNDING_TESTS``
+ a real ``OPENAI_API_KEY``). The deterministic tests guard against a
future PR silently dropping the grounding rule from the prompt.
"""

from __future__ import annotations

import json
import os
import re
from unittest.mock import MagicMock, patch

import pytest

from app.services.llm_decision_memo import (
    STRUCTURED_MEMO_SYSTEM_PROMPT,
    build_memo_context,
    generate_structured_memo,
    render_structured_memo_prompt,
)


# ── Fixture: candidate with two named competitors carrying ONLY the
# grounded spatial / presence facts (no rent, pricing, margin, AOV). ──

def _grounding_fixture_candidate() -> dict:
    """Candidate whose comparable_competitors carry only display_name_en,
    branch_count, nearest_distance_m — exactly the fields the candidate-
    memo API actually emits."""
    return {
        "id": "cand-grounding-1",
        "parcel_id": "parcel-grounding-1",
        "rank_position": 1,
        "feature_snapshot_json": {
            "district": "Al Olaya",
            "district_display": "العليا",
            "area_m2": 150,
            "estimated_annual_rent_sar": 432000,
            "comparable_median_annual_rent_sar": 542000,
            "comparable_n": 14,
            "comparable_source_label": "district_type",
            "unit_street_width_m": 18,
            "access_visibility_score": 78,
            "population_reach": 38000,
            "delivery_listing_count": 22,
            "brand_presence": {
                "top_chains": [
                    {
                        "display_name_en": "Dunkin",
                        "display_name_ar": None,
                        "branch_count": 1,
                        "nearest_distance_m": 240,
                    },
                    {
                        "display_name_en": "Starbucks",
                        "display_name_ar": None,
                        "branch_count": 2,
                        "nearest_distance_m": 380,
                    },
                ],
            },
            "candidate_location": {"is_vacant": True},
            "listing_age": {"created_days": 42},
        },
        "score_breakdown_json": {
            "occupancy_economics": 78,
            "listing_quality": 70,
            "brand_fit": 76,
            "competition_whitespace": 60,
            "demand_potential": 72,
            "access_visibility": 78,
            "landlord_signal": 60,
            "delivery_demand": 65,
            "confidence": 80,
            "economics_detail": {"rent_burden": {"percentile": 0.28}},
        },
        "gate_status_json": [
            {"gate": "zoning_fit_pass", "verdict": "pass", "reason": "C-2 allowed"},
            {"gate": "rent_reasonable", "verdict": "pass", "reason": "20% below median"},
        ],
        # Critical: only the four fields the real API emits — name, branch
        # count, distance, district. No rent, pricing, margin, AOV.
        "comparable_competitors_json": [
            {
                "display_name_en": "Dunkin",
                "branch_count": 1,
                "nearest_distance_m": 240,
                "district": "Al Olaya",
            },
            {
                "display_name_en": "Starbucks",
                "branch_count": 2,
                "nearest_distance_m": 380,
                "district": "Al Olaya",
            },
        ],
    }


def _grounding_fixture_brief() -> dict:
    return {
        "brand_name": "TestCafe",
        "category": "cafe",
        "service_model": "cafe",
        "min_area_m2": 80,
        "max_area_m2": 200,
        "target_area_m2": 150,
    }


# ── Banned-phrase regexes shared by deterministic + live tests ───────

_NAMED_COMPETITORS_PATTERN = (
    r"(Dunkin|Starbucks|Peer Chain [A-Z]|Peer [A-Z]\b|KFC|Burger King)"
)

BANNED_COMPARISON_PATTERNS: tuple[re.Pattern[str], ...] = (
    # rent / price / margin / AOV / throughput tied to a named competitor —
    # both directions, since the fabrication can read "AOV ... Peer Chain A"
    # or "Peer Chain A's typical AOV".
    re.compile(
        r"(rent|price|pricing|margin|AOV|throughput).{0,40}" + _NAMED_COMPETITORS_PATTERN,
        re.IGNORECASE,
    ),
    re.compile(
        _NAMED_COMPETITORS_PATTERN
        + r".{0,40}(rent|price|pricing|margin|AOV|throughput)",
        re.IGNORECASE,
    ),
    re.compile(
        _NAMED_COMPETITORS_PATTERN
        + r".{0,40}(typical|usual|standard|premium).{0,20}(price|rent|positioning)",
        re.IGNORECASE,
    ),
    re.compile(
        r"compared to.{0,30}(typical|established|standard).{0,30}"
        r"(price|rent|positioning|economics)",
        re.IGNORECASE,
    ),
)

# Presence / grounded-facts the comparison MAY use. The output must include
# at least one of these.
GROUNDED_PRESENCE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"branch(?:es)?", re.IGNORECASE),
    re.compile(r"\bm\b|metres?|meters?|distance|nearest", re.IGNORECASE),
    re.compile(r"operate(?:s)?\s+(?:nearby|within|at)", re.IGNORECASE),
    re.compile(r"district", re.IGNORECASE),
)


def _assert_comparison_grounded(comparison: str) -> None:
    """Assertion helper — share between deterministic + live tests."""
    assert isinstance(comparison, str) and comparison.strip(), (
        f"comparison must be a non-empty string, got: {comparison!r}"
    )
    for pat in BANNED_COMPARISON_PATTERNS:
        match = pat.search(comparison)
        assert match is None, (
            f"comparison violates competitor-economics grounding rule "
            f"(matched pattern {pat.pattern!r} at {match.group()!r}): "
            f"{comparison!r}"
        )
    assert any(pat.search(comparison) for pat in GROUNDED_PRESENCE_PATTERNS), (
        f"comparison must reference at least one grounded fact "
        f"(branch count, distance, presence language, or district), "
        f"got: {comparison!r}"
    )


# ── Deterministic prompt-content tests (no LLM call) ────────────────


class TestPromptContainsGroundingRule:
    """The new prohibition rule + AVOID section must be present in the
    rendered system prompt for every memo call. This guards the prompt
    against future regressions that drop the rule wording silently."""

    def test_critical_rules_block_includes_competitor_economics_prohibition(self):
        ctx = build_memo_context(
            candidate=_grounding_fixture_candidate(),
            brief=_grounding_fixture_brief(),
            lang="en",
        )
        messages = render_structured_memo_prompt(ctx)
        system = messages[0]["content"]

        assert "CRITICAL OUTPUT FORMAT RULES" in system
        # Rule #6 — the competitor-economics prohibition. The wording is
        # wrapped across lines in the prompt; collapse whitespace so the
        # test is resilient to harmless reformatting.
        flat = re.sub(r"\s+", " ", system)
        assert "Do NOT make any claims about a named competitor's rent, pricing, margin, AOV, throughput" in flat
        assert "competitor name, branch count, nearest distance, and district" in flat

    def test_avoid_section_lists_banned_phrasings(self):
        ctx = build_memo_context(
            candidate=_grounding_fixture_candidate(),
            brief=_grounding_fixture_brief(),
            lang="en",
        )
        system = render_structured_memo_prompt(ctx)[0]["content"]

        assert "AVOID" in system
        assert "Dunkin's typical pricing" in system
        assert "Starbucks premium positioning" in system
        # The AVOID note explaining WHY (no AOV / no margin / no price data
        # on competitors) anchors the rule.
        assert "no AOV on competitors" in system
        assert "no margin data on competitors" in system

    def test_voice_examples_no_longer_fabricate_competitor_rent(self):
        """Example C / D / E / F comparison fields must not assert what a
        named competitor pays in rent — that's the bug we're fixing."""
        # These exact strings were the regressed examples before the fix.
        assert "undercuts Peer Chain A on rent by roughly 17%" not in STRUCTURED_MEMO_SYSTEM_PROMPT
        assert "Peer Chain A in this district closed at roughly SAR" not in STRUCTURED_MEMO_SYSTEM_PROMPT
        assert "Peer Chain A in the same district closed at SAR" not in STRUCTURED_MEMO_SYSTEM_PROMPT


# ── Mock-LLM contract test: a model that ignored the rule produces an
# output the assertion correctly rejects. A model that obeyed it produces
# an output the assertion accepts. ──


class TestComparisonAssertionContract:
    """Sanity-check the regex set: catches fabrications, accepts grounded
    prose. Independent of any actual LLM call."""

    def test_assertion_rejects_fabricated_dunkin_rent(self):
        bad = (
            "This site beats Dunkin on rent by roughly 12% and matches "
            "Starbucks' typical price points on visibility."
        )
        with pytest.raises(AssertionError, match="grounding rule"):
            _assert_comparison_grounded(bad)

    def test_assertion_rejects_fabricated_aov_claim(self):
        bad = "Compared to Peer Chain A's typical AOV, this site's mix is stronger."
        with pytest.raises(AssertionError, match="grounding rule"):
            _assert_comparison_grounded(bad)

    def test_assertion_accepts_grounded_presence_prose(self):
        good = (
            "Dunkin operates 1 branch within 240 m and Starbucks 2 branches "
            "at 380 m — established cafe demand at this corner. Against rank 2 "
            "in this search, this site pulls ahead on rent positioning (cheaper "
            "than ~72% vs cheaper than ~53%) and access/visibility (78/100 vs 71/100)."
        )
        # Should not raise.
        _assert_comparison_grounded(good)

    def test_assertion_requires_at_least_one_grounded_fact(self):
        bare = "This site is a strong fit."
        with pytest.raises(AssertionError, match="grounded fact"):
            _assert_comparison_grounded(bare)


# ── Live LLM regression test (opt-in) ───────────────────────────────


def _live_llm_enabled() -> bool:
    """Live LLM tests are opt-in. Set RUN_LLM_GROUNDING_TESTS=1 and
    OPENAI_API_KEY to a real key to exercise them. CI runs with the flag
    unset and the test skips."""
    return (
        os.environ.get("RUN_LLM_GROUNDING_TESTS", "").strip() == "1"
        and bool(os.environ.get("OPENAI_API_KEY", "").strip())
    )


@pytest.mark.skipif(
    not _live_llm_enabled(),
    reason=(
        "Set RUN_LLM_GROUNDING_TESTS=1 and OPENAI_API_KEY to run the live "
        "LLM regression. Loops 5 iterations to handle nondeterminism."
    ),
)
@pytest.mark.parametrize("iteration", range(5))
def test_live_llm_comparison_does_not_fabricate_competitor_economics(iteration):
    """Live LLM regression — runs 5 times under nondeterminism. Each
    generation must produce a comparison that does not match any banned
    fabrication regex AND references at least one grounded fact."""
    ctx = build_memo_context(
        candidate=_grounding_fixture_candidate(),
        brief=_grounding_fixture_brief(),
        lang="en",
    )
    result = generate_structured_memo(ctx)
    assert result is not None, (
        f"iteration {iteration}: generate_structured_memo returned None — "
        "the live LLM call failed or was disabled by config."
    )
    _assert_comparison_grounded(result["comparison"])


# ── Mocked end-to-end smoke test (always runs) ──────────────────────


class TestStructuredMemoMockedEndToEnd:
    """Exercise the full ``generate_structured_memo`` → parse → return
    pipeline against a mocked LLM that emits a grounded comparison.
    Ensures the pipeline does not strip or rewrite the comparison field."""

    @patch("app.services.llm_decision_memo._get_client")
    def test_grounded_mock_response_passes_assertion(self, mock_get_client):
        grounded_response = {
            "headline_recommendation": "Recommend — rent is cheaper than about 72% of district comparables.",
            "ranking_explanation": (
                "Rent is the spine of the case: SAR 432,000/yr is cheaper than about 72% "
                "of 14 district comparables — roughly 20% below the SAR 542,000 median. "
                "Site quality reinforces the economics with an access/visibility score of "
                "78/100, and a population reach of 38,000 supports the dine-in mix. "
                "Two named chains operate within 500 m, validating the catchment."
            ),
            "key_evidence": [
                {"signal": "annual rent", "value": "SAR 432,000/yr",
                 "implication": "asking sits roughly 20% below the district median",
                 "polarity": "positive"},
                {"signal": "rent percentile vs comparables",
                 "value": "cheaper than ~72% (vs 14 district comparables)",
                 "implication": "deal pricing is genuinely below market",
                 "polarity": "positive"},
                {"signal": "access/visibility score", "value": "78/100",
                 "implication": "site quality reinforces the rent advantage",
                 "polarity": "positive"},
                {"signal": "named chains within 500 m", "value": "2 count",
                 "implication": "the catchment validates the category",
                 "polarity": "neutral"},
            ],
            "risks": [
                {"risk": "Two established chains operate within 500 m — undifferentiated entry will compete on price.",
                 "mitigation": "Lead with a single-SKU hero menu in the first 90 days."},
                {"risk": "Listing has been live for 42 days.",
                 "mitigation": "Open negotiation 8% below asking."},
            ],
            "comparison": (
                "Dunkin operates 1 branch within 240 m of this site and Starbucks holds "
                "2 branches at 380 m — established cafe demand at this corner, not a "
                "greenfield. The two named operators in this district confirm the category "
                "trades at scale here, which raises the bar on differentiation for an "
                "incoming brand."
            ),
            "bottom_line": "Sign it — the rent is the deal.",
            "property_overview": {
                "summary": "150 m² unit on an 18 m secondary street in Al Olaya; listed 42 days ago.",
                "area_m2": 150,
                "frontage_width_m": 18,
                "street_type": "secondary",
                "parking_evidence": "shared",
                "visibility_score": 78,
                "listing_age_days": 42,
                "vacancy_status": "vacant",
            },
            "financial_framing": {
                "summary": "SAR 432,000/yr cheaper than about 72% of 14 district comparables.",
                "thesis": (
                    "Rent is the spine of the case. SAR 432,000/yr lands roughly 20% below "
                    "the SAR 542,000 district median across 14 peer listings — a cushion "
                    "that compounds across a five-year lease and absorbs first-year ramp risk."
                ),
                "annual_rent_sar": 432000,
                "comparable_median_annual_rent_sar": 542000,
                "rent_percentile_vs_comparables": 0.28,
                "comparable_n": 14,
                "comparable_scope": "district",
                "spread_to_median_sar": -110000,
            },
            "market_context": {
                "summary": "38,000 walking-catchment population in Al Olaya with 22 delivery listings.",
                "demand_thesis": (
                    "A walking-catchment population of 38,000 supports the dine-in mix "
                    "without leaning on delivery, and 22 active delivery listings in the "
                    "corridor confirm the category trades. Realized order data was not "
                    "available for this catchment so the demand read leans on reach."
                ),
                "population_reach": 38000,
                "district_momentum": None,
                "realized_demand_30d": None,
                "realized_demand_branches": None,
                "delivery_listing_count": 22,
            },
            "competitive_landscape": {
                "summary": "Dunkin and Starbucks operate within 500 m of this site.",
                "saturation_thesis": (
                    "Dunkin operates 1 branch within 240 m and Starbucks 2 branches at "
                    "380 m — the cafe category trades at this corner, raising the bar on "
                    "differentiation. The operator is paying roughly 20% below the district "
                    "median for a stronger street position than peer listings priced at the "
                    "median."
                ),
                "top_chains": [
                    {"display_name_en": "Dunkin", "display_name_ar": None,
                     "branch_count": 1, "nearest_distance_m": 240},
                    {"display_name_en": "Starbucks", "display_name_ar": None,
                     "branch_count": 2, "nearest_distance_m": 380},
                ],
                "comparable_competitors": [],
                "next_candidate_summary": None,
            },
        }
        client = MagicMock()
        client.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content=json.dumps(grounded_response)))],
            usage=MagicMock(prompt_tokens=400, completion_tokens=220),
        )
        mock_get_client.return_value = client

        ctx = build_memo_context(
            candidate=_grounding_fixture_candidate(),
            brief=_grounding_fixture_brief(),
            lang="en",
        )
        result = generate_structured_memo(ctx)

        assert result is not None
        _assert_comparison_grounded(result["comparison"])
