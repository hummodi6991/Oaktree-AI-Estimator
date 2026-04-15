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


# ── Structured memo (Phase 1) ───────────────────────────────────────


VALID_STRUCTURED_RESPONSE = {
    "headline_recommendation": "recommend — 15% below-median rent with strong delivery pull",
    "ranking_explanation": (
        "occupancy_economics contributed 27.0 out of 30 and brand_fit 8.8 out of 11, "
        "driving rank 2 of 12. Access_visibility at 6.5 of 10 was the only sub-median "
        "component and the gap to rank 1 is narrow."
    ),
    "key_evidence": [
        {"signal": "annual rent", "value": "SAR 480,000/yr", "implication": "15% below Al Olaya median"},
        {"signal": "realized demand 30d", "value": "1,400 orders", "implication": "7.8x district median"},
    ],
    "risks": [
        {"risk": "street width 8 m limits drive-thru", "mitigation": "curbside handoff via Keeta"},
    ],
    "comparison": "Matches Peer A on rent and beats Peer B on realized demand by 3x.",
    "bottom_line": "Take it — the rent alone justifies the deal.",
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
        # Spot-check the headline number the prompt expects to see
        assert contributions["occupancy_economics"] == 27.0
        # Weights sub-dict carried through for the LLM
        assert ctx.score_breakdown["weights"] == dict(COMPONENT_WEIGHTS)


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
        # System prompt was upgraded with EVIDENCE PRIORITY instruction
        assert "EVIDENCE PRIORITY" in messages[0]["content"]


class TestRenderPromptFailedGate:
    """Step 8, test 6."""

    def test_failed_gate_triggers_lead_with_failure_instruction(self):
        cand = dict(BASE_STRUCTURED_CANDIDATE)
        cand["gate_status_json"] = [
            {"gate": "zoning_fit_pass", "verdict": "fail", "reason": "C-2 not allowed on this parcel"},
            {"gate": "rent_reasonable", "verdict": "pass", "reason": "ok"},
        ]

        ctx = build_memo_context(candidate=cand, brief=BASE_STRUCTURED_BRIEF, lang="en")
        messages = render_structured_memo_prompt(ctx)
        system_content = messages[0]["content"]

        assert "GATE FAILURE" in system_content
        assert "zoning_fit_pass" in system_content
        assert "Lead the headline_recommendation with this failure" in system_content


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
    def test_text_renderer_uses_six_section_headers(self):
        out = render_structured_memo_as_text(VALID_STRUCTURED_RESPONSE, "en")
        for header in (
            "## Headline Recommendation",
            "## Ranking Explanation",
            "## Key Evidence",
            "## Risks",
            "## Comparison",
            "## Bottom Line",
        ):
            assert header in out
