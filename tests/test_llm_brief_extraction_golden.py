"""Golden + unit tests for the LLM brief extraction pipeline.

The golden half feeds each fixture's ``expected_extraction`` through the
deterministic post-processing pipeline as a mocked LLM response (the same
``@patch(_get_client)`` pattern as tests/test_llm_decision_memo.py) and
asserts the applied profile delta, unrecognized districts, and conflict
pass-through — CI-green without an API key. The LIVE half (does the real
model produce ``expected_extraction``?) lives in
scripts/llm_brief_extraction_live_eval.py and is the merge gate.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.services.llm_brief_extraction import (
    BRIEF_EXTRACTION_PROMPT_VERSION,
    MAX_BRIEF_CHARS,
    _daily_cost_tracker,
    _invalid_value_counts,
    extract_brief,
    postprocess_extraction,
    proposal_to_profile_delta,
    sanitize_brief_text,
)

GOLDEN_DIR = Path(__file__).parent / "fixtures" / "llm_brief_golden"

FIXTURES = sorted(
    (json.loads(p.read_text(encoding="utf-8")) for p in GOLDEN_DIR.glob("*.json")),
    key=lambda f: f["id"],
)

# Static stand-in for _cached_district_lookup covering every district the
# goldens reference (labels from app/data/riyadh_district_crosswalk.py).
GOLDEN_DISTRICT_LOOKUP = {
    "الياسمين": {"label_ar": "الياسمين", "label_en": "Al Yasmin"},
    "النرجس": {"label_ar": "النرجس", "label_en": "An Narjis"},
    "الملز": {"label_ar": "الملز", "label_en": "Al Malaz"},
    "الديرة": {"label_ar": "الديرة", "label_en": "Ad Dirah"},
    "الملقا": {"label_ar": "الملقا", "label_en": "Al Malqa"},
    "العليا": {"label_ar": "العليا", "label_en": "Al Olaya"},
    "النخيل": {"label_ar": "النخيل", "label_en": "Al Nakheel"},
}


def _make_mock_response(content: dict | str):
    mock = MagicMock()
    mock.choices = [
        MagicMock(
            message=MagicMock(
                content=(
                    json.dumps(content, ensure_ascii=False)
                    if isinstance(content, dict)
                    else content
                )
            )
        )
    ]
    mock.usage = MagicMock(prompt_tokens=500, completion_tokens=200)
    return mock


@pytest.fixture(autouse=True)
def _reset_module_state():
    _daily_cost_tracker.clear()
    _invalid_value_counts.clear()
    yield
    _daily_cost_tracker.clear()
    _invalid_value_counts.clear()


# ── Golden set: all 32 fixtures through the deterministic pipeline ──


def test_golden_set_is_complete():
    assert len(FIXTURES) == 32
    assert sum(1 for f in FIXTURES if f["kind"] == "adversarial") == 8


@pytest.mark.parametrize("fixture", FIXTURES, ids=[f["id"] for f in FIXTURES])
@patch("app.services.llm_brief_extraction._get_client")
def test_golden(mock_get_client, fixture):
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_mock_response(
        fixture["expected_extraction"]
    )
    mock_get_client.return_value = mock_client

    result = extract_brief(
        fixture["brief_text"], fixture["form_context"], GOLDEN_DISTRICT_LOOKUP
    )

    if not fixture["brief_text"].strip():
        # Empty briefs must short-circuit before any LLM call (adv_06).
        mock_client.chat.completions.create.assert_not_called()

    delta = proposal_to_profile_delta(result["proposal"])
    assert delta == fixture["expected_applied"], fixture["rationale"]
    assert (
        result["unrecognized_districts"] == fixture["expected_unrecognized_districts"]
    )

    expected_conflict_fields = [
        c["field"] for c in fixture["expected_extraction"].get("conflicts", [])
    ]
    assert [c["field"] for c in result["conflicts"]] == expected_conflict_fields

    expected_memo_color = fixture["expected_extraction"].get("memo_color", [])
    assert result["memo_color"] == expected_memo_color

    assert result["prompt_version"] == BRIEF_EXTRACTION_PROMPT_VERSION


# ── Unit: post-processing guardrails ─────────────────────────────────


BRIEF = "We are a premium specialty coffee brand in quiet neighborhoods."


def test_evidence_substring_check_drops_field():
    raw = {
        "price_tier": {
            "value": "premium",
            "confidence": "high",
            "evidence": "this quote is not in the brief",
        }
    }
    result = postprocess_extraction(raw, BRIEF, {})
    assert result["proposal"] == {}


def test_evidence_whitespace_normalized_match_passes():
    raw = {
        "price_tier": {
            "value": "premium",
            "confidence": "high",
            "evidence": "premium  specialty   coffee",
        }
    }
    result = postprocess_extraction(raw, BRIEF, {})
    assert result["proposal"]["price_tier"]["value"] == "premium"


def test_out_of_enum_value_dropped_and_counted():
    raw = {
        "price_tier": {"value": "luxury", "confidence": "high", "evidence": "premium"},
        "brand_archetype": {
            "value": "mall_kiosk",
            "confidence": "high",
            "evidence": "coffee",
        },
    }
    result = postprocess_extraction(raw, BRIEF, {})
    assert result["proposal"] == {}
    assert _invalid_value_counts["price_tier"] == 1
    assert _invalid_value_counts["brand_archetype"] == 1


def test_tolerance_clamped_to_0_5000():
    brief = "keep branches 99 km apart, premium brand"
    raw = {
        "cannibalization_tolerance_m": {
            "value": 99000,
            "confidence": "high",
            "evidence": "99 km apart",
        }
    }
    result = postprocess_extraction(raw, brief, {})
    assert result["proposal"]["cannibalization_tolerance_m"]["value"] == 5000.0

    raw["cannibalization_tolerance_m"]["value"] = -50
    result = postprocess_extraction(raw, brief, {})
    assert result["proposal"]["cannibalization_tolerance_m"]["value"] == 0.0


def test_non_numeric_tolerance_dropped():
    raw = {
        "cannibalization_tolerance_m": {
            "value": "two kilometers",
            "confidence": "high",
            "evidence": "premium",
        }
    }
    result = postprocess_extraction(raw, BRIEF, {})
    assert result["proposal"] == {}
    assert _invalid_value_counts["cannibalization_tolerance_m"] == 1


def test_hallucinated_district_mention_dropped_entirely():
    raw = {
        "district_mentions": [
            {"text": "العليا", "polarity": "preferred", "confidence": "high"},
        ]
    }
    # "العليا" does not appear in BRIEF — neither applied nor unrecognized.
    result = postprocess_extraction(raw, BRIEF, GOLDEN_DISTRICT_LOOKUP)
    assert result["proposal"] == {}
    assert result["unrecognized_districts"] == []


def test_service_model_conflict_suppresses_archetype():
    brief = "specialty coffee with quiet seating"
    raw = {
        "brand_archetype": {
            "value": "neighborhood_local",
            "confidence": "medium",
            "evidence": "quiet seating",
        },
        "conflicts": [
            {
                "field": "service_model",
                "evidence": "specialty coffee",
                "note": "café vs qsr",
            }
        ],
    }
    result = postprocess_extraction(raw, brief, {})
    assert "brand_archetype" not in result["proposal"]
    assert result["conflicts"][0]["field"] == "service_model"


@patch("app.services.llm_brief_extraction._get_client")
def test_empty_brief_short_circuits_without_llm_call(mock_get_client):
    result = extract_brief("   ", {}, {})
    mock_get_client.assert_not_called()
    assert result["proposal"] == {}


@patch("app.services.llm_brief_extraction._get_client")
def test_mojibake_brief_short_circuits_without_llm_call(mock_get_client):
    result = extract_brief("��� garbled", {}, {})
    mock_get_client.assert_not_called()
    assert result["proposal"] == {}


@patch("app.services.llm_brief_extraction._get_client")
def test_invalid_json_from_llm_degrades_to_empty(mock_get_client):
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_mock_response(
        "not json at all"
    )
    mock_get_client.return_value = mock_client
    result = extract_brief(BRIEF, {}, {})
    assert result["proposal"] == {}


def test_sanitize_strips_bidi_and_caps_length():
    text = "ab‮cd" + "x" * (MAX_BRIEF_CHARS + 500)
    cleaned = sanitize_brief_text(text)
    assert "‮" not in cleaned
    assert len(cleaned) <= MAX_BRIEF_CHARS


def test_ceiling_raises_runtime_error():
    from app.services.llm_brief_extraction import _check_daily_ceiling, _today_key

    _daily_cost_tracker[_today_key()] = 10.0
    with pytest.raises(RuntimeError, match="ceiling"):
        _check_daily_ceiling()


# ── Endpoint behavior (flag gating, 422, 503) ───────────────────────


class _StubDB:
    """Minimal stub: _build_district_lookup's query fails → caught → {}."""

    def execute(self, *a, **kw):
        raise RuntimeError("no db in this test")

    def begin_nested(self):
        raise RuntimeError("no db in this test")

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


@pytest.fixture
def _clear_overrides():
    yield
    from app.main import app

    app.dependency_overrides.clear()


def _set_flag(monkeypatch, value: bool):
    # Patch the settings instance the API module actually references.
    import app.api.expansion_advisor as api_mod

    monkeypatch.setattr(api_mod.settings, "EXPANSION_BRIEF_EXTRACTION_ENABLED", value)


class TestFlagOffInertness:
    def test_endpoint_404_when_flag_off(self, monkeypatch, _clear_overrides):
        _set_flag(monkeypatch, False)
        client = _endpoint_client(_StubDB())
        res = client.post(
            "/v1/expansion-advisor/brief-extraction",
            json={"brief_text": "specialty coffee"},
        )
        assert res.status_code == 404

    def test_brand_profile_payload_byte_identical_when_unused(self):
        """With the textarea unused, the persisted brand-profile payload must
        be byte-identical to the pre-feature shape (locked decision L6)."""
        from app.api.expansion_advisor import (
            ExpansionBrandProfileInput,
            _brand_profile_request_payload,
        )

        profile = ExpansionBrandProfileInput(price_tier="mid")
        payload = _brand_profile_request_payload(profile)

        legacy_keys = {
            "price_tier",
            "average_check_sar",
            "primary_channel",
            "parking_sensitivity",
            "frontage_sensitivity",
            "visibility_sensitivity",
            "expansion_goal",
            "brand_archetype",
            "cannibalization_tolerance_m",
            "preferred_districts",
            "excluded_districts",
        }
        assert set(payload) == legacy_keys

        legacy_payload = {
            k: v for k, v in profile.model_dump().items() if k in legacy_keys
        }
        assert json.dumps(payload, sort_keys=True, ensure_ascii=False) == json.dumps(
            legacy_payload, sort_keys=True, ensure_ascii=False
        )

    def test_payload_carries_brief_fields_when_present(self):
        from app.api.expansion_advisor import (
            BriefExtractionMetaInput,
            ExpansionBrandProfileInput,
            _brand_profile_request_payload,
        )

        profile = ExpansionBrandProfileInput(
            brief_text="مقهى مختص",
            brief_extraction=BriefExtractionMetaInput(
                model="gpt-4o-mini-2024-07-18",
                prompt_version=BRIEF_EXTRACTION_PROMPT_VERSION[:32],
                accepted=True,
                edited_fields=[],
            ),
        )
        payload = _brand_profile_request_payload(profile)
        assert payload["brief_text"] == "مقهى مختص"
        assert payload["brief_extraction"]["accepted"] is True


class TestEndpointFlagOn:
    @patch("app.services.llm_brief_extraction._get_client")
    def test_happy_path(self, mock_get_client, monkeypatch, _clear_overrides):
        _set_flag(monkeypatch, True)
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _make_mock_response(
            {
                "price_tier": {
                    "value": "value",
                    "confidence": "high",
                    "evidence": "we win on price",
                }
            }
        )
        mock_get_client.return_value = mock_client
        client = _endpoint_client(_StubDB())
        res = client.post(
            "/v1/expansion-advisor/brief-extraction",
            json={
                "brief_text": "budget fried chicken, we win on price",
                "form_context": {"service_model": "qsr"},
            },
        )
        assert res.status_code == 200
        body = res.json()
        assert body["proposal"]["price_tier"]["value"] == "value"
        assert body["prompt_version"] == BRIEF_EXTRACTION_PROMPT_VERSION

    def test_oversized_brief_422(self, monkeypatch, _clear_overrides):
        _set_flag(monkeypatch, True)
        client = _endpoint_client(_StubDB())
        res = client.post(
            "/v1/expansion-advisor/brief-extraction",
            json={"brief_text": "x" * (MAX_BRIEF_CHARS + 1)},
        )
        assert res.status_code == 422

    def test_ceiling_503_with_fallback_message(self, monkeypatch, _clear_overrides):
        from app.services.llm_brief_extraction import _today_key

        _set_flag(monkeypatch, True)
        _daily_cost_tracker[_today_key()] = 10.0
        client = _endpoint_client(_StubDB())
        res = client.post(
            "/v1/expansion-advisor/brief-extraction",
            json={"brief_text": "specialty coffee brand"},
        )
        assert res.status_code == 503
        assert "fill the form manually" in res.json()["detail"]
