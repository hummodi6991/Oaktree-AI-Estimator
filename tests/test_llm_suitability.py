"""Unit tests for the LLM suitability classifier.

The OpenAI client is mocked — these tests do not make real API calls
and do not require OPENAI_API_KEY to be set.
"""
from unittest.mock import MagicMock, patch

from app.services.llm_suitability import (
    classify_listing,
    _coerce_int_score,
    _coerce_verdict,
    _parse_response,
)


def test_coerce_int_score_clamps_range():
    assert _coerce_int_score(50) == 50
    assert _coerce_int_score(150) == 100
    assert _coerce_int_score(-5) == 0
    assert _coerce_int_score("75") == 75
    assert _coerce_int_score("not a number") == 50  # default


def test_coerce_verdict_normalizes():
    assert _coerce_verdict("suitable") == "suitable"
    assert _coerce_verdict("UNSUITABLE") == "unsuitable"
    assert _coerce_verdict("nonsense") == "uncertain"
    assert _coerce_verdict(None) == "uncertain"


def test_parse_response_handles_code_fences():
    text = '```json\n{"suitability_score": 80}\n```'
    parsed = _parse_response(text)
    assert parsed == {"suitability_score": 80}


def test_parse_response_handles_preamble():
    text = 'Here is my analysis:\n{"suitability_score": 70}'
    parsed = _parse_response(text)
    assert parsed == {"suitability_score": 70}


def test_parse_response_returns_none_on_garbage():
    assert _parse_response("not json at all") is None
    assert _parse_response("") is None


def _mock_completion(json_content: str):
    completion = MagicMock()
    completion.choices = [MagicMock()]
    completion.choices[0].message.content = json_content
    return completion


def test_classify_listing_returns_neutral_on_api_error():
    """When the OpenAI API call raises, classify_listing returns a
    neutral uncertain verdict instead of propagating the exception.
    """
    with patch("app.services.llm_suitability._get_client") as mock_get:
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = RuntimeError("api down")
        mock_get.return_value = mock_client

        result = classify_listing({"aqar_id": "test123"})

        assert result["llm_suitability_verdict"] == "uncertain"
        assert result["llm_suitability_score"] is None
        assert "failed" in result["llm_reasoning"].lower()
        assert result["llm_classifier_version"] == "v1.0"


def test_classify_listing_text_path_happy():
    json_response = """{
        "suitability_score": 88,
        "suitability_verdict": "suitable",
        "listing_quality_score": 70,
        "landlord_signal_score": 90,
        "reasoning": "Strong F&B context."
    }"""
    with patch("app.services.llm_suitability._get_client") as mock_get:
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _mock_completion(
            json_response
        )
        mock_get.return_value = mock_client

        result = classify_listing(
            {
                "aqar_id": "6636258",
                "description": "shop in al-shafa district",
            }
        )

        assert result["llm_suitability_verdict"] == "suitable"
        assert result["llm_suitability_score"] == 88
        assert result["llm_listing_quality_score"] == 70
        assert result["llm_landlord_signal_score"] == 90
        assert "F&B" in result["llm_reasoning"]


def test_classify_listing_uncertain_triggers_photo_retry():
    """If the text-only pass returns uncertain and photos are provided,
    a second call is made with the photos.
    """
    uncertain_response = (
        '{"suitability_score": 50, "suitability_verdict": "uncertain",'
        ' "listing_quality_score": 50, "landlord_signal_score": 30,'
        ' "reasoning": "sparse"}'
    )
    confident_response = (
        '{"suitability_score": 15, "suitability_verdict": "unsuitable",'
        ' "listing_quality_score": 10, "landlord_signal_score": 5,'
        ' "reasoning": "concrete shell visible in photos"}'
    )

    with patch("app.services.llm_suitability._get_client") as mock_get:
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = [
            _mock_completion(uncertain_response),
            _mock_completion(confident_response),
        ]
        mock_get.return_value = mock_client

        result = classify_listing(
            {"aqar_id": "6630998", "description": "Shop for Rent"},
            photo_urls=["https://example.com/photo.jpg"],
        )

        assert mock_client.chat.completions.create.call_count == 2
        assert result["llm_suitability_verdict"] == "unsuitable"
        assert result["llm_suitability_score"] == 15


def test_classify_listing_uncertain_without_photos_stays_uncertain():
    """If text pass returns uncertain and no photo URLs are given,
    no retry is attempted and the uncertain verdict is preserved.
    """
    uncertain_response = (
        '{"suitability_score": 50, "suitability_verdict": "uncertain",'
        ' "listing_quality_score": 50, "landlord_signal_score": 30,'
        ' "reasoning": "sparse"}'
    )

    with patch("app.services.llm_suitability._get_client") as mock_get:
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _mock_completion(
            uncertain_response
        )
        mock_get.return_value = mock_client

        result = classify_listing(
            {"aqar_id": "test456", "description": "store for rent"},
            photo_urls=None,
        )

        assert mock_client.chat.completions.create.call_count == 1
        assert result["llm_suitability_verdict"] == "uncertain"
        assert result["llm_suitability_score"] == 50
