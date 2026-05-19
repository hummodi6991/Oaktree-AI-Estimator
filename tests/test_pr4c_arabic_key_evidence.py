"""PR #4c — Arabic key_evidence vocabulary and worked example.

Covers the AR-only prompt changes that make the structured decision memo
emit Arabic ``signal`` and ``value`` strings (rules 7-8 and the worked
Example AR-1 inside ``_CRITICAL_BLOCK_AR``), while keeping the English
composition byte-identical to pre-PR-4a ``main``.
"""

from __future__ import annotations

import pathlib
from unittest.mock import patch

from app.services.llm_decision_memo import (
    STRUCTURED_MEMO_SYSTEM_PROMPT,
    _CRITICAL_BLOCK_AR,
    _compose_structured_system_prompt,
)

from tests.test_llm_decision_memo import (
    BASE_STRUCTURED_BRIEF,
    BASE_STRUCTURED_CANDIDATE,
    VALID_STRUCTURED_RESPONSE,
    _DummyDB,
    _endpoint_client,
    _mock_client_returning,
)

_HEAD_SNAPSHOT_PATH = (
    pathlib.Path(__file__).parent
    / "data"
    / "pr4a_structured_memo_system_prompt_en_head.txt"
)

# The 8 signal-vocabulary strings approved in the PR #4c brief (§1).
_AR_SIGNAL_VOCAB = [
    "الإيجار السنوي",
    "نسبة الإيجار مقابل المقارنات",
    "الواجهة",
    "نقاط الوصول والرؤية",
    "عدد السكان القابلين للوصول",
    "سلاسل مذكورة ضمن 500 م",
    "بوابة الاقتصاديات",
    "عمر الإعلان",
]

# The 6 signal strings the worked Example AR-1 must carry (§3).
_AR1_EXAMPLE_SIGNALS = [
    "الإيجار السنوي",
    "نسبة الإيجار مقابل المقارنات",
    "الواجهة",
    "نقاط الوصول والرؤية",
    "عدد السكان القابلين للوصول",
    "سلاسل مذكورة ضمن 500 م",
]


# ── §6.1 — EN byte-identity (rule #1, re-asserted for PR #4c) ────────


class TestENByteIdentityUnchanged:
    """PR #4c touches ``_CRITICAL_BLOCK_AR`` only — the EN composition
    must still equal the pre-PR-4a fixture byte-for-byte."""

    def test_en_compose_matches_head_snapshot(self):
        head = _HEAD_SNAPSHOT_PATH.read_text(encoding="utf-8")
        assert _compose_structured_system_prompt("en") == head
        assert STRUCTURED_MEMO_SYSTEM_PROMPT == head


# ── §6.2 — AR signal vocabulary present ─────────────────────────────


class TestARSignalVocabulary:

    def test_all_eight_signal_translations_in_ar_prompt(self):
        ar = _compose_structured_system_prompt("ar")
        missing = [v for v in _AR_SIGNAL_VOCAB if v not in ar]
        assert not missing, f"AR signal vocab missing: {missing}"

    def test_value_unit_token_policy_in_ar_prompt(self):
        ar = _compose_structured_system_prompt("ar")
        # Representative §2 unit tokens that must be taught.
        for token in ["ريال سعودي/سنة", "ضمن نطاق المشي", "يوماً", "تقييم/30 يوم"]:
            assert token in ar, f"AR value-token policy missing: {token}"


# ── §6.3 — AR worked example (Example AR-1) present ─────────────────


class TestARWorkedExample:

    def test_example_ar1_present_with_all_six_signals(self):
        ar = _compose_structured_system_prompt("ar")
        assert "Example AR-1" in ar
        # All six key_evidence signal rows render as Arabic JSON values.
        for sig in _AR1_EXAMPLE_SIGNALS:
            assert f'"signal": "{sig}"' in ar, f"Example AR-1 missing signal row: {sig}"

    def test_example_ar1_value_strings_are_arabic(self):
        ar = _compose_structured_system_prompt("ar")
        # The worked example must not leave any value as a bare English token.
        assert '"value": "432,000 ريال سعودي/سنة"' in ar
        assert '"value": "3 منافسين"' in ar
        # JSON keys themselves stay English even in the AR example.
        assert '"polarity": "positive"' in ar


# ── §6.4 — Arabic-yeh discipline ────────────────────────────────────


class TestArabicYehDiscipline:
    """Every yeh in the AR critical block must be U+064A, never the
    Persian/Farsi yeh U+06CC."""

    def test_no_persian_yeh_in_critical_block_ar(self):
        offending = [(i, ch) for i, ch in enumerate(_CRITICAL_BLOCK_AR) if ch == "ی"]
        assert not offending, f"Persian yeh U+06CC found at offsets: {offending}"

    def test_no_persian_yeh_in_full_ar_prompt(self):
        ar = _compose_structured_system_prompt("ar")
        assert "ی" not in ar


# ── §6.5 — AR vocabulary must NOT leak into the EN branch ───────────


class TestNoARVocabInEN:

    def test_ar_signal_vocab_absent_from_en_prompt(self):
        en = _compose_structured_system_prompt("en")
        leaked = [v for v in _AR_SIGNAL_VOCAB if v in en]
        assert not leaked, f"AR vocab leaked into EN prompt: {leaked}"

    def test_example_ar1_absent_from_en_prompt(self):
        en = _compose_structured_system_prompt("en")
        assert "Example AR-1" not in en


# ── §6.6 — mock-LLM AR memo round-trip ──────────────────────────────


# An AR memo whose key_evidence rows carry Arabic signal/value/implication
# strings — what PR #4c expects the model to produce for lang="ar".
_AR_KEY_EVIDENCE_RESPONSE = {
    **VALID_STRUCTURED_RESPONSE,
    "headline_recommendation": "نوصي بالموقع — اقتصاديات قوية تدعم القرار",
    "key_evidence": [
        {
            "signal": "الإيجار السنوي",
            "value": "480,000 ريال سعودي/سنة",
            "implication": "أساس الدخول أدنى فعلياً من إعلانات النظراء",
            "polarity": "positive",
        },
        {
            "signal": "التقييمات على الفروع المجاورة",
            "value": "تقييم/30 يوم بمعدل مرتفع",
            "implication": "الطلب ملموس وليس مقدّراً نموذجياً",
            "polarity": "positive",
        },
    ],
}


class TestArabicKeyEvidenceRoundTrip:

    @patch("app.services.llm_decision_memo._get_client")
    def test_ar_key_evidence_persists_without_retry(self, mock_get_client):
        client_mock = _mock_client_returning(_AR_KEY_EVIDENCE_RESPONSE)
        mock_get_client.return_value = client_mock
        db = _DummyDB(preload_row=None)
        client = _endpoint_client(db)

        payload = {
            "candidate": BASE_STRUCTURED_CANDIDATE,
            "brief": BASE_STRUCTURED_BRIEF,
            "lang": "ar",
            "search_id": "search-ar-ke",
            "parcel_id": "parcel-ar-ke",
        }
        resp = client.post("/v1/expansion-advisor/decision-memo", json=payload)
        assert resp.status_code == 200, resp.text
        body = resp.json()

        # Validator accepted the Arabic key_evidence — memo persisted.
        assert body["memo_json"] is not None
        evidence = body["memo_json"]["key_evidence"]
        assert [row["signal"] for row in evidence] == [
            "الإيجار السنوي",
            "التقييمات على الفروع المجاورة",
        ]
        assert evidence[0]["value"] == "480,000 ريال سعودي/سنة"
        # No English row labels survived.
        for row in evidence:
            assert "annual rent" not in row["signal"]
            assert "ratings/30d" not in row["value"]
        # No retry fired — single LLM call.
        assert client_mock.chat.completions.create.call_count == 1
        # Persisted under the Arabic locale.
        assert db.persisted is not None
        assert db.persisted["lang"] == "ar"
        assert db.committed is True
