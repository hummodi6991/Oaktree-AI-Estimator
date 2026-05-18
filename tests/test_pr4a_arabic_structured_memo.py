"""PR #4a — structured-memo Arabic end-to-end.

Covers the validator / retry / rewrite / persistence / GET changes that
make the structured decision memo produce coherent Arabic when
``lang="ar"`` is requested, while keeping the English path byte-identical
to pre-PR-4a ``main`` (the seven-rule discipline).
"""

from __future__ import annotations

import pathlib
from unittest.mock import patch

import pytest

from app.services.llm_decision_memo import (
    STRUCTURED_MEMO_SYSTEM_PROMPT,
    _ALLOWED_HEADLINE_PREFIXES_AR,
    _compose_structured_system_prompt,
    _corrective_retry_preamble,
    _headline_validity_reason,
    _rewrite_headline_locally,
    build_memo_context,
    generate_structured_memo,
)

# Reuse the established fixtures / mock helpers from the main memo suite.
from tests.test_llm_decision_memo import (
    BASE_STRUCTURED_BRIEF,
    BASE_STRUCTURED_CANDIDATE,
    VALID_STRUCTURED_RESPONSE,
    _DummyDB,
    _enable_structured_memo,
    _endpoint_client,
    _memo_with_headline,
    _mock_client_returning,
    _two_response_client,
)
from tests.test_llm_decision_memo import (
    _RANK1_ALL_PASS_CANDIDATE,
)
from tests.test_expansion_advisor_service import FakeDB, _Result


_HEAD_SNAPSHOT_PATH = (
    pathlib.Path(__file__).parent
    / "data"
    / "pr4a_structured_memo_system_prompt_en_head.txt"
)


# ── §6.1 — EN byte-identity (rule #1) ───────────────────────────────


class TestSystemPromptENByteIdentity:
    """``_compose_structured_system_prompt("en")`` must equal the
    pre-PR-4a ``STRUCTURED_MEMO_SYSTEM_PROMPT`` bytes exactly."""

    def test_en_compose_matches_head_snapshot(self):
        head = _HEAD_SNAPSHOT_PATH.read_text(encoding="utf-8")
        assert _compose_structured_system_prompt("en") == head
        # The back-compat module constant is the EN composition.
        assert STRUCTURED_MEMO_SYSTEM_PROMPT == head

    def test_ar_compose_differs_and_carries_arabic_canon(self):
        en = _compose_structured_system_prompt("en")
        ar = _compose_structured_system_prompt("ar")
        assert ar != en
        # AR branch mandates the Arabic triad, drops the English mandate.
        assert "نوصي" in ar and "نوصي مع تحفظات" in ar and "نرفض" in ar
        assert 'MUST begin with exactly one of' not in ar
        # The locale-invariant preamble is shared verbatim.
        assert ar.split("══════")[0] == en.split("══════")[0]


_EN_VALIDATOR_CASES = [
    # (headline, kwargs) — representative valid + invalid English headlines.
    ("Recommend — strong economics", dict(final_rank=1, final_score=80.0,
        overall_pass=True, blocking_failed=[])),
    ("Recommend with reservations — mixed signals", dict(final_rank=4,
        final_score=65.0, overall_pass=True, blocking_failed=[])),
    ("Decline — economics gate fails", dict(final_rank=8, final_score=50.0,
        overall_pass=False, blocking_failed=["economics"])),
    ("Consider this site", dict(final_rank=2, final_score=70.0,
        overall_pass=True, blocking_failed=[])),
    ("", dict(final_rank=1, final_score=80.0, overall_pass=True,
        blocking_failed=[])),
    ("Decline — parking fails on-site", dict(final_rank=1, final_score=85.0,
        overall_pass=True, blocking_failed=[])),
    ("Decline due to failed parking", dict(final_rank=3, final_score=60.0,
        overall_pass=True, blocking_failed=[])),
    ("Recommend — top pick", dict(final_rank=9, final_score=40.0,
        overall_pass=False, blocking_failed=[])),
]


class TestValidatorENByteIdentity:
    """The EN validation path must be unchanged: ``locale`` omitted and
    ``locale="en"`` must return identical values, and the values are the
    pre-PR-4a outcomes."""

    @pytest.mark.parametrize("headline,kwargs", _EN_VALIDATOR_CASES)
    def test_default_locale_equals_explicit_en(self, headline, kwargs):
        no_arg = _headline_validity_reason(headline, **kwargs)
        explicit = _headline_validity_reason(headline, locale="en", **kwargs)
        assert no_arg == explicit

    def test_en_known_outcomes(self):
        out = {
            h: _headline_validity_reason(h, **k) for h, k in _EN_VALIDATOR_CASES
        }
        assert out["Recommend — strong economics"] is None
        assert out["Recommend with reservations — mixed signals"] is None
        assert out["Decline — economics gate fails"] is None
        assert out["Consider this site"] is not None
        assert out[""] == "headline missing or empty"
        # rank-1, score>=70, no blocking → a Decline headline is rejected.
        assert "Decline" in out["Decline — parking fails on-site"]
        # Confabulated gate failure (gates.failed empty).
        assert "failed gates" in out["Decline due to failed parking"]
        # overall_pass=False → a Recommend headline is rejected.
        assert "Decline headline" in out["Recommend — top pick"]


class TestRewriteENByteIdentity:
    """``_rewrite_headline_locally`` EN path unchanged."""

    @pytest.mark.parametrize("original,kwargs", [
        ({"ranking_explanation": "Strong rent advantage and corner frontage."},
         dict(final_rank=1, final_score=82.0, overall_pass=True,
              blocking_failed=[])),
        ({"ranking_explanation": "Economics gate fails badly."},
         dict(final_rank=8, final_score=44.0, overall_pass=False,
              blocking_failed=["economics"])),
        ({"headline_recommendation": "consider due to mixed signals",
          "ranking_explanation": "Mixed but workable."},
         dict(final_rank=4, final_score=65.0, overall_pass=True,
              blocking_failed=[])),
    ])
    def test_default_locale_equals_explicit_en(self, original, kwargs):
        assert (
            _rewrite_headline_locally(original, **kwargs)
            == _rewrite_headline_locally(original, locale="en", **kwargs)
        )

    def test_en_prefixes(self):
        rec = _rewrite_headline_locally(
            {"ranking_explanation": "Strong rent advantage."},
            final_rank=1, final_score=82.0, overall_pass=True,
            blocking_failed=[],
        )
        assert rec.startswith("Recommend")
        dec = _rewrite_headline_locally(
            {"ranking_explanation": "Economics gate fails."},
            final_rank=8, final_score=44.0, overall_pass=False,
            blocking_failed=["economics"],
        )
        assert dec.startswith("Decline")


_EXPECTED_EN_PREAMBLE = (
    "PREVIOUS RESPONSE WAS REJECTED. Reason: SOME_REASON.\n\n"
    "The headline_recommendation field must follow the format "
    "rules exactly. Do not begin with \"Consider\" or any "
    "other word outside the three allowed prefixes "
    "(\"Recommend\", \"Recommend with reservations\", "
    "\"Decline\"). Do not cite gate failures unless they "
    "appear in gates.failed. Re-emit the full structured "
    "memo with a corrected headline."
)


class TestCorrectiveRetryPreambleENByteIdentity:
    def test_en_preamble_matches_head_text(self):
        assert (
            _corrective_retry_preamble("en", "SOME_REASON")
            == _EXPECTED_EN_PREAMBLE
        )


# ── §6.2 — AR validator (new) ───────────────────────────────────────


class TestHeadlineValidityArabic:
    _CTX = dict(
        final_rank=None, final_score=None, overall_pass=None,
        blocking_failed=[],
    )

    def test_valid_recommend(self):
        assert _headline_validity_reason(
            "نوصي بالموقع", locale="ar", **self._CTX) is None

    def test_valid_recommend_with_reservations(self):
        assert _headline_validity_reason(
            "نوصي مع تحفظات بشأن المنافسة", locale="ar", **self._CTX) is None

    def test_valid_decline(self):
        assert _headline_validity_reason(
            "نرفض الموقع", locale="ar", **self._CTX) is None

    def test_english_headline_in_ar_locale_is_invalid(self):
        reason = _headline_validity_reason(
            "Recommend the location", locale="ar", **self._CTX)
        assert reason is not None and "allowed prefix" in reason

    def test_unknown_prefix_is_invalid(self):
        reason = _headline_validity_reason(
            "بسم الله", locale="ar", **self._CTX)
        assert reason is not None and "allowed prefix" in reason

    def test_ar_prefix_canon_is_longest_first(self):
        assert _ALLOWED_HEADLINE_PREFIXES_AR[0] == "نوصي مع تحفظات"

    def test_rank1_high_score_decline_rejected(self):
        reason = _headline_validity_reason(
            "نرفض الموقع", locale="ar", final_rank=1, final_score=85.0,
            overall_pass=True, blocking_failed=[])
        assert reason is not None and "Decline headline" in reason

    def test_overall_pass_false_recommend_rejected(self):
        reason = _headline_validity_reason(
            "نوصي بالموقع", locale="ar", final_rank=9, final_score=40.0,
            overall_pass=False, blocking_failed=[])
        assert reason is not None and "Decline headline" in reason

    def test_confabulated_gate_failure_rejected(self):
        # "فشل" = the Arabic failure marker; gates.failed empty → confabulation.
        reason = _headline_validity_reason(
            "نرفض بسبب فشل بوابة الموقف", locale="ar", final_rank=None,
            final_score=None, overall_pass=None, blocking_failed=[])
        assert reason is not None and "failed gates" in reason


# ── §6.3 — AR retry / rewrite (new) ─────────────────────────────────


class TestArabicRetryAndRewrite:

    @patch("app.services.llm_decision_memo._get_client")
    def test_retry_recovers_arabic_headline(self, mock_get_client, monkeypatch):
        _enable_structured_memo(monkeypatch)
        # First attempt: English headline in an AR memo → rejected.
        bad_first = _memo_with_headline("Recommend — strong economics")
        # Retry returns a valid Arabic headline.
        good_second = _memo_with_headline("نوصي — اقتصاديات قوية في حي مستقر")
        mock_get_client.return_value = _two_response_client(bad_first, good_second)

        ctx = build_memo_context(
            candidate=_RANK1_ALL_PASS_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="ar",
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        assert memo["headline_recommendation"].startswith("نوصي")
        # Retry succeeded → body preserved (not nulled).
        assert memo["ranking_explanation"] != ""
        assert mock_get_client.return_value.chat.completions.create.call_count == 2

    @patch("app.services.llm_decision_memo._get_client")
    def test_local_rewrite_stamps_arabic_prefix_and_nulls_body(
        self, mock_get_client, monkeypatch
    ):
        _enable_structured_memo(monkeypatch)
        # Both attempts return English headlines → AR validator rejects both.
        bad_first = _memo_with_headline("Recommend — strong economics")
        bad_second = _memo_with_headline("Recommend — still English")
        mock_get_client.return_value = _two_response_client(bad_first, bad_second)

        ctx = build_memo_context(
            candidate=_RANK1_ALL_PASS_CANDIDATE,
            brief=BASE_STRUCTURED_BRIEF,
            lang="ar",
        )
        memo = generate_structured_memo(ctx)

        assert memo is not None
        # rank-1, score>=70, no blocking → Arabic "نوصي" prefix.
        assert memo["headline_recommendation"].startswith("نوصي")
        # Body nulled on the local-rewrite safety-net path (both locales).
        assert memo["ranking_explanation"] == ""
        assert memo["key_evidence"] == []
        assert memo["risks"] == []
        assert memo["comparison"] == ""
        assert memo["bottom_line"] == ""
        assert mock_get_client.return_value.chat.completions.create.call_count == 2


# ── §6.4 — AR end-to-end (new) ──────────────────────────────────────


_AR_STRUCTURED_RESPONSE = {
    **VALID_STRUCTURED_RESPONSE,
    "headline_recommendation": "نوصي بالموقع — اقتصاديات قوية تدعم القرار",
}


class TestArabicEndToEnd:

    @patch("app.services.llm_decision_memo._get_client")
    def test_post_decision_memo_ar_persists_arabic(self, mock_get_client):
        mock_get_client.return_value = _mock_client_returning(
            _AR_STRUCTURED_RESPONSE
        )
        db = _DummyDB(preload_row=None)
        client = _endpoint_client(db)

        payload = {
            "candidate": BASE_STRUCTURED_CANDIDATE,
            "brief": BASE_STRUCTURED_BRIEF,
            "lang": "ar",
            "search_id": "search-ar",
            "parcel_id": "parcel-ar",
        }
        resp = client.post("/v1/expansion-advisor/decision-memo", json=payload)
        assert resp.status_code == 200, resp.text
        body = resp.json()

        assert body["memo_json"] is not None
        assert body["memo_json"]["headline_recommendation"].startswith(
            ("نوصي", "نرفض")
        )
        # Rendered text carries the Arabic section headers.
        assert "## التوصية الرئيسية" in body["memo_text"]
        # Persisted row records the Arabic locale (PR #4a column).
        assert db.persisted is not None
        assert db.persisted["lang"] == "ar"
        assert db.committed is True


# ── §6.5 — persistence / regenerate-on-mismatch (new) ───────────────


class _RecordingDB(FakeDB):
    """FakeDB that records UPDATE params and supports commit/rollback so
    the regenerate-on-mismatch path can be exercised."""

    def __init__(self, memo_row):
        super().__init__(memo_row=memo_row)
        self.updates: list[dict] = []
        self.committed = False

    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        if "UPDATE expansion_candidate" in sql:
            self.updates.append(dict(params or {}))
            return _Result([])
        return super().execute(stmt, params)

    def commit(self):
        self.committed = True

    def rollback(self):
        pass


def _memo_row(decision_memo_lang):
    return {
        "candidate_id": "c-regen",
        "search_id": "s-regen",
        "brand_name": "Brand X",
        "category": "burger",
        "service_model": "qsr",
        "parcel_id": "p-regen",
        "district": "Olaya",
        "area_m2": 180,
        "final_score": 82,
        "economics_score": 70,
        "cannibalization_score": 35,
        "confidence_grade": "A",
        "rank_position": 1,
        "deterministic_rank": 1,
        "final_rank": 1,
        "rerank_applied": False,
        "rerank_reason": None,
        "rerank_delta": 0,
        "rerank_status": "flag_off",
        "decision_memo": "## Headline Recommendation\nRecommend",
        "decision_memo_json": dict(VALID_STRUCTURED_RESPONSE),
        "decision_memo_lang": decision_memo_lang,
    }


def _import_service():
    from app.services import expansion_advisor as svc
    return svc


class TestRegenerateOnMismatch:

    def test_en_memo_en_request_no_regeneration(self):
        svc = _import_service()
        db = _RecordingDB(_memo_row("en"))
        with patch(
            "app.services.llm_decision_memo.generate_structured_memo"
        ) as gen:
            memo = svc.get_candidate_memo(db, "c-regen", lang="en")
        assert memo is not None
        gen.assert_not_called()
        assert db.updates == []

    def test_en_memo_ar_request_triggers_regeneration(self):
        svc = _import_service()
        db = _RecordingDB(_memo_row("en"))
        ar_json = {
            **VALID_STRUCTURED_RESPONSE,
            "headline_recommendation": "نوصي بالموقع — اقتصاديات قوية",
        }
        with patch(
            "app.services.llm_decision_memo.generate_structured_memo",
            return_value=ar_json,
        ) as gen:
            memo = svc.get_candidate_memo(db, "c-regen", lang="ar")
        assert memo is not None
        gen.assert_called_once()
        # Response carries the freshly-generated Arabic memo.
        assert memo["decision_memo_json"]["headline_recommendation"].startswith(
            "نوصي"
        )
        assert "## التوصية الرئيسية" in memo["decision_memo"]
        # Persisted with decision_memo_lang = "ar".
        assert len(db.updates) == 1
        assert db.updates[0]["lang"] == "ar"
        assert db.committed is True

    def test_null_lang_en_request_no_regeneration(self):
        """Pre-PR-4a row (decision_memo_lang IS NULL) — back-compat path."""
        svc = _import_service()
        db = _RecordingDB(_memo_row(None))
        with patch(
            "app.services.llm_decision_memo.generate_structured_memo"
        ) as gen:
            memo = svc.get_candidate_memo(db, "c-regen", lang="en")
        assert memo is not None
        gen.assert_not_called()
        assert db.updates == []

    def test_null_lang_ar_request_triggers_regeneration(self):
        svc = _import_service()
        db = _RecordingDB(_memo_row(None))
        ar_json = {
            **VALID_STRUCTURED_RESPONSE,
            "headline_recommendation": "نرفض الموقع — عبء الإيجار مرتفع",
        }
        with patch(
            "app.services.llm_decision_memo.generate_structured_memo",
            return_value=ar_json,
        ) as gen:
            memo = svc.get_candidate_memo(db, "c-regen", lang="ar")
        assert memo is not None
        gen.assert_called_once()
        assert len(db.updates) == 1
        assert db.updates[0]["lang"] == "ar"

    def test_regeneration_unavailable_serves_existing_memo(self):
        """When structured generation returns None (flag off / LLM error),
        the persisted memo is served unchanged — no crash, no update."""
        svc = _import_service()
        db = _RecordingDB(_memo_row("en"))
        with patch(
            "app.services.llm_decision_memo.generate_structured_memo",
            return_value=None,
        ):
            memo = svc.get_candidate_memo(db, "c-regen", lang="ar")
        assert memo is not None
        # Falls back to the persisted English memo.
        assert memo["decision_memo_json"] == dict(VALID_STRUCTURED_RESPONSE)
        assert db.updates == []


# ── §6.6 — migration ────────────────────────────────────────────────


class TestDecisionMemoLangMigration:
    """Static verification of the additive migration. A live up/down/up
    cycle needs a PostgreSQL instance, unavailable in this environment;
    instead the migration module is imported and its upgrade/downgrade
    bodies are executed against a mocked ``op`` so the exact column spec
    is asserted."""

    def _load(self):
        import importlib.util
        path = (
            pathlib.Path(__file__).parent.parent
            / "alembic" / "versions" / "20260519_decision_memo_lang.py"
        )
        spec = importlib.util.spec_from_file_location("_mig_4a", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_revision_chain(self):
        mod = self._load()
        assert mod.revision == "20260519_decision_memo_lang"
        assert len(mod.revision) <= 32
        assert mod.down_revision == "20260518_ea_strengths_risks"

    def test_upgrade_adds_nullable_varchar8_column(self):
        mod = self._load()
        captured = {}

        def fake_add_column(table, column):
            captured["table"] = table
            captured["column"] = column

        with patch.object(mod.op, "add_column", side_effect=fake_add_column):
            mod.upgrade()

        assert captured["table"] == "expansion_candidate"
        col = captured["column"]
        assert col.name == "decision_memo_lang"
        assert col.nullable is True
        assert col.default is None
        assert col.server_default is None
        assert isinstance(col.type, mod.sa.String)
        assert col.type.length == 8

    def test_downgrade_drops_column(self):
        mod = self._load()
        dropped = {}

        def fake_drop_column(table, column):
            dropped["table"] = table
            dropped["column"] = column

        with patch.object(mod.op, "drop_column", side_effect=fake_drop_column):
            mod.downgrade()

        assert dropped["table"] == "expansion_candidate"
        assert dropped["column"] == "decision_memo_lang"
