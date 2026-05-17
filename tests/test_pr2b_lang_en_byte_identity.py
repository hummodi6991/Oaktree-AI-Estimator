"""PR #2b — read-path English byte-identity guard (discipline rule #2).

PR #2b adds a ``lang`` parameter to ``_normalize_candidate_payload`` /
``_normalize_saved_search_payload`` and the Expansion Advisor read-path
service functions. With ``lang="en"`` or omitted, the normalized payload
must stay byte-identical to HEAD.

The golden fixtures in ``tests/fixtures/pr2b_golden/`` were captured
against HEAD before the PR #2b edits (see ``scripts/gen_pr2b_golden.py``).
The five internal ``*_structured_json`` columns — which PR #2b drops from
the outgoing payload — are stripped from the captured ``expected_en`` so
the comparison reflects the intended post-edit response shape.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.services.expansion_advisor import (
    _normalize_candidate_payload,
    _normalize_saved_search_payload,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "pr2b_golden"


def _load_fixtures() -> list[dict]:
    return [
        json.loads(p.read_text(encoding="utf-8"))
        for p in sorted(FIXTURE_DIR.glob("*.json"))
    ]


FIXTURES = _load_fixtures()
FIXTURE_IDS = [fx["id"] for fx in FIXTURES]


def _normalize(fx: dict, lang: str | None) -> dict:
    if fx["kind"] == "candidate":
        if lang is None:
            return _normalize_candidate_payload(dict(fx["candidate"]))
        return _normalize_candidate_payload(dict(fx["candidate"]), lang=lang)
    if lang is None:
        return _normalize_saved_search_payload(dict(fx["saved"]))
    return _normalize_saved_search_payload(dict(fx["saved"]), lang=lang)


def _jsonable(obj: object) -> object:
    """Round-trip through JSON so the comparison matches the fixture,
    which was itself captured via json.dumps."""
    return json.loads(json.dumps(obj, ensure_ascii=False, default=str))


def test_fixtures_present() -> None:
    assert FIXTURES, "pr2b_golden fixtures missing — run scripts/gen_pr2b_golden.py"


@pytest.mark.parametrize("fx", FIXTURES, ids=FIXTURE_IDS)
def test_omitted_lang_byte_identical(fx: dict) -> None:
    """Omitted lang reproduces the HEAD English payload byte-for-byte."""
    assert _jsonable(_normalize(fx, None)) == fx["expected_en"]


@pytest.mark.parametrize("fx", FIXTURES, ids=FIXTURE_IDS)
def test_explicit_en_byte_identical(fx: dict) -> None:
    """Explicit lang="en" reproduces the HEAD English payload byte-for-byte."""
    assert _jsonable(_normalize(fx, "en")) == fx["expected_en"]


@pytest.mark.parametrize("fx", FIXTURES, ids=FIXTURE_IDS)
def test_omitted_equals_explicit_en(fx: dict) -> None:
    """Omitted lang and explicit "en" are identical."""
    assert _jsonable(_normalize(fx, None)) == _jsonable(_normalize(fx, "en"))
