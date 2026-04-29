"""Concurrency behaviour for ``_prewarm_decision_memos``.

The pre-warm loop was changed from a strict sequential ``for`` loop to a
``ThreadPoolExecutor``-driven fan-out. These tests pin down the new
contract:

1. With concurrency > 1, all candidates complete inside a generous budget,
   in roughly ``ceil(n / workers) × per-call`` wall time rather than
   ``n × per-call``.
2. When the budget breaches mid-batch, remaining futures are cancelled
   (no crash, ``remaining`` count surfaced in the log line).
3. Per-candidate exceptions are swallowed; ``failed`` and ``skipped``
   counters are correct, batch completes.
4. Concurrency=1 reproduces sequential behaviour exactly (rollback path).
5. Each worker uses its own DB session — the outer scope never opens a
   shared session that workers reuse.
"""
from __future__ import annotations

import threading
import time
from typing import Any
from unittest.mock import MagicMock, patch

from app.core.config import settings as _ea_settings


def _baseline_monkeypatch(monkeypatch, *, top_n: int, budget_s: float, concurrency: int) -> None:
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_ENABLED", True)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_TOP_N", top_n)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_BUDGET_S", budget_s)
    monkeypatch.setattr(
        _ea_settings, "EXPANSION_MEMO_PREWARM_CONCURRENCY", concurrency
    )


def _stub_memo_helpers(monkeypatch, api_mod, gen_fn) -> list[str]:
    """Stub out the memo helpers so tests don't need a real DB or LLM.

    Returns the list that captures persisted parcel_ids in call order.
    """
    monkeypatch.setattr(api_mod, "generate_structured_memo", gen_fn)
    monkeypatch.setattr(
        api_mod, "render_structured_memo_as_text", lambda j, lang: "text"
    )
    monkeypatch.setattr(
        api_mod, "_decision_memo_cache_lookup", lambda db, sid, pid: None
    )
    writes: list[str] = []
    writes_lock = threading.Lock()

    def _record_write(db, sid, pid, t, j):
        with writes_lock:
            writes.append(pid)

    monkeypatch.setattr(api_mod, "_decision_memo_cache_write", _record_write)
    return writes


# ---------------------------------------------------------------------------
# 1. Concurrent execution completes the full batch within budget.
# ---------------------------------------------------------------------------
def test_prewarm_concurrent_completes_all_within_budget(monkeypatch):
    from app.api import expansion_advisor as api_mod

    _baseline_monkeypatch(monkeypatch, top_n=15, budget_s=10.0, concurrency=5)

    specs = [{"id": f"c{i}", "parcel_id": f"p{i}"} for i in range(1, 16)]

    def _slow_gen(ctx):
        time.sleep(0.1)
        return {"headline_recommendation": "ok"}

    writes = _stub_memo_helpers(monkeypatch, api_mod, _slow_gen)

    with patch.object(
        api_mod.db_session, "SessionLocal", return_value=MagicMock()
    ):
        wall_start = time.monotonic()
        api_mod._prewarm_decision_memos("s1", specs, {})
        wall_elapsed = time.monotonic() - wall_start

    assert sorted(writes) == sorted(s["parcel_id"] for s in specs)
    # 15 candidates / 5 workers = 3 batches × 0.1s ≈ 0.3s + scheduling.
    # Sequential would be 15 × 0.1s = 1.5s. Anything under ~1s proves
    # parallelism is real.
    assert wall_elapsed < 1.0, (
        f"prewarm wall time {wall_elapsed:.2f}s suggests sequential execution"
    )


# ---------------------------------------------------------------------------
# 2. Budget breach cancels remaining futures cleanly.
# ---------------------------------------------------------------------------
def test_prewarm_budget_breach_cancels_remaining(monkeypatch, caplog):
    from app.api import expansion_advisor as api_mod

    _baseline_monkeypatch(monkeypatch, top_n=15, budget_s=1.0, concurrency=5)

    specs = [{"id": f"c{i}", "parcel_id": f"p{i}"} for i in range(1, 16)]

    def _very_slow_gen(ctx):
        time.sleep(2.0)
        return {"headline_recommendation": "ok"}

    writes = _stub_memo_helpers(monkeypatch, api_mod, _very_slow_gen)

    with caplog.at_level("INFO", logger=api_mod.logger.name):
        with patch.object(
            api_mod.db_session, "SessionLocal", return_value=MagicMock()
        ):
            api_mod._prewarm_decision_memos("s1", specs, {})

    # At most ~1 batch of workers (5) finishes before the budget trips. We
    # cap the assertion well above 5 to absorb scheduler noise but still
    # well below 15 (sequential).
    assert len(writes) <= 10
    budget_lines = [
        r.message for r in caplog.records if "budget exhausted" in r.message
    ]
    assert len(budget_lines) == 1
    # Cancellation surfaced at least one un-started future. Workers that
    # were already in-flight will not be cancellable, which is fine.
    msg = budget_lines[0]
    assert "remaining=" in msg
    remaining_value = int(msg.split("remaining=")[1].split()[0])
    assert remaining_value >= 1


# ---------------------------------------------------------------------------
# 3. Per-candidate exception (and a None return) does not crash the batch.
# ---------------------------------------------------------------------------
def test_prewarm_per_candidate_exception_does_not_crash_batch(monkeypatch, caplog):
    from app.api import expansion_advisor as api_mod

    _baseline_monkeypatch(monkeypatch, top_n=15, budget_s=120.0, concurrency=5)

    specs = [{"id": f"c{i}", "parcel_id": f"p{i}"} for i in range(1, 16)]

    def _gen(ctx):
        if ctx.parcel_id == "p3":
            raise RuntimeError("synthetic failure")
        if ctx.parcel_id == "p5":
            return None
        return {"headline_recommendation": "ok"}

    writes = _stub_memo_helpers(monkeypatch, api_mod, _gen)

    with caplog.at_level("WARNING", logger=api_mod.logger.name):
        with patch.object(
            api_mod.db_session, "SessionLocal", return_value=MagicMock()
        ):
            api_mod._prewarm_decision_memos("s1", specs, {})

    # 13 generated (15 minus the raise and the None), no crash.
    assert len(writes) == 13
    assert "p3" not in writes  # raised
    assert "p5" not in writes  # validator returned None

    # The exception path went through logger.warning with exc_info.
    fail_records = [
        r for r in caplog.records
        if r.levelname == "WARNING"
        and "expansion_memo_prewarm fail" in r.message
        and "parcel_id=p3" in r.message
    ]
    assert len(fail_records) == 1


# ---------------------------------------------------------------------------
# 4. Concurrency=1 is sequential-equivalent (rollback path).
# ---------------------------------------------------------------------------
def test_prewarm_concurrency_one_is_sequential_equivalent(monkeypatch):
    from app.api import expansion_advisor as api_mod

    _baseline_monkeypatch(monkeypatch, top_n=3, budget_s=120.0, concurrency=1)

    specs = [{"id": f"c{i}", "parcel_id": f"p{i}"} for i in range(1, 4)]

    sleep_per_call = 0.05
    call_order: list[str] = []

    def _gen(ctx):
        call_order.append(str(ctx.parcel_id))
        time.sleep(sleep_per_call)
        return {"headline_recommendation": "ok"}

    writes = _stub_memo_helpers(monkeypatch, api_mod, _gen)

    with patch.object(
        api_mod.db_session, "SessionLocal", return_value=MagicMock()
    ):
        wall_start = time.monotonic()
        api_mod._prewarm_decision_memos("s1", specs, {})
        wall_elapsed = time.monotonic() - wall_start

    # Strict sequential ordering and write order.
    assert call_order == ["p1", "p2", "p3"]
    assert writes == ["p1", "p2", "p3"]
    # Wall time ≈ 3 × per-call sleep. A real concurrent run would be ~1×.
    assert wall_elapsed >= 3 * sleep_per_call * 0.9, (
        f"wall {wall_elapsed:.3f}s shorter than sequential lower bound — "
        f"concurrency=1 is not actually sequential"
    )


# ---------------------------------------------------------------------------
# 5. Each worker opens its own DB session.
# ---------------------------------------------------------------------------
def test_prewarm_each_worker_uses_own_db_session(monkeypatch):
    from app.api import expansion_advisor as api_mod

    _baseline_monkeypatch(monkeypatch, top_n=4, budget_s=120.0, concurrency=4)

    specs = [{"id": f"c{i}", "parcel_id": f"p{i}"} for i in range(1, 5)]

    sessions_seen: list[int] = []
    sessions_lock = threading.Lock()

    def _fake_session_local():
        m = MagicMock(name=f"Session-{len(sessions_seen)}")
        with sessions_lock:
            sessions_seen.append(id(m))
        return m

    # The lookup helper records which session id was passed in, so we can
    # verify the inner helper's session — not some smuggled outer session
    # — is what gets to the DB layer.
    seen_in_lookup: list[int] = []

    def _fake_lookup(db, sid, pid):
        with sessions_lock:
            seen_in_lookup.append(id(db))
        return None

    monkeypatch.setattr(api_mod, "_decision_memo_cache_lookup", _fake_lookup)
    monkeypatch.setattr(
        api_mod, "generate_structured_memo",
        lambda ctx: {"headline_recommendation": "ok"},
    )
    monkeypatch.setattr(
        api_mod, "render_structured_memo_as_text", lambda j, lang: "text"
    )
    monkeypatch.setattr(
        api_mod, "_decision_memo_cache_write",
        lambda db, sid, pid, t, j: None,
    )

    with patch.object(
        api_mod.db_session, "SessionLocal", side_effect=_fake_session_local
    ):
        api_mod._prewarm_decision_memos("s1", specs, {})

    # One SessionLocal() call per candidate (no shared outer session).
    assert len(sessions_seen) == 4
    # All session ids are distinct — no thread is sharing.
    assert len(set(sessions_seen)) == 4
    # Every lookup saw one of the per-worker sessions.
    assert set(seen_in_lookup).issubset(set(sessions_seen))
    assert len(seen_in_lookup) == 4
