"""Regression tests for PR3 — cross-portal REGA-license dedup at the
candidate_location stage.

These tests pin the SQL shape and model contract that the dedup logic
relies on. They do NOT exercise a live PostGIS database — the existing
pattern in tests/test_commercial_unit_platform_columns.py asserts on
the emitted SQL strings, and that pattern is reused here.

PR3 invariants pinned:

1. ``candidate_location.rega_advertisement_license`` exists as
   ``String(64)`` nullable with a partial index on the non-NULL subset.
2. ``_ingest_tier1_aqar`` projects ``cu.aqar_advertisement_license`` into
   the new column for every Tier 1 row.
3. ``_run_deduplication`` emits the REGA-license collapse pass with the
   correct normalisation (LOWER+TRIM), the Aqar-wins tiebreak ordering,
   and the JOIN to ``commercial_unit`` for ``last_seen_at``.
4. ``_run_deduplication`` emits the fingerprint fallback with 25 m +
   ±5% area + ±5% rent thresholds, NULL/zero guards, and connected-
   component resolution.
5. The pre-existing Step 1 "mark every Tier 1 row primary" UPDATE is
   preserved unchanged — both new passes only DEMOTE rows.
6. The dedup SQL uses no clock or random functions (idempotency).
"""

from __future__ import annotations

import re
from typing import Any


class _FakeResult:
    def __init__(self, scalar_value: Any = None, rowcount: int = 0) -> None:
        self._scalar = scalar_value
        self.rowcount = rowcount

    def scalar(self) -> Any:
        return self._scalar


class _RecordingDB:
    """Records every ``execute`` call. Returns a generic result so the
    dedup function can run all four UPDATEs end-to-end.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, stmt, params=None):
        self.calls.append((str(stmt), dict(params or {})))
        # Count primaries probe returns 0; nothing reads it.
        return _FakeResult(scalar_value=0, rowcount=0)

    def commit(self) -> None:
        pass


# ---------------------------------------------------------------------------
# 1. Model shape
# ---------------------------------------------------------------------------


class TestCandidateLocationModel:
    def test_has_rega_advertisement_license_column(self):
        from app.models.tables import CandidateLocation

        cols = CandidateLocation.__table__.columns
        assert "rega_advertisement_license" in cols
        col = cols["rega_advertisement_license"]
        assert col.nullable is True
        assert col.type.length == 64

    def test_partial_index_on_rega_license_exists(self):
        from app.models.tables import CandidateLocation

        indexes = {ix.name: ix for ix in CandidateLocation.__table__.indexes}
        ix = indexes.get("ix_candidate_location_rega_license")
        assert ix is not None
        assert ix.unique is False
        assert [c.name for c in ix.columns] == ["rega_advertisement_license"]
        where_sql = str(
            ix.dialect_options.get("postgresql", {}).get("where", "")
        )
        assert "rega_advertisement_license IS NOT NULL" in where_sql


# ---------------------------------------------------------------------------
# 2. Ingest projection
# ---------------------------------------------------------------------------


class TestIngestTier1AqarProjection:
    def test_insert_projects_rega_license_from_commercial_unit(self):
        from app.ingest.candidate_locations import _ingest_tier1_aqar

        db = _RecordingDB()
        _ingest_tier1_aqar(db, run_id="abcd1234")

        assert db.calls, "ingest must emit at least one SQL statement"
        insert_sql, params = db.calls[0]
        assert "INSERT INTO candidate_location" in insert_sql
        # Column list must include the new column.
        assert "rega_advertisement_license" in insert_sql
        # SELECT projection must source it from commercial_unit's column.
        assert "cu.aqar_advertisement_license" in insert_sql
        assert params["run_id"] == "abcd1234"


# ---------------------------------------------------------------------------
# 3. Dedup SQL — structure
# ---------------------------------------------------------------------------


def _collect_dedup_sql(run_id: str = "deadbeef") -> list[str]:
    from app.ingest.candidate_locations import _run_deduplication

    db = _RecordingDB()
    _run_deduplication(db, run_id=run_id)
    return [sql for sql, _ in db.calls]


class TestRunDeduplicationStructure:
    def test_dbscan_step_unchanged(self):
        sqls = _collect_dedup_sql()
        # First statement is the DBSCAN clustering UPDATE.
        assert "ST_ClusterDBSCAN" in sqls[0]
        assert "eps := 0.00045" in sqls[0]
        assert "minpoints := 1" in sqls[0]

    def test_step1_marks_all_tier1_primary(self):
        sqls = _collect_dedup_sql()
        # Step 1 is the unconditional Tier-1-primary UPDATE.
        step1 = next(
            s for s in sqls
            if "SET is_cluster_primary = TRUE" in s
            and "source_tier = 1" in s
            and "ST_ClusterDBSCAN" not in s
            and "license_groups" not in s
            and "fingerprint_pairs" not in s
        )
        assert "WHERE population_run_id = :run_id" in step1
        assert "AND source_tier = 1" in step1


class TestRegaLicenseCollapsePass:
    def setup_method(self):
        self.sqls = _collect_dedup_sql()
        self.rega_sql = next(s for s in self.sqls if "license_groups" in s)

    def test_filters_to_tier1_with_non_null_non_blank_license(self):
        assert "cl.source_tier = 1" in self.rega_sql
        assert "cl.rega_advertisement_license IS NOT NULL" in self.rega_sql
        assert "TRIM(cl.rega_advertisement_license) <> ''" in self.rega_sql

    def test_partitions_by_normalised_license(self):
        # Both PARTITION BY clauses (in COUNT and ROW_NUMBER) must normalise.
        normalised = "LOWER(TRIM(cl.rega_advertisement_license))"
        assert self.rega_sql.count(normalised) >= 2

    def test_tiebreak_order_aqar_first_then_last_seen_then_id(self):
        # Order must be: Aqar-vs-non-Aqar, then last_seen_at DESC, then id ASC.
        m = re.search(
            r"ORDER BY\s+\(cl\.source_type = 'aqar'\) DESC,\s*"
            r"cu\.last_seen_at DESC NULLS LAST,\s*"
            r"cl\.id ASC",
            self.rega_sql,
        )
        assert m is not None, "REGA tiebreak order must be aqar→last_seen→id"

    def test_joins_commercial_unit_via_platform_listing_id(self):
        # JOIN predicate must be (platform, platform_listing_id) so it works
        # for both Aqar today and Bayut in PR4.
        assert "LEFT JOIN commercial_unit cu" in self.rega_sql
        assert "cu.platform = cl.source_type" in self.rega_sql
        assert "cu.platform_listing_id = cl.source_id" in self.rega_sql

    def test_demotes_non_primary_rows_in_groups_of_size_gt_1(self):
        assert "SET is_cluster_primary = FALSE" in self.rega_sql
        assert "lg.group_size > 1" in self.rega_sql
        assert "lg.rn > 1" in self.rega_sql

    def test_scoped_to_run_id(self):
        assert "cl.population_run_id = :run_id" in self.rega_sql


class TestFingerprintFallbackPass:
    def setup_method(self):
        self.sqls = _collect_dedup_sql()
        self.fp_sql = next(s for s in self.sqls if "fingerprint_pairs" in s)

    def test_distance_threshold_is_25_metres(self):
        assert "ST_DWithin(cl1.geom::geography, cl2.geom::geography, 25.0)" in self.fp_sql

    def test_area_threshold_5_percent_with_null_and_zero_guards(self):
        assert "cl1.area_sqm IS NOT NULL" in self.fp_sql
        assert "cl2.area_sqm IS NOT NULL" in self.fp_sql
        assert "cl1.area_sqm > 0" in self.fp_sql
        assert "cl2.area_sqm > 0" in self.fp_sql
        assert (
            "ABS(cl1.area_sqm - cl2.area_sqm)\n"
            "                   / GREATEST(cl1.area_sqm, cl2.area_sqm) <= 0.05"
        ) in self.fp_sql

    def test_rent_threshold_5_percent_with_null_and_zero_guards(self):
        assert "cl1.rent_sar_annual IS NOT NULL" in self.fp_sql
        assert "cl2.rent_sar_annual IS NOT NULL" in self.fp_sql
        assert "cl1.rent_sar_annual > 0" in self.fp_sql
        assert "cl2.rent_sar_annual > 0" in self.fp_sql
        assert (
            "ABS(cl1.rent_sar_annual - cl2.rent_sar_annual)\n"
            "                   / GREATEST(cl1.rent_sar_annual, cl2.rent_sar_annual) <= 0.05"
        ) in self.fp_sql

    def test_self_match_and_symmetric_pair_guard(self):
        assert "cl1.id < cl2.id" in self.fp_sql

    def test_only_fires_when_at_least_one_side_has_no_rega_license(self):
        # The REGA pass owns same-license pairs; the fingerprint fallback
        # only fires when at least one side has a NULL/blank license.
        assert "cl1.rega_advertisement_license IS NULL" in self.fp_sql
        assert "cl2.rega_advertisement_license IS NULL" in self.fp_sql

    def test_only_considers_currently_primary_rows(self):
        # Step 1a may have already demoted rows; don't second-guess it.
        assert "cl1.is_cluster_primary = TRUE" in self.fp_sql
        assert "cl2.is_cluster_primary = TRUE" in self.fp_sql

    def test_connected_component_resolution(self):
        # Recursive CTE → MIN(root) per node → ROW_NUMBER per component.
        assert "WITH " in self.fp_sql or "components AS" in self.fp_sql
        assert "components AS" in self.fp_sql
        assert "component_root" in self.fp_sql
        assert "MIN(root) AS root_id" in self.fp_sql

    def test_fingerprint_tiebreak_order(self):
        m = re.search(
            r"ORDER BY\s+\(cl\.source_type = 'aqar'\) DESC,\s*"
            r"cu\.last_seen_at DESC NULLS LAST,\s*"
            r"cl\.id ASC",
            self.fp_sql,
        )
        assert m is not None, "fingerprint tiebreak must be aqar→last_seen→id"

    def test_demotes_non_winners_in_components_of_size_gt_1(self):
        assert "SET is_cluster_primary = FALSE" in self.fp_sql
        assert "r.component_size > 1" in self.fp_sql
        assert "r.rn > 1" in self.fp_sql


# ---------------------------------------------------------------------------
# 4. Idempotency
# ---------------------------------------------------------------------------


class TestDedupIdempotency:
    def test_dedup_sql_has_no_clock_or_random_functions(self):
        """No NOW(), CURRENT_TIMESTAMP, RANDOM(): two runs over identical
        data must produce identical results.
        """
        sqls = _collect_dedup_sql()
        joined = "\n".join(sqls).upper()
        assert "NOW()" not in joined
        assert "CURRENT_TIMESTAMP" not in joined
        assert "RANDOM(" not in joined

    def test_two_consecutive_runs_emit_identical_sql(self):
        first = _collect_dedup_sql(run_id="run-1")
        second = _collect_dedup_sql(run_id="run-1")
        # SQL text must be byte-identical across runs (deterministic plan).
        assert first == second


# ---------------------------------------------------------------------------
# 5. Today's reality: Aqar-only data, unique licenses → no demotions
# ---------------------------------------------------------------------------


class TestPrePR3InvariantPreserved:
    def test_step1_still_promotes_every_tier1_first(self):
        """The pre-PR3 invariant — "every Tier 1 row starts as primary" —
        is preserved. The new passes only DEMOTE rows. So against today's
        Aqar-only data (no Bayut, no shared licenses), the dedup is a
        no-op on the primary set."""
        sqls = _collect_dedup_sql()

        # Find the "mark every Tier 1 primary" step.
        promote_idx = next(
            i for i, s in enumerate(sqls)
            if "SET is_cluster_primary = TRUE" in s
            and "AND source_tier = 1" in s
            and "ST_ClusterDBSCAN" not in s
            and "license_groups" not in s
            and "fingerprint_pairs" not in s
        )

        # All subsequent Tier 1 mutations are DEMOTIONS only.
        for s in sqls[promote_idx + 1:]:
            if "source_tier" not in s and "license_groups" not in s and "fingerprint_pairs" not in s:
                continue
            if "is_cluster_primary" in s and (
                "license_groups" in s or "fingerprint_pairs" in s
            ):
                assert "SET is_cluster_primary = FALSE" in s
