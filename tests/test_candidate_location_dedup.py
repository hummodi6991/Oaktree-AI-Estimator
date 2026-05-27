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

    def test_source_type_projected_from_cu_platform_not_hardcoded(self):
        """PR5: ``source_type`` must come from ``cu.platform`` so Bayut rows
        receive ``source_type='bayut'`` (not the pre-PR5 hardcoded literal
        ``'aqar'``). The dynamic projection is what activates the
        cross-portal dedup ladder in ``_run_deduplication``.

        Behavior implication (verified at the SQL-shape layer here; a live-
        PG integration variant would insert one Bayut and one Aqar row,
        run the function, and assert the resulting ``candidate_location``
        rows carry the corresponding ``source_type``):
          * commercial_unit row with platform='bayut' → source_type='bayut'
          * commercial_unit row with platform='aqar'  → source_type='aqar'
        """
        from app.ingest.candidate_locations import _ingest_tier1_aqar

        db = _RecordingDB()
        _ingest_tier1_aqar(db, run_id="zzz")
        insert_sql, _ = db.calls[0]

        # Projection now emits ``cu.platform`` in the source_type slot.
        assert "1, cu.platform, cu.aqar_id" in insert_sql, (
            "SELECT must project cu.platform (not a hardcoded literal) "
            "into candidate_location.source_type"
        )
        # The pre-PR5 broken literal must not appear in the projection.
        assert "1, 'aqar', cu.aqar_id" not in insert_sql


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
        # Pre-PR5 the ``(cl.source_type = 'aqar') DESC`` predicate was a
        # no-op — every Tier 1 row had source_type='aqar' hardcoded by
        # ``_ingest_tier1_aqar``, so the predicate evaluated TRUE for
        # every row and the tier-break reduced to last_seen_at→id.
        # Post-PR5 the projection emits ``cu.platform`` (real values
        # ``'aqar'``/``'bayut'``), so this predicate becomes the actual
        # cross-portal tier-break: same-license Bayut rows lose to
        # same-license Aqar rows.
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
        assert "WITH RECURSIVE" in self.fp_sql
        assert "components AS" in self.fp_sql
        assert "component_root" in self.fp_sql
        assert "MIN(root) AS root_id" in self.fp_sql

    def test_fingerprint_tiebreak_order(self):
        # Same pre/post-PR5 nuance as the REGA pass: pre-PR5 the
        # ``(cl.source_type = 'aqar') DESC`` predicate evaluated TRUE
        # for every row (hardcoded literal), making it a no-op.
        # Post-PR5 the projection emits ``cu.platform``, so this is the
        # real cross-portal tier-break inside a fingerprint component.
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
# 5. Promote-then-demote-only invariant (pure-Aqar pool → no demotions)
# ---------------------------------------------------------------------------


class TestDedupPreservesAqarOnlyWithinPortalOnPureAqarPool:
    """Renamed from ``TestPrePR3InvariantPreserved``: the underlying
    SQL-shape assertion is unchanged but the *meaning* changes post-PR5.

    Pre-PR5 the invariant "today's pool is Aqar-only and licenses are
    unique, so dedup is a no-op" was guaranteed by data. Post-PR5 the
    pool contains real Bayut rows and the dedup actively collapses
    cross-portal duplicates, so that data-level invariant no longer
    holds.

    What this test still pins is the structural invariant: every Tier-1
    row is promoted to primary in Step 1, and every subsequent Tier-1
    mutation is a DEMOTION (``SET is_cluster_primary = FALSE``), never a
    promotion. That guarantees a pure-Aqar pool with unique licenses
    remains a no-op (REGA group_size>1 and fingerprint component_size>1
    never fire), even though the dedup is now active for mixed pools.
    """

    def test_step1_still_promotes_every_tier1_first(self):
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


# ---------------------------------------------------------------------------
# 6. PR5: cross-portal and within-portal dedup activation
# ---------------------------------------------------------------------------


class TestCrossPortalDedupActivation:
    """PR5 activates the cross-portal collapse by emitting real ``cu.platform``
    values into ``candidate_location.source_type``. The dedup SQL ladder
    itself does not change — these tests pin the SQL shape that produces
    the now-correct behavior.

    Behavioral expectations (verified by SQL shape; a live-PG variant
    would actually INSERT rows and assert on the resulting state):

    * test_cross_portal_dedup_collapses_same_license: insert one Aqar
      and one Bayut row sharing a REGA license → only one primary
      survives, and per the ``(source_type='aqar') DESC`` tiebreak it
      is the Aqar row.
    * test_within_portal_bayut_dedup_collapses_same_license: insert two
      Bayut rows sharing a REGA license → one primary, one demoted
      (same license group, partitioned by normalized license alone, not
      by platform).
    """

    def setup_method(self):
        self.sqls = _collect_dedup_sql()
        self.rega_sql = next(s for s in self.sqls if "license_groups" in s)

    def test_license_partition_does_not_filter_by_platform(self):
        # The PARTITION BY is on the normalised license alone — not
        # platform. So a same-license pair collapses regardless of
        # whether both rows are Aqar, both Bayut, or one of each.
        normalised = "LOWER(TRIM(cl.rega_advertisement_license))"
        assert self.rega_sql.count(normalised) >= 2
        # Sanity: no platform filter sneaks into the partition.
        # (We assert on the PARTITION BY clauses specifically; the
        # tier-break expression ``(cl.source_type = 'aqar') DESC`` lives
        # inside the ORDER BY, not the PARTITION BY.)
        for partition_match in re.finditer(
            r"PARTITION BY\s+([^\)]*?)\s+ORDER BY", self.rega_sql
        ):
            partition_expr = partition_match.group(1)
            assert "cl.source_type" not in partition_expr
            assert "cu.platform" not in partition_expr

    def test_cross_portal_dedup_collapses_same_license(self):
        # Cross-portal collapse: with real source_type values, a pair
        # (Aqar, Bayut) sharing a license falls in the same partition
        # and the Bayut row is demoted by the tier-break.
        assert "cu.platform = cl.source_type" in self.rega_sql
        assert "cu.platform_listing_id = cl.source_id" in self.rega_sql
        # The Aqar-wins tier-break must be present.
        assert re.search(
            r"\(cl\.source_type = 'aqar'\) DESC", self.rega_sql,
        ) is not None
        # Demotion fires when group has more than one row.
        assert "lg.group_size > 1" in self.rega_sql
        assert "lg.rn > 1" in self.rega_sql
        # Demotion targets non-winners only (rn > 1).
        assert "SET is_cluster_primary = FALSE" in self.rega_sql

    def test_within_portal_bayut_dedup_collapses_same_license(self):
        # Within-portal collapse: two Bayut rows sharing a license fall
        # in the same license partition. Neither matches the Aqar-wins
        # tier-break, so the second-place tier-break (``cu.last_seen_at
        # DESC NULLS LAST, cl.id ASC``) decides the winner. SQL shape
        # is identical to the cross-portal case — only the data differs.
        # Pin the tier-break sequence end-to-end:
        m = re.search(
            r"ORDER BY\s+\(cl\.source_type = 'aqar'\) DESC,\s*"
            r"cu\.last_seen_at DESC NULLS LAST,\s*"
            r"cl\.id ASC",
            self.rega_sql,
        )
        assert m is not None
        # And the demotion gate uses group_size>1 → fires for any
        # license group with more than one row, regardless of platform.
        assert "lg.group_size > 1" in self.rega_sql
