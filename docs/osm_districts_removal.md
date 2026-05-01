# osm_districts layer removal

Removed in alembic revision `20260501b_drop_osm_districts`.

## Why

Investigation 2026-05-01 found the `osm_districts` layer in
`external_feature` was contaminated with non-Riyadh data:
- 9 of 12 polygons resolved to districts of Reghin, Romania (Cartierul
  Făgărașului, Cartierul Gării, Maurer Residence, etc.). They envelope-
  intersected the Riyadh bbox in the OSM ingest's `ST_Intersects` filter
  but covered no Riyadh territory.
- The 3 Arabic-labeled OSM rows used a different naming convention from
  Aqar (`وادي` valley-prefix vs `حي` neighborhood-prefix), producing
  `labels_disagree` for every overlapping listing.
- Across 1845 active commercial_units, OSM and Aqar **never agreed** on a
  district label.

## Impact

- District resolution: 15 of 1845 active listings (0.81%) had been
  receiving OSM-derived labels (mostly Romanian); they now receive Aqar
  labels.
- Map tiles (`app/api/tiles.py`): visible district labels switched from
  OSM `name` (sometimes Romanian) to Aqar `district_raw` (Arabic) +
  `district_en` (English where the crosswalk covers it).
- Search autocomplete (`app/api/search.py`): duplicate hits for the same
  district eliminated.
- `_district_momentum_score` and other district-keyed signals: now
  consistent for the formerly-OSM-resolved 15 listings.

## Recovery

Re-introducing OSM is **not** a clean revert. The upstream leakage at
`app/ingest/osm_district_polygons.py` (deleted in this PR) was not
addressed. Recovery requires:

1. Re-create a working OSM ingest with a corrected filter (e.g. tag-based
   exclusion of non-Saudi polygons, not just bbox `ST_Intersects`).
2. Re-run that ingest.
3. Re-add `osm_districts` to the matview definition + WHERE clauses
   across the consumers listed below.

Consumer sites updated by this PR:
- `app/services/expansion_advisor.py` — `_district_momentum_score`,
  `_build_district_lookup`
- `app/ingest/expansion_advisor_district_labels.py`
- `app/ingest/candidate_locations.py` — `_resolve_districts`
- `app/ingest/black_marble_radiance.py` — `_load_district_polygons`
- `app/api/tiles.py`
- `app/api/search.py`
- `app/api/expansion_advisor.py` — districts endpoint
- `app/services/district_resolver.py`
- `tests/test_expansion_advisor_regression.py`
- `tests/test_expansion_districts_api.py`

## Post-merge checklist

1. **Deploy** normally — alembic upgrade runs the migration, OSM rows
   delete, matview rebuilds without OSM. The "Refresh
   external_feature_polygons_mat" workflow does **not** need to be
   triggered; the migration recreates the matview `WITH DATA`.

2. **Refresh `search_index_mat` manually**. The 2026-02-02 migration
   pre-baked a UNION that included `osm_districts` rows from
   `external_feature`. After this PR's migration deletes those rows, the
   matview will still return stale OSM-derived district hits for search
   autocomplete until refreshed. There is no auto-refresh path. Trigger
   it once after deploy:
   ```sql
   SELECT public.refresh_search_index_mat();
   ```

3. **Stale `candidate_location.district_ar` values**: rows resolved
   before this PR may carry OSM-derived labels (potentially Romanian
   text). Run a one-shot re-resolution after this PR ships:
   ```sql
   UPDATE candidate_location SET district_ar = NULL, district_en = NULL
   WHERE district_ar IS NOT NULL;
   ```
   Then re-run `_resolve_districts` against all rows. Decide based on
   row count and acceptable downtime.

## Out of scope

The following were flagged but not addressed in this PR:

- **Aqar overlap cleanup**: 30 pairs of `aqar_district_hulls` polygons
  overlap by ≥10% of the smaller polygon. Top pair:
  `ديراب حي` × `نمار ضاحية حي` at 69.6%. These are duplicate or
  artifact-overlapping hulls; need their own analysis.
- **Aqar giant hulls**: three peripheral hulls exceed 1000 km² (Banban
  1783, An-Nadhim 1645, Ash-Sharq 1385). Convex-hull artifacts;
  switching to `ST_ConcaveHull` would remediate.
