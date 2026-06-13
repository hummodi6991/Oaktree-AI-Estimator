-- competitor_cross_source_overlap_v2.sql
-- =====================================================================
-- F2 RE-PROBE — cross-source competitor overlap with REAL category terms
-- READ-ONLY sizing probe. No app code. Run via:  psql -x -f <thisfile>
-- =====================================================================
--
-- WHY A RE-PROBE
-- --------------
-- The first probe matched competitors with a single token 'burger', which
-- is NOT how production counts. Production (_bulk_enrich_competitors, the
-- UNION ALL at the `combined` CTE, app/services/expansion_advisor.py:7503+)
-- matches the in-category set the way mirrored below:
--
--   Source 1 restaurant_poi:  lower(rp.category) = ANY(:category_keys)
--                             AND (business_status IS NULL
--                                  OR business_status = 'OPERATIONAL')
--   Source 2 delivery_source_record:
--             lower(COALESCE(dsr.category_raw,'')) ~* :category_regex
--          OR lower(COALESCE(dsr.cuisine_raw,'')) ~* :category_regex
--
-- where category_keys / category_regex are the
-- _expand_category(category)["keys"] / ["regex"] outputs.
--
-- This probe measures the overlap (cross-source double counting) the way
-- production actually counts, so the _WHITESPACE_LOG_REF re-anchor is sized
-- against an unbiased sample.
--
-- TERM SETS (verbatim from the live tree, NOT this probe's prose)
-- ---------------------------------------------------------------
--  _CATEGORY_ALIAS_MAP  @ app/services/expansion_advisor.py:154-223
--  _expand_category()   @ app/services/expansion_advisor.py:558-574
--      keys  = aliases["keys"]
--      regex = "|".join(re.escape(p).replace(r"\.", ".") for p in raw_patterns)
--  i.e. the regex == raw_patterns joined by "|" with "." kept as a wildcard.
--
--  Category    POI keys (= ANY)                    DSR regex (~*)
--  burger      {burger}                            burger|hamburger|برجر
--  fast food   {burger,pizza,chicken,fast_food}    fast.food|fast_food|qsr|burger|
--                                                   hamburger|chicken|broasted|
--                                                   fried.chicken|pizza|pizzeria|
--                                                   وجبات سريعة|برجر|دجاج|بيتزا|فاست فود
--  cafe        {cafe,coffee,bakery,dessert}        cafe|coffee|bakery|dessert|pastry|
--                                                   قهوة|مقهى|كافيه|مخبز|حلويات
--  chicken     {chicken}                           chicken|broasted|fried.chicken|wings|دجاج
--
-- CAFE POI KEYS — POST-F8 (baked in; no sed hand-patch needed)
-- -----------------------------------------------------------
-- restaurant_poi.category is assigned ONLY via normalize_category()
-- (app/services/restaurant_categories.py:106), driven by restaurant_pois.py.
-- normalize_category() returns granular keys (coffee, bakery, dessert,
-- juice, burger, pizza, chicken, traditional, international, ...) and NEVER
-- returns the meta-bucket 'coffee_bakery'. The PRIOR version of this probe
-- used the dark key {coffee_bakery}, which matched ~0 real rows — a pure
-- MEASUREMENT ARTIFACT. F8 (PR #1312, commit 9e5790645) lit the cafe POI leg
-- in production by switching to the granular keys {cafe,coffee,bakery,dessert}.
-- This probe now mirrors the DEPLOYED keys so re-runs are correct by default.
-- Result set [C] confirms these keys hit real restaurant_poi.category rows.
-- Likewise 'fast_food' (one of the four fast-food keys) is never produced by
-- normalize_category — but burger/pizza/chicken carry that category, so fast
-- food is still well covered.
--
-- COLUMN/TABLE VERIFICATION (checked against the live tree)
-- --------------------------------------------------------
--  restaurant_poi          : name, category, business_status, geom (NOT NULL,
--                            geometry(Point,4326), trigger-populated from
--                            lat/lon — migration 0010), lat, lon.
--  delivery_source_record  : restaurant_name_raw, category_raw, cuisine_raw,
--                            lat, lon, geom (added by migration
--                            20260322_geom_indexes_dsr_pop, trigger-synced).
--                            _dsr_has_geom branch handled below.
--  expansion_candidate     : id, search_id (FK expansion_search.id),
--                            parcel_id, lat, lon, computed_at.
--                            NOTE: `source_type` was DROPPED by migration
--                            20260330_exp_adv_commercial_units — it no longer
--                            exists, so this probe does NOT reference it.
--  expansion_search        : id, created_at, category, service_model.
--
-- PARAMETERS
-- ----------
--  radius_m       = 1000  (qsr/dine_in/delivery_first tight competition radius
--                          per _catchment_radii; note the `cafe` SERVICE model
--                          uses 800 m in production — see findings note).
--  overlap_dist_m =   75  (spatial cross-source duplicate threshold)
--  per-category sample cap = 60 most-recent distinct candidate centroids.
--
-- _CHAIN_NAME_NORM_SQL is mirrored inline below, quoted verbatim from
-- app/ingest/expansion_advisor_competitors.py:54-66.
-- =====================================================================

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------
-- DSR geom-optional handling (mirrors the _dsr_has_geom branch in
-- _bulk_enrich_competitors). Resolves the geo expression + WHERE guard
-- once, then injects them as raw SQL fragments via :dsr_geo / :dsr_where.
-- ---------------------------------------------------------------------
SELECT
  CASE WHEN EXISTS (SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'delivery_source_record'
                      AND column_name = 'geom')
       THEN 'dsr.geom::geography'
       ELSE 'ST_SetSRID(ST_MakePoint(dsr.lon::double precision, dsr.lat::double precision), 4326)::geography'
  END AS dsr_geo,
  CASE WHEN EXISTS (SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'delivery_source_record'
                      AND column_name = 'geom')
       THEN 'dsr.geom IS NOT NULL'
       ELSE 'dsr.lat IS NOT NULL AND dsr.lon IS NOT NULL'
  END AS dsr_where
\gset

-- ---------------------------------------------------------------------
-- [0] Category term sets (verbatim from the live _CATEGORY_ALIAS_MAP)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS probe_cat;
CREATE TEMP TABLE probe_cat (
    cat_label  text   PRIMARY KEY,
    poi_keys   text[] NOT NULL,
    dsr_regex  text   NOT NULL
);
INSERT INTO probe_cat (cat_label, poi_keys, dsr_regex) VALUES
  ('burger',
     ARRAY['burger'],
     'burger|hamburger|برجر'),
  ('fast food',
     ARRAY['burger','pizza','chicken','fast_food'],
     'fast.food|fast_food|qsr|burger|hamburger|chicken|broasted|fried.chicken|pizza|pizzeria|وجبات سريعة|برجر|دجاج|بيتزا|فاست فود'),
  ('cafe',
     -- POST-F8 (PR #1312, commit 9e5790645): cafe POI keys are the granular
     -- normalize_category() values — the DEPLOYED fix — NOT the dark
     -- meta-bucket {coffee_bakery} (which matched ~0 rows and had to be
     -- hand-patched via sed in the prior run). Baked in so re-runs are correct.
     ARRAY['cafe','coffee','bakery','dessert'],
     'cafe|coffee|bakery|dessert|pastry|قهوة|مقهى|كافيه|مخبز|حلويات'),
  ('chicken',
     ARRAY['chicken'],
     'chicken|broasted|fried.chicken|wings|دجاج');

-- Current _WHITESPACE_LOG_REF in effect (app/services/expansion_advisor.py:2789).
DROP TABLE IF EXISTS probe_ref;
CREATE TEMP TABLE probe_ref (service_model text PRIMARY KEY, current_ref numeric);
INSERT INTO probe_ref (service_model, current_ref) VALUES
  ('qsr', 75), ('dine_in', 50), ('delivery_first', 50);
-- (any other service_model falls back to the default 25 via COALESCE below)

-- ---------------------------------------------------------------------
-- [1] Candidate sample: up to 60 most-recent DISTINCT centroids per
--     probe category, pulled from expansion_candidate joined to the
--     matching expansion_search (category equality on the producing search).
--     service_model is the AUTHORITATIVE expansion_search.service_model of
--     each candidate's OWN producing search (ec.search_id = es.id) — NOT a
--     value inferred from the candidate's category. REF is service_model-keyed,
--     so [D] anchors on this real value (see [D] note on the qsr/cafe split).
--
--     NOTE on the cafe ↔ qsr coupling (grounded, not a probe artifact):
--     expansion_search.service_model DEFAULTS to 'qsr' at the API and the
--     brief form (app/api/expansion_advisor.py:153, ExpansionBriefForm.tsx:34)
--     and is an INPUT independent of `category`. So a cafe-CATEGORY search
--     whose service_model was left at the default is stored with
--     service_model='qsr'. Such candidates therefore carry real sm='qsr' and
--     land in [D]'s qsr bucket by their TRUE service_model — this is real data,
--     surfaced (not hidden) by [D]'s per-bucket category list below.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS probe_cand;
CREATE TEMP TABLE probe_cand AS
WITH ranked AS (
    SELECT
        pc.cat_label,
        ec.parcel_id,
        ec.lon::double precision AS lon,
        ec.lat::double precision AS lat,
        es.service_model,
        es.created_at,
        ROW_NUMBER() OVER (
            PARTITION BY pc.cat_label, ec.parcel_id
            ORDER BY es.created_at DESC
        ) AS rn_parcel
    FROM probe_cat pc
    JOIN expansion_search es
      ON lower(btrim(es.category)) = pc.cat_label
    JOIN expansion_candidate ec
      ON ec.search_id = es.id
    WHERE ec.lat IS NOT NULL AND ec.lon IS NOT NULL
),
distinct_cand AS (
    SELECT cat_label, parcel_id, lon, lat, service_model, created_at
    FROM ranked
    WHERE rn_parcel = 1          -- one row per (category, parcel) centroid
),
capped AS (
    SELECT
        cat_label, parcel_id, lon, lat, service_model,
        ROW_NUMBER() OVER (
            PARTITION BY cat_label ORDER BY created_at DESC
        ) AS rn
    FROM distinct_cand
)
SELECT cat_label, parcel_id, lon, lat, service_model
FROM capped
WHERE rn <= 60;

-- ---------------------------------------------------------------------
-- [2] In-category POI rows within radius (production POI match).
--     name_norm mirrors _CHAIN_NAME_NORM_SQL on rp.name.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS probe_poi;
CREATE TEMP TABLE probe_poi AS
SELECT
    c.cat_label,
    c.parcel_id,
    rp.id AS poi_id,
    TRIM(regexp_replace(
      regexp_replace(
        TRANSLATE(
          LOWER(COALESCE(rp.name, '')),
          E'أإآىـ',
          E'اااي'
        ),
        '[^a-z0-9\s؀-ۿ]', ' ', 'g'
      ),
      '\s+', ' ', 'g'
    )) AS name_norm,
    rp.geom::geography AS geog
FROM probe_cand c
JOIN probe_cat pc ON pc.cat_label = c.cat_label
JOIN restaurant_poi rp
  ON lower(rp.category) = ANY(pc.poi_keys)
 AND (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
 AND ST_DWithin(
        rp.geom::geography,
        ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography,
        1000
     );

-- ---------------------------------------------------------------------
-- [3] In-category DSR rows within radius (production DSR match).
--     name_norm mirrors _CHAIN_NAME_NORM_SQL on dsr.restaurant_name_raw.
--     :dsr_geo / :dsr_where resolved above (geom if present, else lat/lon).
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS probe_dsr;
CREATE TEMP TABLE probe_dsr AS
SELECT
    c.cat_label,
    c.parcel_id,
    dsr.id AS dsr_id,
    TRIM(regexp_replace(
      regexp_replace(
        TRANSLATE(
          LOWER(COALESCE(dsr.restaurant_name_raw, '')),
          E'أإآىـ',
          E'اااي'
        ),
        '[^a-z0-9\s؀-ۿ]', ' ', 'g'
      ),
      '\s+', ' ', 'g'
    )) AS name_norm,
    :dsr_geo AS geog
FROM probe_cand c
JOIN probe_cat pc ON pc.cat_label = c.cat_label
JOIN delivery_source_record dsr
  ON (lower(COALESCE(dsr.category_raw, '')) ~* pc.dsr_regex
      OR lower(COALESCE(dsr.cuisine_raw, '')) ~* pc.dsr_regex)
 AND :dsr_where
 AND ST_DWithin(
        :dsr_geo,
        ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography,
        1000
     );

-- ---------------------------------------------------------------------
-- [4] Cross-source duplicates: a DSR row is a duplicate when it matches
--     ANY in-category POI in the SAME candidate radius by either rule:
--       (a) normalized-name equality (non-empty), OR
--       (b) spatial proximity within 75 m.
--     DISTINCT (cat,parcel,dsr_id) => each DSR row counted at most once.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS probe_dup;
CREATE TEMP TABLE probe_dup AS
SELECT DISTINCT d.cat_label, d.parcel_id, d.dsr_id
FROM probe_dsr d
JOIN probe_poi p
  ON p.cat_label = d.cat_label
 AND p.parcel_id = d.parcel_id
 AND (
       (d.name_norm <> '' AND d.name_norm = p.name_norm)
       OR ST_DWithin(d.geog, p.geog, 75)
     );

-- ---------------------------------------------------------------------
-- [5] Per-candidate roll-up: counts, union (today's inflated count),
--     duplicates, deduped, and overlap_share.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS probe_per_cand;
CREATE TEMP TABLE probe_per_cand AS
WITH poi_c AS (
    SELECT cat_label, parcel_id, COUNT(*) AS poi_count
    FROM probe_poi GROUP BY 1, 2
),
dsr_c AS (
    SELECT cat_label, parcel_id, COUNT(*) AS dsr_count
    FROM probe_dsr GROUP BY 1, 2
),
dup_c AS (
    SELECT cat_label, parcel_id, COUNT(*) AS dup_count
    FROM probe_dup GROUP BY 1, 2
)
SELECT
    c.cat_label,
    c.service_model,
    c.parcel_id,
    COALESCE(poi_c.poi_count, 0) AS poi_count,
    COALESCE(dsr_c.dsr_count, 0) AS dsr_count,
    COALESCE(poi_c.poi_count, 0) + COALESCE(dsr_c.dsr_count, 0) AS union_count,
    COALESCE(dup_c.dup_count, 0) AS dup_count,
    COALESCE(poi_c.poi_count, 0) + COALESCE(dsr_c.dsr_count, 0)
        - COALESCE(dup_c.dup_count, 0) AS deduped_count,
    CASE
      WHEN COALESCE(poi_c.poi_count, 0) + COALESCE(dsr_c.dsr_count, 0) > 0
      THEN COALESCE(dup_c.dup_count, 0)::numeric
           / (COALESCE(poi_c.poi_count, 0) + COALESCE(dsr_c.dsr_count, 0))
      ELSE NULL          -- overlap undefined when no competitors in radius
    END AS overlap_share
FROM probe_cand c
LEFT JOIN poi_c ON poi_c.cat_label = c.cat_label AND poi_c.parcel_id = c.parcel_id
LEFT JOIN dsr_c ON dsr_c.cat_label = c.cat_label AND dsr_c.parcel_id = c.parcel_id
LEFT JOIN dup_c ON dup_c.cat_label = c.cat_label AND dup_c.parcel_id = c.parcel_id;

-- =====================================================================
-- OUTPUT
-- =====================================================================

\echo '==== [A] PER-CATEGORY overlap share + union/deduped percentiles ===='
\echo '     (percentile_cont ignores NULL overlap_share, i.e. zero-competitor candidates)'
WITH agg AS (
    SELECT
        cat_label,
        COUNT(*)                                          AS n_candidates,
        COUNT(*) FILTER (WHERE union_count > 0)           AS n_with_competitors,
        mode() WITHIN GROUP (ORDER BY service_model)      AS dominant_service_model,
        ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY union_count)::numeric, 1) AS union_p50,
        ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY union_count)::numeric, 1) AS union_p75,
        ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY union_count)::numeric, 1) AS union_p90,
        ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY deduped_count)::numeric, 1) AS deduped_p50,
        ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY deduped_count)::numeric, 1) AS deduped_p75,
        ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY deduped_count)::numeric, 1) AS deduped_p90,
        ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY overlap_share)::numeric, 3) AS overlap_p50,
        ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY overlap_share)::numeric, 3) AS overlap_p75,
        ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY overlap_share)::numeric, 3) AS overlap_p90
    FROM probe_per_cand
    GROUP BY cat_label
)
SELECT
    pc.cat_label,
    COALESCE(a.n_candidates, 0)        AS n_candidates,
    a.n_with_competitors,
    a.dominant_service_model,
    COALESCE(r.current_ref, 25)        AS current_ref_for_dominant_sm,
    a.union_p50, a.union_p75, a.union_p90,
    a.deduped_p50, a.deduped_p75, a.deduped_p90,
    a.overlap_p50, a.overlap_p75, a.overlap_p90
FROM probe_cat pc
LEFT JOIN agg a       ON a.cat_label = pc.cat_label
LEFT JOIN probe_ref r ON r.service_model = a.dominant_service_model
ORDER BY pc.cat_label;

\echo ''
\echo '==== [B] ALL-CATEGORIES-POOLED blended summary ===='
SELECT
    'ALL'                                              AS scope,
    COUNT(*)                                           AS n_candidates,
    COUNT(*) FILTER (WHERE union_count > 0)            AS n_with_competitors,
    ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY union_count)::numeric, 1)   AS union_p50,
    ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY union_count)::numeric, 1)   AS union_p75,
    ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY union_count)::numeric, 1)   AS union_p90,
    ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY deduped_count)::numeric, 1) AS deduped_p50,
    ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY deduped_count)::numeric, 1) AS deduped_p75,
    ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY deduped_count)::numeric, 1) AS deduped_p90,
    ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY overlap_share)::numeric, 3) AS overlap_p50,
    ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY overlap_share)::numeric, 3) AS overlap_p75,
    ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY overlap_share)::numeric, 3) AS overlap_p90
FROM probe_per_cand;

\echo ''
\echo '==== [C] POI KEYS-MATCH DIAGNOSTIC (does each key hit real restaurant_poi.category?) ===='
\echo '     poi_rows_matching_keys ~0 => dark key (artifact). Post-F8 cafe={cafe,coffee,'
\echo '     bakery,dessert} should now hit real rows; only the redundant fast_food key stays 0.'
SELECT
    pc.cat_label,
    pc.poi_keys,
    (SELECT COUNT(*) FROM restaurant_poi rp
       WHERE lower(rp.category) = ANY(pc.poi_keys)) AS poi_rows_matching_keys,
    (SELECT array_agg(DISTINCT lower(rp.category)) FROM restaurant_poi rp
       WHERE lower(rp.category) = ANY(pc.poi_keys)) AS matched_category_values
FROM probe_cat pc
ORDER BY pc.cat_label;

\echo ''
\echo '==== [C2] Top restaurant_poi.category literals actually stored (for context) ===='
SELECT lower(category) AS poi_category, COUNT(*) AS n
FROM restaurant_poi
GROUP BY lower(category)
ORDER BY n DESC
LIMIT 40;

\echo ''
\echo '==== [D] ANCHOR-SIZING: current vs suggested _WHITESPACE_LOG_REF (per service_model) ===='
\echo '     GROUPED BY the REAL per-candidate expansion_search.service_model (NOT category).'
\echo '     suggested_ref = deduped p90 rounded. SUGGESTED, FOR REVIEW — NOT applied.'
\echo '     fed_by_categories = distinct probe categories whose candidates fall in this'
\echo '     service_model bucket (sanity check: cafe candidates should NOT sit in qsr'
\echo '     unless their search was created with the default service_model=qsr).'
\echo '     reliability: buckets with n_candidates < 5 are too thin to anchor on.'
SELECT
    COALESCE(pcc.service_model, '(unresolved/null)')                                   AS service_model,
    COALESCE(r.current_ref, 25)                                                        AS current_ref,
    COUNT(*)                                                                            AS n_candidates,
    array_agg(DISTINCT pcc.cat_label ORDER BY pcc.cat_label)                            AS fed_by_categories,
    ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY union_count)::numeric, 1)        AS union_p90,
    ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY deduped_count)::numeric, 1)      AS deduped_p90,
    ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY deduped_count))::int             AS suggested_ref_for_review,
    CASE
      WHEN pcc.service_model IS NULL THEN 'UNRESOLVED service_model — report, do not anchor'
      WHEN COUNT(*) < 5              THEN 'THIN (n<5) — unreliable, do not anchor'
      ELSE 'ok'
    END                                                                                AS reliability
FROM probe_per_cand pcc
LEFT JOIN probe_ref r ON r.service_model = pcc.service_model
GROUP BY pcc.service_model, r.current_ref
ORDER BY n_candidates DESC, service_model;

\echo ''
\echo '==== [D2] BUCKET COMPOSITION: deduped tail per (service_model x category) ===='
\echo '     Diagnostic only — does NOT change the [D] sizing grouping. Shows how much each'
\echo '     real-service_model bucket is fed by each category, so a cafe-category cohort'
\echo '     leaking into the qsr bucket (via default service_model=qsr) is visible directly.'
SELECT
    COALESCE(pcc.service_model, '(unresolved/null)')                                   AS service_model,
    pcc.cat_label                                                                       AS category,
    COUNT(*)                                                                            AS n_candidates,
    ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY deduped_count)::numeric, 1)      AS deduped_p50,
    ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY deduped_count)::numeric, 1)      AS deduped_p90
FROM probe_per_cand pcc
GROUP BY pcc.service_model, pcc.cat_label
ORDER BY service_model, n_candidates DESC, category;

-- =====================================================================
-- END — sizing only. No patch. Ahmed runs this via Codespace and pastes
-- results back for the _WHITESPACE_LOG_REF re-anchor decision.
--
-- READ THIS BEFORE RE-ANCHORING (grounded finding from the live tree):
--   [D] groups by each candidate's REAL expansion_search.service_model. There
--   is NO category->service_model inference anywhere in this probe. If the qsr
--   bucket's deduped p90 is still inflated by dense cafe density after F8 lit
--   the cafe POI leg, it is because expansion_search.service_model DEFAULTS to
--   'qsr' (app/api/expansion_advisor.py:153; brief form default) and is an
--   INPUT independent of `category`. So cafe-CATEGORY searches that left
--   service_model at the default are genuinely stored with sm='qsr' and their
--   competitor density legitimately belongs to the qsr bucket per production's
--   own keying. [D].fed_by_categories and [D2] make this explicit. Decide the
--   qsr re-anchor with that in mind; deliberately excluding cafe-category
--   cohorts from qsr would be a category-based choice that needs sign-off (the
--   probe will NOT make it silently).
-- =====================================================================
