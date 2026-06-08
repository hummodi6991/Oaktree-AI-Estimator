-- ============================================================================
-- l3_market_indicator_check.sql
-- ----------------------------------------------------------------------------
-- READ-ONLY check for Layer 3 (SAMA POS + GASTAT city-level multiplier).
--
-- L3 is a CITY-LEVEL market-trend multiplier / spend-capacity weight, applied
-- uniformly to all candidates — NOT a per-listing ranker. The plan is to reuse
-- the existing market_indicator storage + the latest_re_price_index_scalar()
-- read pattern (app/services/indicators.py:164-172) for a new F&B / expenditure
-- scalar. This script confirms that storage home exists and is populated, and
-- shows exactly which indicator_type / city / asset_type values are present so
-- the new SAMA-POS / GASTAT-F&B rows slot into the same conventions.
--
-- HOW TO RUN:
--   psql "$DATABASE_URL" -f scripts/diagnostics/l3_market_indicator_check.sql
--
-- market_indicator schema (app/models/tables.py:111-122):
--   date, city, asset_type, indicator_type, value, unit, source_url, asof_date
-- ============================================================================

\timing on

-- Does the storage table exist?
SELECT to_regclass('public.market_indicator') IS NOT NULL AS market_indicator_exists;

-- What indicator_types / units exist today, and how fresh are they?
-- The GASTAT real-estate price index scalar lives here as
-- indicator_type='real_estate_price_index', unit='index_2014_100'
-- (app/ingest/real_estate_indices.py:33,35). A new F&B scalar would be a new
-- indicator_type (e.g. 'fnb_spend_index' / 'pos_restaurants_index') in the SAME
-- table, read via the same latest_* pattern.
SELECT indicator_type,
       unit,
       city,
       asset_type,
       COUNT(*)        AS rows,
       MIN(date)       AS first_date,
       MAX(date)       AS latest_date,
       round(AVG(value), 2) AS avg_value
FROM market_indicator
GROUP BY indicator_type, unit, city, asset_type
ORDER BY indicator_type, city, asset_type;

-- Confirm the real-estate price-index scalar is present and readable (the
-- template the F&B scalar reuses). Latest value /100 = the 2014=1.0 scalar.
SELECT city,
       asset_type,
       MAX(date)                          AS latest_date,
       (SELECT value FROM market_indicator mi2
         WHERE mi2.indicator_type = 'real_estate_price_index'
           AND mi2.city = mi.city
           AND mi2.asset_type = mi.asset_type
         ORDER BY mi2.date DESC LIMIT 1)   AS latest_index_2014_100,
       round(
         (SELECT value FROM market_indicator mi2
           WHERE mi2.indicator_type = 'real_estate_price_index'
             AND mi2.city = mi.city
             AND mi2.asset_type = mi.asset_type
           ORDER BY mi2.date DESC LIMIT 1) / 100.0, 4) AS scalar_2014_1_0
FROM market_indicator mi
WHERE indicator_type = 'real_estate_price_index'
GROUP BY city, asset_type
ORDER BY city, asset_type;

-- Does any F&B / restaurant / POS / spend indicator ALREADY exist? (Expected:
-- none today — confirms L3 is greenfield and needs a new ingest job, not a
-- duplicate.)
SELECT indicator_type, city, COUNT(*) AS rows
FROM market_indicator
WHERE indicator_type ILIKE '%fnb%'
   OR indicator_type ILIKE '%food%'
   OR indicator_type ILIKE '%restaurant%'
   OR indicator_type ILIKE '%pos%'
   OR indicator_type ILIKE '%spend%'
   OR indicator_type ILIKE '%expenditure%'
   OR indicator_type ILIKE '%cci%'
GROUP BY indicator_type, city
ORDER BY indicator_type;

-- How does 'Riyadh' resolve in the city field today (so a SAMA-POS Riyadh row
-- matches the existing convention rather than introducing a new spelling)?
SELECT DISTINCT city
FROM market_indicator
ORDER BY city;
