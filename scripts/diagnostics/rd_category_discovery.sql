\pset footer off
\echo ''
\echo '=== A. Top 50 raw category_raw values in rating_history ==='
SELECT
  lower(COALESCE(category_raw, '(null)')) AS category_raw_lower,
  COUNT(DISTINCT source_record_id)         AS branches,
  COUNT(*)                                  AS rows
FROM expansion_delivery_rating_history
GROUP BY lower(COALESCE(category_raw, '(null)'))
ORDER BY branches DESC
LIMIT 50;

\echo ''
\echo '=== B. Top 50 raw cuisine_raw values in rating_history ==='
SELECT
  lower(COALESCE(cuisine_raw, '(null)')) AS cuisine_raw_lower,
  COUNT(DISTINCT source_record_id)        AS branches,
  COUNT(*)                                 AS rows
FROM expansion_delivery_rating_history
GROUP BY lower(COALESCE(cuisine_raw, '(null)'))
ORDER BY branches DESC
LIMIT 50;

\echo ''
\echo '=== C. Candidate Arabic / Saudi / chicken / broasted matches ==='
SELECT
  probe,
  COUNT(DISTINCT source_record_id) AS branches
FROM expansion_delivery_rating_history h
CROSS JOIN (VALUES
  ('saudi'),('traditional'),('arabic'),('arab'),('lebanese'),('egyptian'),
  ('chicken'),('fried'),('broast'),('grill'),('kebab'),('mandi'),
  ('kabsa'),('mashawi'),('مشويات'),('مطعم'),('دجاج'),('سعودي'),
  ('عربي'),('كبسة'),('مندي')
) AS p(probe)
WHERE lower(COALESCE(category_raw,'')) LIKE '%' || probe || '%'
   OR lower(COALESCE(cuisine_raw, '')) LIKE '%' || probe || '%'
GROUP BY probe
ORDER BY branches DESC;
