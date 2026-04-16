-- Cleanup: set commercial_unit.neighborhood = NULL where the value is pure
-- digits (scraper garbage). Confirmed scope at run time: 87 rows across
-- values "2" (25), "3" (31), "4" (31). Does not touch any other row.
--
-- Run once manually via:
--   PGPASSWORD='...' psql -h ... -U oaktree -d oaktree \
--     --set=sslmode=require -f scripts/cleanup_garbage_neighborhoods.sql
--
-- After this runs, _percentile_rent_burden() will fail the plausibility
-- check on these rows' neighborhood field and correctly fall through to
-- the citywide tier (damped to 0.25 confidence by PR #1114).

BEGIN;

-- Dry-run inspection: show what would be updated.
SELECT neighborhood, COUNT(*) AS n
FROM commercial_unit
WHERE neighborhood ~ '^[0-9]+$'
GROUP BY neighborhood
ORDER BY n DESC;

-- Actual cleanup.
UPDATE commercial_unit
SET neighborhood = NULL
WHERE neighborhood ~ '^[0-9]+$';

-- Verify: should return zero rows.
SELECT COUNT(*) AS remaining_garbage
FROM commercial_unit
WHERE neighborhood ~ '^[0-9]+$';

COMMIT;
