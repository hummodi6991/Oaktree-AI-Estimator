# Restaurant Location Finder — Validation Checklist

## 1. Migration

```bash
# From a fresh DB or existing production-like DB:
alembic upgrade head

# Verify the migration applied cleanly:
alembic current
# Expected: 0010_restaurant_location_tables (head)

# Verify tables exist:
psql -c "\d restaurant_poi"
# Should show: id, name, name_ar, category, subcategory, source, lat, lon,
#              geom (geometry), rating, review_count, price_level, chain_name,
#              district, raw, observed_at

psql -c "\d population_density"
psql -c "\d location_score"

# Verify indexes:
psql -c "\di ix_restaurant_poi_*"
# Expected: ix_restaurant_poi_category, ix_restaurant_poi_source,
#           ix_restaurant_poi_district, ix_restaurant_poi_chain_name,
#           ix_restaurant_poi_geom_gist

# Verify geom trigger:
psql -c "INSERT INTO restaurant_poi (id, name, category, source, lat, lon)
         VALUES ('test:1', 'Test', 'burger', 'test', 24.7, 46.7);"
psql -c "SELECT ST_AsText(geom) FROM restaurant_poi WHERE id = 'test:1';"
# Expected: POINT(46.7 24.7)
psql -c "DELETE FROM restaurant_poi WHERE id = 'test:1';"
```

## 2. Ingestion

```bash
# Overture only:
python -c "
from app.db.session import SessionLocal
from app.ingest.restaurant_pois import ingest_overture_restaurants
db = SessionLocal()
n = ingest_overture_restaurants(db)
print(f'Ingested {n} Overture restaurants')
db.close()
"

# OSM only:
python -c "
from app.db.session import SessionLocal
from app.ingest.restaurant_pois import ingest_osm_restaurants
db = SessionLocal()
n = ingest_osm_restaurants(db)
print(f'Ingested {n} OSM restaurants')
db.close()
"

# Verify counts:
psql -c "SELECT source, category, count(*)
         FROM restaurant_poi
         GROUP BY source, category
         ORDER BY source, count DESC;"
```

## 3. API Smoke Tests

```bash
# 1. Category list
curl -s http://localhost:8000/v1/restaurant/categories | python -m json.tool
# Expected: JSON array with objects {key, name_en, name_ar}

# 2. Score a location (Al Olaya, Riyadh)
curl -s -X POST http://localhost:8000/v1/restaurant/score \
  -H "Content-Type: application/json" \
  -d '{"lat": 24.6937, "lon": 46.6853, "category": "burger"}' \
| python -m json.tool
# Expected: JSON with opportunity_score, demand_score, cost_penalty, factors,
#           contributions, confidence, nearby_competitors

# 3. Heatmap (small bbox around Al Olaya)
curl -s "http://localhost:8000/v1/restaurant/heatmap?category=burger&min_lon=46.65&min_lat=24.66&max_lon=46.72&max_lat=24.72&resolution=8" \
| python -m json.tool | head -20
# Expected: GeoJSON FeatureCollection with features[].properties.score
```

## 4. Performance Check — Competitor Query Using GiST

```sql
-- Verify the competitor query uses the GiST index:
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, name, category, rating, source, lat, lon,
       ST_Distance(
           geom::geography,
           ST_SetSRID(ST_MakePoint(46.6853, 24.6937), 4326)::geography
       ) AS distance_m
FROM restaurant_poi
WHERE geom IS NOT NULL
  AND ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(46.6853, 24.6937), 4326)::geography,
    1000
);

-- Expected in query plan:
--   Index Scan using ix_restaurant_poi_geom_gist on restaurant_poi
-- NOT:
--   Seq Scan on restaurant_poi
```

## 5. Scoring Output Verification

The `/v1/restaurant/score` endpoint returns:

| Field | Description |
|-------|-------------|
| `opportunity_score` | Combined score: `0.8 * demand_score + 0.2 * cost_penalty` |
| `demand_score` | Aggregated demand-side factors (competition, population, traffic, etc.) |
| `cost_penalty` | Aggregated cost-side factors (rent, parking) — higher = cheaper = better |
| `factors` | Individual 0-100 scores for each factor |
| `contributions` | Factors sorted by weighted impact (weight × score / total_weight) |
| `confidence` | 0-1 based on data availability (need ~20 POIs for high confidence) |
| `nearby_competitors` | Up to 20 same-category restaurants sorted by distance |

**NOTE:** `opportunity_score` is a demand-potential proxy, not a profitability
predictor. True profitability requires merchant outcome data.
