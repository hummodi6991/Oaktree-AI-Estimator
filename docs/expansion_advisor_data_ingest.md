# Expansion Advisor Data Ingestion Pipeline

## Overview

The Expansion Advisor uses five normalized data tables populated by dedicated GitHub Actions workflows. These tables provide Riyadh-specific data for road/access context, parking, delivery marketplace, rent comps, and competitor quality scoring.

When populated, the Expansion Advisor service automatically prefers these tables over legacy data sources (raw OSM tables, delivery_source_record, rent_comp, restaurant_poi). When empty, the service falls back safely to existing behavior.

## Workflows

| Workflow | File | Schedule | What it ingests |
|----------|------|----------|-----------------|
| **Roads & Access** | `expansion-advisor-data-roads.yml` | Monday 03:00 UTC | OSM road geometry → `expansion_road_context` |
| **Parking Context** | `expansion-advisor-data-parking.yml` | Tuesday 04:00 UTC | OSM parking amenities → `expansion_parking_asset` |
| **Delivery Marketplace** | `expansion-advisor-data-delivery.yml` | Wednesday 05:00 UTC | `delivery_source_record` → `expansion_delivery_market` |
| **Retail Rent & Lease Comps** | `expansion-advisor-data-rent-comps.yml` | Thursday 06:00 UTC | Aqar/Kaggle/CSV → `expansion_rent_comp` |
| **Competitor Quality** | `expansion-advisor-data-competitors.yml` | Friday 07:00 UTC | `restaurant_poi` + delivery data → `expansion_competitor_quality` |

All workflows can also be triggered manually via `workflow_dispatch`.

## Required Secrets

| Secret | Used by | Required |
|--------|---------|----------|
| `DATABASE_URL` | All workflows | Yes (or individual POSTGRES_* vars) |
| `POSTGRES_USER` | All workflows | If DATABASE_URL not set |
| `POSTGRES_PASSWORD` | All workflows | If DATABASE_URL not set |
| `POSTGRES_HOST` | All workflows | If DATABASE_URL not set |
| `POSTGRES_DB` | All workflows | If DATABASE_URL not set |
| `PGSSLMODE` | All workflows | Recommended (`require`) |
| `KAGGLE_USERNAME` | Rent comps | Optional (for Kaggle download) |
| `KAGGLE_KEY` | Rent comps | Optional (for Kaggle download) |
| `GOOGLE_PLACES_API_KEY` | Competitors | Optional (for Google review enrichment) |

## Normalized Tables

### `expansion_road_context`
- **Source**: planet_osm_line / planet_osm_roads / osm_roads
- **Key fields**: road_class, is_major_road, touches_road, frontage_length_m, corner_lot
- **Used by**: frontage_score, access_score, visibility signals
- **Note**: frontage_length_m and corner_lot are heuristic approximations

### `expansion_parking_asset`
- **Source**: planet_osm_polygon, planet_osm_point
- **Key fields**: amenity_type, capacity, walk_access_score, dropoff_score
- **Used by**: parking_score, parking context availability

### `expansion_delivery_market`
- **Source**: delivery_source_record (normalized, Riyadh-only)
- **Key fields**: platform, brand_name, category, rating, eta_minutes
- **Used by**: provider_density_score, multi_platform_presence_score

### `expansion_rent_comp`
- **Source**: rent_comp table or direct CSV/Kaggle import
- **Key fields**: district, asset_type, rent_sar_m2_year
- **Used by**: rent estimation (preferred over aqar_rent_median when populated)

### `expansion_competitor_quality`
- **Source**: restaurant_poi + delivery data + optional Google reviews
- **Key fields**: chain_strength_score, review_score, delivery_presence_score, overall_quality_score
- **Used by**: comparable competitors, competitive pressure analysis

## How the App Consumes the Tables

The Expansion Advisor service (`app/services/expansion_advisor.py`) checks each normalized table at query time:

1. **Road/Access**: If `expansion_road_context` has rows, road queries use it instead of raw `planet_osm_line`
2. **Parking**: If `expansion_parking_asset` has rows, parking queries use it instead of raw `planet_osm_polygon`
3. **Delivery**: If `expansion_delivery_market` has rows, delivery scoring uses it instead of raw `delivery_source_record`
4. **Rent**: If `expansion_rent_comp` has rows, rent estimation uses it before falling back to `aqar_rent_median`
5. **Competitors**: If `expansion_competitor_quality` has rows, comparable competitor queries use it before `restaurant_poi`

Source provenance is tracked in `feature_snapshot_json.context_sources`:
- `road_source`: `"expansion_road_context"` or `"estimated"`
- `parking_source`: `"expansion_parking_asset"` or `"estimated"`
- `delivery_source`: `"expansion_delivery_market"` or `"delivery_source_record"`
- `rent_source`: `"expansion_rent_district_retail"`, `"expansion_rent_district_commercial"`, `"expansion_rent_city_retail"`, `"expansion_rent_city_commercial"`, `"aqar_district"`, etc.
- `competitor_source`: `"expansion_competitor_quality"` or `"restaurant_poi"`

## Verifying Row Counts After a Run

Each workflow uploads a JSON artifact with row counts. You can also check directly:

```sql
SELECT 'expansion_road_context' AS t, COUNT(*) FROM expansion_road_context
UNION ALL
SELECT 'expansion_parking_asset', COUNT(*) FROM expansion_parking_asset
UNION ALL
SELECT 'expansion_delivery_market', COUNT(*) FROM expansion_delivery_market
UNION ALL
SELECT 'expansion_rent_comp', COUNT(*) FROM expansion_rent_comp
UNION ALL
SELECT 'expansion_competitor_quality', COUNT(*) FROM expansion_competitor_quality;
```

Or run the refresh module:
```bash
python -m app.ingest.expansion_advisor_refresh --skip-alembic
```

## Running Ingest Modules Manually

Each module supports `python -m` execution with argparse:

```bash
# Roads
python -m app.ingest.expansion_advisor_roads --replace true --write-stats stats.json

# Parking
python -m app.ingest.expansion_advisor_parking --replace true --write-stats stats.json

# Delivery
python -m app.ingest.expansion_advisor_delivery --platforms hungerstation,jahez --write-stats stats.json

# Rent comps (from existing rent_comp table)
python -m app.ingest.expansion_advisor_rent_comps --replace true --write-stats stats.json

# Rent comps (from CSV URL)
python -m app.ingest.expansion_advisor_rent_comps --csv-url https://example.com/rents.csv

# Competitors
python -m app.ingest.expansion_advisor_competitors --replace true --write-stats stats.json
```
