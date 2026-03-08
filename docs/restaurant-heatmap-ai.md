# Restaurant Heatmap AI

Dedicated AI model for the citywide restaurant opportunity heatmap.

## Architecture

The heatmap AI is **separate** from the parcel-level scoring AI:

| Model | Purpose | Artifact |
|---|---|---|
| **Parcel AI** (`restaurant_score_v0`) | Adjusts demand-factor *weights* for per-location scoring | `models/restaurant_score_v0.pkl` |
| **Heatmap AI** (`restaurant_heatmap_v1`) | Directly predicts cell-level opportunity scores for the citywide heatmap | `models/restaurant_heatmap_v1.pkl` |

When the heatmap AI model artifact is present, the `/v1/restaurant/opportunity-heatmap` endpoint uses AI-predicted scores. When absent, it falls back to the curated static scoring — zero breakage.

## Target Definition

The heatmap AI predicts a **demand-gap proxy** per (H3 cell, category):

```
demand_signal = log1p(sum_review_count) * log1p(population)
supply_signal = log1p(competitor_count)
raw_target   = demand_signal / (1 + supply_signal)
target       = max-scaled to 0–100  (divided by training-set max)
```

- **High score** = strong demand indicators with limited competition (underserved area)
- **Low score** = saturated area or low demand signals

This is an explicit heuristic proxy. True merchant-outcome data (sales, order volumes) is not available.

## Features

14 numeric features + one-hot encoded category:

| Feature | Description |
|---|---|
| `population` | H3 cell population |
| `competitor_count` | Same-category POIs within 1200m |
| `all_restaurant_count` | All POIs within 1200m |
| `chain_count` | Distinct chain names in same-category |
| `avg_rating` | Mean rating of same-category POIs |
| `sum_review_count` | Total reviews from same-category |
| `avg_review_count` | Average reviews per same-category POI |
| `avg_price_level` | Mean price level |
| `platform_count` | Delivery platform POIs count |
| `platform_diversity` | Distinct delivery sources |
| `google_coverage` | Fraction with Google place ID |
| `avg_google_confidence` | Mean Google match confidence |
| `poi_density` | Total nearby POIs |
| `complementary_count` | Different-category POIs |

## How to Train Locally

```bash
# Requires database with restaurant_poi and population_density data
python -m app.ml.restaurant_heatmap_train
```

Artifacts produced:
- `models/restaurant_heatmap_v1.pkl`
- `models/restaurant_heatmap_v1.meta.json`

## Smoke-Test Curl Examples

### 1. Check heatmap AI status

```bash
curl -s -H "X-API-Key: YOUR_KEY" \
  http://localhost:8000/v1/restaurant/heatmap-ai-status | python -m json.tool
```

Expected response:
```json
{
  "ai_model_available": false,
  "model_version": null,
  "artifact_present": false,
  "fallback_mode": true,
  "description": "Dedicated citywide heatmap AI model ..."
}
```

### 2. Heatmap with AI metadata

```bash
curl -s -H "X-API-Key: YOUR_KEY" \
  "http://localhost:8000/v1/restaurant/opportunity-heatmap?category=burger" | python -m json.tool | head -30
```

Check the `metadata` block for:
- `ai_used`: true/false
- `model_version`: "heatmap_ai_v1" or "curated_static_v1"
- `scoring_mode`: "heatmap_ai_v1" or "curated_static_v1"

### 3. Parcel score with AI note inputs

```bash
curl -s -X POST -H "X-API-Key: YOUR_KEY" -H "Content-Type: application/json" \
  -d '{"lat": 24.7, "lon": 46.7, "category": "burger"}' \
  http://localhost:8000/v1/restaurant/score | python -m json.tool
```

Check for:
- `model_version`: "weighted_v3" or "ai_weighted_v3"
- `ai_weights_used`: true/false

## Deployment Notes

1. **No migration needed** — the model uses existing database tables (`restaurant_poi`, `population_density`).
2. **Backward compatible** — without the model artifact, the heatmap falls back to the existing curated static scoring.
3. **Cache invalidation** — after placing a new model artifact, set `cache_bust=true` on the next heatmap request or wait for the 7-day cache TTL to expire.
4. **CI** — the GitHub Actions workflow `train-restaurant-heatmap.yml` trains weekly (Sundays 06:00 UTC) and uploads artifacts.

## Risk List

| Risk | Mitigation |
|---|---|
| Model trained on heuristic proxy, not real outcomes | Documented explicitly; acceptable for screening |
| Low-data cells may have noisy predictions | HistGBR handles NaN natively; confidence score still filters low-data cells |
| Cache serves stale AI vs static scores after model deployment | Use `cache_bust=true` or clear `restaurant_heatmap_cache` table |
| Model artifact size | Compressed with joblib (compress=3); typically < 5 MB |
| Frontend shows wrong AI status | Driven by backend metadata, not hardcoded |
