# ArcGIS Riyadh Parcel ingest workflow

## What it does
The workflow pulls object IDs from the public ArcGIS FeatureServer layer for Riyadh parcels, fetches features in batches, converts ESRI JSON rings into GeoJSON polygons, and upserts them into PostGIS. The job writes progress checkpoints so the ingest can resume across workflow runs.

## Required secrets
The workflow expects the existing GitHub Actions secrets below (do not add new ones):

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `PGSSLMODE`

## Running the workflow
1. Navigate to **Actions** → **Ingest ArcGIS Riyadh Parcels**.
2. Select **Run workflow**.
3. Provide inputs (defaults shown):
   - `batch_size` (default `300`, must be `<= 2000`)
   - `sleep_s` (default `0.25`)
   - `max_minutes` (default `330`)
   - `resume` (default `true`)
   - `start_from_index` (default `0`)

## Resumability
The ingest uses the `public.ingest_checkpoints` table with job name `arcgis_riyadh_parcel`. When `resume=true`, the script starts from the maximum of `next_index` and `start_from_index`. When `resume=false`, the script resets the checkpoint to `start_from_index` and begins from there. Checkpoints are updated after every successful batch and before graceful exits.

## Verify counts
Use the following SQL to validate progress:

```sql
SELECT * FROM public.ingest_checkpoints WHERE job_name='arcgis_riyadh_parcel';
SELECT count(*) FROM public.riyadh_parcels_arcgis_raw;
```
