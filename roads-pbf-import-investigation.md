# Roads Workflow PBF Import — Investigation Report

**File:** `.github/workflows/expansion-advisor-data-roads.yml` (read in full, 144 lines)
**Mode:** read-only investigation — no edits, commits, or pushes made.
**Date:** 2026-05-31

---

## 1. What consumes the "PBF URL for OSM import" input?

The input is defined at lines 8–11:

```yaml
pbf_url:
  description: "PBF URL for OSM import"
  required: false
  default: "https://download.geofabrik.de/asia/saudi-arabia-latest.osm.pbf"
```

It is consumed by the **"Import OSM data if needed"** step (lines 59–112), specifically at line 67:

```bash
PBF_URL="${{ github.event.inputs.pbf_url || 'https://download.geofabrik.de/asia/saudi-arabia-latest.osm.pbf' }}"
```

**Yes, it runs osm2pgsql.** After downloading (line 90 `curl -L "$PBF_URL"`) and clipping to a Riyadh bbox with osmium (lines 95–100), it imports via osm2pgsql at lines 105–108:

```bash
osm2pgsql --create --slim --latlong --hstore --multi-geometry \
  --style "${STYLE_PATH}" \
  -d "${PGDATABASE}" \
  /tmp/osm-riyadh.osm.pbf
```

---

## 2. Shared tables or roads-only table? (the decisive question)

**It targets the SHARED `planet_osm_*` tables, NOT a separate roads-only table.**

The osm2pgsql invocation at line 105 passes **no `--prefix`**, so osm2pgsql uses its default prefix `planet_osm`, producing exactly `planet_osm_line`, `planet_osm_point`, `planet_osm_polygon`, `planet_osm_roads`. This is confirmed by the post-import verification at line 112, which queries those names directly:

```bash
psql -c "SELECT 'planet_osm_line' AS tbl, COUNT(*) FROM planet_osm_line UNION ALL SELECT 'planet_osm_roads', COUNT(*) FROM planet_osm_roads;"
```

These are the same tables `osm-import.yml` writes to. That workflow's osm2pgsql explicitly uses `--prefix planet_osm` (`.github/workflows/osm-import.yml:271`) into the identical table family. So there is **no table isolation** — both workflows aim at the same shared `planet_osm_*` tables that parking/proxy/search read.

---

## 3. `--create` vs `--append`?

**`--create`** (line 105) — i.e. drop-and-replace of the targeted tables. (For contrast, `osm-import.yml:262-264` chooses `--append` or `--create` dynamically; this roads workflow is hardcoded to `--create`.)

---

## 4. The critical guard — why it doesn't actually overwrite

The osm2pgsql `--create` only ever runs **if both shared tables are empty.** Before importing, the step counts existing rows (lines 71–74) and bails out early if either is populated (lines 77–84):

```bash
LINE_COUNT=$(psql -tAc "SELECT COUNT(*) FROM planet_osm_line;" 2>/dev/null || echo "0")
ROADS_COUNT=$(psql -tAc "SELECT COUNT(*) FROM planet_osm_roads;" 2>/dev/null || echo "0")
...
if [ "${LINE_COUNT:-0}" != "0" ] && [ "$LINE_COUNT" != "" ]; then
  echo "planet_osm_line already populated ($LINE_COUNT rows), skipping import"
  exit 0
fi
if [ "${ROADS_COUNT:-0}" != "0" ] && [ "$ROADS_COUNT" != "" ]; then
  echo "planet_osm_roads already populated ($ROADS_COUNT rows), skipping import"
  exit 0
fi
echo "No OSM road data found — downloading and importing PBF"   # only reached when both are empty
```

So a fresh `planet_osm_line` (just refreshed by `osm-import.yml` / Overpass) makes `LINE_COUNT` non-zero → the step prints "already populated… skipping import" and `exit 0` **before any download or osm2pgsql call.**

---

## Source read & write targets (`app/ingest/expansion_advisor_roads.py`)

- **Reads** from the first existing of `planet_osm_line`, `planet_osm_roads`, `osm_roads` — `_detect_source_table()`, lines 59–65 (preferring `planet_osm_line`). Used as `{source_table}` in the `SELECT … FROM {source_table} l` at lines 134–138.
- **Writes** into `expansion_road_context` via `INSERT INTO expansion_road_context (…)` at lines 91–144 (Riyadh rows only; `--replace` deletes `WHERE city = 'riyadh'` at line 71). This is a normalized, separate table — it does not write back to `planet_osm_*`.

Note: the workflow's `replace_mode` input only feeds `--replace` (line 117), which controls the DELETE of `expansion_road_context` Riyadh rows — it has **no** effect on the OSM tables.

---

## Conclusion

**Dispatching this workflow against a database that already has fresh `planet_osm_*` data is safe — it will NOT overwrite it.**

But be precise about *why*: the safety does **not** come from isolation. The osm2pgsql import here is aimed squarely at the **shared `planet_osm_*` tables in `--create` (drop+replace) mode**, the very tables `osm-import.yml`, parking, proxy, and search depend on. If those tables were empty and the import fired, it **would** clobber/recreate them.

The protection is purely the **skip-if-populated guard** (lines 77–84): because the freshly refreshed `planet_osm_line` (and/or `planet_osm_roads`) is non-empty, the import step exits early and the workflow proceeds straight to reading the existing `planet_osm_line` into `expansion_road_context`.

**Practical risk note:** the import only triggers when both `planet_osm_line` and `planet_osm_roads` are empty (e.g. a fresh/wiped DB). In that scenario, running this workflow would perform a `--create` import of a **Riyadh-bbox-clipped** PBF into the shared tables — narrower than a full Saudi `osm-import.yml` load. So it's safe to run *now* (data present), but it should not be relied on to populate the shared tables for the rest of the app, since it intentionally only loads a Riyadh clip.
