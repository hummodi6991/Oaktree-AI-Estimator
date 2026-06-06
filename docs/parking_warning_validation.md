# Parking-warning validation artifacts (READ-ONLY)

Target API: **`http://8.213.84.191/`** · Endpoint: **`POST /v1/estimates`** (router mounted at `/v1`, `app/main.py:208`).
All shell/SQL below is single-line (iPad/Safari safe). Set `OAKTREE_API_KEY` once in Codespace if auth is on.

---

## 1) Auth — does POST require it?

The estimates router is mounted with `dependencies=[Depends(set_auth_context)]` → `auth.require` (`app/main.py:204,208`,
`app/security/auth_context.py:7`, `app/security/auth.py:32`). Behavior is decided at request time by env `AUTH_MODE`:

- `AUTH_MODE=disabled` → endpoint is **open**, no header needed.
- `AUTH_MODE=api_key` → send header **`X-API-Key: <key>`** (also accepts `Authorization: Bearer <key>`).
  Server validates against `ADMIN_API_KEYS_JSON` / `API_KEYS_JSON` / `API_KEY` (`app/security/auth.py:39-56`).
- `AUTH_MODE=oidc` → returns 501 (placeholder; not wired).

**Production almost certainly runs `api_key`**: the SPA gates the whole app behind `AccessCodeModal` and the access
code is stored in `localStorage["oaktree_api_key"]` and sent as `X-API-Key` (`frontend/src/api.ts:23,29`).

**Token source from Codespace:** use the same value the app uses. Either (a) read the server env var
(`API_KEY` / a value inside `API_KEYS_JSON`), or (b) copy the logged-in browser value: DevTools → Application →
Local Storage → `http://8.213.84.191` → key `oaktree_api_key`. Then `export OAKTREE_API_KEY='<that value>'`.
If the server is `disabled`, omit the `-H "X-API-Key: …"` line entirely.

> Quick probe (expect `401` if api_key+missing key, `200/422` if open): `curl -s -o /dev/null -w "%{http_code}\n" -X POST "http://8.213.84.191/v1/estimates" -H "Content-Type: application/json" -d '{}'`

---

## 2) Create-estimate contract

Request model `EstimateRequest` (`app/api/estimates.py:599`). Required fields: **`geometry`** (GeoJSON dict or JSON
string) and **`excel_inputs`** (dict). Everything else has defaults (`city` defaults to "Riyadh"; `unit_mix` defaults
to `[]`). `createEstimate` in `frontend/src/api.ts:417` just POSTs JSON to `/v1/estimates`.

**There is no `parcel_id` field** — the endpoint takes an explicit GeoJSON `geometry`, not a parcel id. So parcel
**`1105644` is not reusable as an id here**; supply a polygon directly (any small Riyadh ring works; the example below
is the in-repo sample near 46.675,24.713). `area_ratio` is supplied inside `excel_inputs` (a residential share there
drives residential GFA, which is what the parking producers key off of).

Minimum viable body: `{"geometry": <Polygon>, "excel_inputs": {"area_ratio": {"residential": 1.6}}}` (add
`"land_price_sar_m2": 2800` to skip the DB land-price lookup and keep the call deterministic).

---

## 3) Fire producer `:342-343` (unit_mix_missing) — `app/services/parking.py:332-344`

Residential GFA present **and no `unit_mix`** → backend approximates 1 space/unit and emits `unit_mix_missing` with
`avg_unit_m2` = `parking_assumed_avg_apartment_m2` (default **120**).

```
curl -sS -X POST "http://8.213.84.191/v1/estimates" -H "Content-Type: application/json" -H "X-API-Key: $OAKTREE_API_KEY" -d '{"geometry":{"type":"Polygon","coordinates":[[[46.675,24.713],[46.676,24.713],[46.676,24.714],[46.675,24.714],[46.675,24.713]]]},"city":"Riyadh","excel_inputs":{"area_ratio":{"residential":1.6,"basement":1},"land_price_sar_m2":2800}}' | jq '{id, items:.notes.parking.requirement_meta.warning_items, warnings:.notes.parking.requirement_meta.warnings}'
```

**Expected:** `items` = `[{"code":"unit_mix_missing","params":{"avg_unit_m2":120}}]` and `warnings` = one English
string `"unit_mix missing/empty; residential parking approximated as 1 space per estimated unit …"`. (POST response
is flat: `notes.parking…`. Note the returned `id` for steps 5–6.)

---

## 4) Fire producer `:324-325` (avg_m2_missing) — `app/services/parking.py:311-325`

A `unit_mix` row with `count>0` and **missing/zero `avg_m2`** alongside residential GFA → emits `avg_m2_missing`
**once per such row** (two rows below → duplicated → the v2 UI de-dups to one, `ExcelForm.tsx:1762`).

```
curl -sS -X POST "http://8.213.84.191/v1/estimates" -H "Content-Type: application/json" -H "X-API-Key: $OAKTREE_API_KEY" -d '{"geometry":{"type":"Polygon","coordinates":[[[46.675,24.713],[46.676,24.713],[46.676,24.714],[46.675,24.714],[46.675,24.713]]]},"city":"Riyadh","unit_mix":[{"type":"apartment","count":40},{"type":"apartment","count":20}],"excel_inputs":{"area_ratio":{"residential":1.6,"basement":1},"land_price_sar_m2":2800}}' | jq '{id, items:.notes.parking.requirement_meta.warning_items, warnings:.notes.parking.requirement_meta.warnings}'
```

**Expected:** `items` contains two `{"code":"avg_m2_missing","params":{}}` entries (one per unit row); the v2 UI
collapses them to a single localized line. Use one `unit_mix` row instead if you want exactly one entry.

---

## 5) Open a persisted estimate in the v2 UI (AR)

**The v2 UI cannot open an arbitrary persisted estimate id.** There is no route/query-param loader: in v2, results
only render in-session after the form POSTs (`frontend/src/main.tsx:71-82` only reads `?ui=`; `estimateId` is set
solely from the POST in `ExcelForm.tsx:317`). And the form auto-derives unit sizes, so it can't fire either producer
through normal use. So a created id can't be reopened/viewed in the running UI.

**Set AR locale (for reference):** `localStorage["oaktree_locale"]="ar"` then reload (or click the language switcher to
Arabic) — `frontend/src/i18n/index.ts:7,69-74`.

**Alternative (the actual validation):** inspect the POST/GET JSON directly (the `warning_items` codes are
locale-agnostic), and confirm the AR rendering keys resolve:
- Rendering maps `code → t(key, params)` and falls back to the English `warnings` string for unknown/legacy codes,
  then de-dups (`ExcelForm.tsx:1714-1762`). Map: `unit_mix_missing → excel.parkingWarnUnitMixMissing`,
  `avg_m2_missing → excel.parkingWarnAvgM2Missing`.
- AR strings exist: `frontend/src/i18n/ar.json:200-201` (EN at `en.json:200-201`). The `unit_mix_missing` AR string
  interpolates `{{avg_unit_m2}}` → "…بافتراض ~120 m² لكل وحدة".
- GET read path (double-wrapped) is handled by `ParkingSummary.unwrapNotes` (`ParkingSummary.tsx:7-14`) and
  `resolveParking` (`ExcelForm.tsx:1598-1606`): both read `notes.notes.parking` for GET and `notes.parking` for POST.

GET an estimate to exercise that unwrap path:
```
curl -sS "http://8.213.84.191/v1/estimates/<ID>" -H "X-API-Key: $OAKTREE_API_KEY" | jq '{items:.notes.notes.parking.requirement_meta.warning_items, warnings:.notes.notes.parking.requirement_meta.warnings}'
```
**Expected:** same `warning_items`/`warnings` as the POST, but nested one level deeper under `notes.notes` (GET
double-wraps; POST is flat). Non-empty for a fresh row.

---

## 6) psql (single-line each)

Column is `estimate_header.notes_json` (Text JSON); the persisted shape is `{"bands":…, "notes":{… "parking":…}}`
(`app/api/estimates.py:871,917-923`, model `tables.py:185-194`). Note the timestamp column is **`created_at`** (there
is no `computed_at` on `estimate_header`).

**(a) Print `warning_items` and `warnings` for a given id (confirm both present on a fresh row):**
```
psql "$DATABASE_URL" -c "SELECT id, notes_json::json#>'{notes,parking,requirement_meta,warning_items}' AS warning_items, notes_json::json#>'{notes,parking,requirement_meta,warnings}' AS warnings FROM estimate_header WHERE id='<ID>';"
```
**Expected:** `warning_items` = JSON array of `{code,params}` and `warnings` = JSON array of English string(s); both non-empty.

**(b) Find one legacy (pre-merge) id: `warnings` non-empty but `warning_items` absent/empty (English-fallback test):**
```
psql "$DATABASE_URL" -c "SELECT id, created_at FROM estimate_header WHERE json_array_length(COALESCE(notes_json::json#>'{notes,parking,requirement_meta,warnings}','[]'::json))>0 AND json_array_length(COALESCE(notes_json::json#>'{notes,parking,requirement_meta,warning_items}','[]'::json))=0 ORDER BY created_at ASC LIMIT 1;"
```
**Expected:** one old row id (warnings present, no `warning_items`) — exactly the pre-merge shape the UI must render via
the English fallback. The missing `warning_items` is itself the "before-merge" marker; add `AND created_at < '<MERGE_TS>'`
if you want to pin it to a timestamp.
