# READ-ONLY Report: `osm-import.yml` resume semantics after mid-import cancellation

**File:** `.github/workflows/osm-import.yml` (all logic is inline in this YAML — no external script).

**Scenario investigated:** an `osm-import` run (`mode=create`, `tile_deg=0.05`, 396 tiles) downloaded all 396 tiles but was CANCELED by GitHub during the per-tile osm2pgsql import after ~314 tiles. DB tables are now partial. Goal: resume and import only the remaining tiles without re-downloading or re-importing the 314 already done.

## TL;DR

| Question | Answer |
|---|---|
| `force_resume_from` meaning | **"Last COMPLETED tile."** Resume starts at value+1. For 314 done → pass **314**. |
| Re-download on resume? | **Yes — always re-downloads all 396.** Ephemeral FS + fresh checkout + no cache/skip logic. |
| `append` onto partial create tables? | **Works and won't duplicate** (osm2pgsql `--slim --append`, keyed by osm_id). This is the same mechanism the first 314 tiles used. |
| Auto-resume from `last_tile` if input empty? | **Yes, but ONLY in append mode.** In create mode it's force-reset to 0. |
| Resume without re-downloading? | **Not possible.** Resume saves *import* time only, not *download* time. |

---

## 1. How `force_resume_from` is consumed — "last completed" semantics

The input maps to the DB column `osm_import_state.last_tile`:

```yaml
# line 207
FORCE_RESUME="${{ github.event.inputs.force_resume_from }}"
...
# lines 215-221
if [ -n "$FORCE_RESUME" ]; then
  ...
  psql ... -c "UPDATE public.osm_import_state SET last_tile=${FORCE_RESUME}, updated_at=now() WHERE id=1;"
fi

# line 223
LAST_IMPORTED=$(psql -Atc "SELECT last_tile FROM public.osm_import_state WHERE id=1;")
```

The import loop then **skips every tile whose index is `<= LAST_IMPORTED`**:

```yaml
# lines 259-262
if [ "$TILE_IDX" -le "$LAST_IMPORTED" ]; then
  echo "Skipping tile #${TILE_IDX} (already imported)"
  continue
fi
```

And `last_tile` is written **after** each tile's successful import:

```yaml
# line 277
psql ... -c "UPDATE public.osm_import_state SET last_tile=${TILE_IDX}, updated_at=now() WHERE id=1;"
```

**Interpretation: the value is the last COMPLETED tile.** With `force_resume_from=314`, tiles 1–314 are skipped (`314 <= 314`), and importing begins at tile **315** (resume = value + 1). This is exactly what you want for "314 already done."

---

## 2. Does resume re-download, or reuse on-disk tiles?

**It always re-downloads all 396.** The "Prepare Overpass query + download" step (lines 60–167) runs unconditionally on every dispatch and has:

- **No cache** (`actions/cache`) and **no artifact** of `data/osm/tiles/`.
- **No "skip if file exists" guard** — the curl loop (lines 125–167) writes `tile_${TILE_IDX}.osm.xml` for every tile with no existence check.
- A fresh `actions/checkout@v4` (line 35) into an ephemeral `ubuntu-latest` runner.

The runner filesystem **does not persist** between runs. Since the previous run was canceled and a new run starts on a brand-new runner, `data/osm/tiles/*.osm.xml` from the canceled run are gone. The import step even enforces this — it hard-fails if a tile file is missing:

```yaml
# lines 254-257
if [ ! -s "$TILE_PATH" ]; then
  echo "Missing tile file $TILE_PATH" >&2
  exit 1
fi
```

So the download step is **mandatory** before import can run. **Resume re-fetches all 396 tiles from Overpass.**

---

## 3. `append` onto the partial tables — duplication risk?

**Safe. Appending tiles 315–396 onto the partial 314 produces a complete, non-duplicated dataset.**

Reasons:

- osm2pgsql runs in **`--slim`** mode (line 270), so the middle tables (`planet_osm_nodes/ways/rels`) exist from the create run. `--append` requires exactly this slim state — which the canceled `--create --slim` run left behind. So append has a valid prior state to build on.
- The output tables (`planet_osm_polygon`, etc.) are **keyed by OSM object id**. `--append` is delete-and-reinsert by id, so re-encountering a boundary-shared node/way/relation that was already imported updates it in place — **it does not create a duplicate row**.
- This is **the same mechanism the first 314 tiles already used**: in create mode only tile #1 is `--create`; tiles #2 onward are `--append` (lines 264–267). So you're continuing an established per-tile append chain, not doing something novel.
- Per-tile commits are isolated osm2pgsql invocations, and `last_tile` is updated only **after** success (line 277). A kill mid-tile-315 rolls back that one transaction, leaving `last_tile=314` accurate and no half-written tile 315.

**Mode selection:** you can leave `mode` empty — the "Decide mode" step auto-selects `append` because `planet_osm_polygon` already exists:

```yaml
# lines 193-194
elif psql -XtAc "SELECT to_regclass('public.planet_osm_polygon')" | grep -q planet_osm_polygon; then
  echo "value=append" >> "$GITHUB_OUTPUT"
```

> ⚠️ **Do NOT force `mode=create`.** Create mode (a) re-runs tile #1 with `--create`, which drops/recreates the tables, and (b) force-resets the resume point to 0:
>
> ```yaml
> # lines 225-228
> if [ "${{ steps.mode.outputs.value }}" = "create" ]; then
>   LAST_IMPORTED=0
>   psql ... -c "UPDATE public.osm_import_state SET ... last_tile=0 ... WHERE id=1;"
> fi
> ```

---

## 4. Does `last_tile` auto-set the resume point if the input is left empty?

**Yes — in append mode.** When `force_resume_from` is empty, the FORCE_RESUME block (lines 215–221) is skipped and `LAST_IMPORTED` is read straight from the DB:

```yaml
# line 223
LAST_IMPORTED=$(psql -Atc "SELECT last_tile FROM public.osm_import_state WHERE id=1;")
```

So if the canceled run committed `last_tile=314`, leaving the input blank auto-resumes from 315. Two guards to be aware of:

- **Create mode overrides it to 0** (lines 225–228) — so empty-input auto-resume only works with append/auto mode.
- **The stale-reset (lines 238–248)** only fires if `last_tile >= TOTAL_TILES` *and* the row is >144h old. Since `314 < 396`, it will **not** reset you.

**Recommendation:** verify the actual committed value first (the smoke-test query at line 295 prints it, or run `SELECT last_tile FROM osm_import_state WHERE id=1`). Then either leave the input empty (auto) or pass that exact number explicitly. "~314" is approximate; use the real DB value.

---

## 5. Any way to resume the IMPORT without re-downloading?

**No.** Given the ephemeral runner and the workflow as written:

- There is no caching/artifact step for `data/osm/tiles/`.
- The download step is unconditional and the import step hard-fails on any missing tile file (lines 254–257).
- Checkout is fresh each run.

**Stated plainly: every dispatch necessarily re-fetches all 396 tiles from Overpass first. "Resume" only saves IMPORT time (it skips osm2pgsql for tiles 1–314), not DOWNLOAD time.** The download phase still runs full (all 396 Overpass calls) before the import phase begins.

---

## ⚠️ Critical gotcha: `tile_deg` MUST match the original run (0.05)

Tile indexing is **derived** from `BBOX` + `TILE_DEG` each run (lines 96–117). With the env BBOX `24.20,46.20,25.10,47.30`:

- `tile_deg=0.05` → lat 0.90/0.05 = 18 rows × lon 1.10/0.05 = 22 cols = **396 tiles** (matches your run)
- `tile_deg=0.10` (the **input default**) → 9 × 11 = **99 tiles**

If you dispatch with the default `0.10` (or anything ≠ 0.05), two things break:

1. **State auto-resets to 0** on the tile_deg mismatch:
   ```yaml
   # line 213
   UPDATE public.osm_import_state SET ... last_tile=0 ... WHERE id=1 AND (bbox <> '${BBOX}' OR tile_deg <> '${TILE_DEG}');
   ```
   (The stored `tile_deg` is `'0.05'` from the canceled run.)
2. Even with `force_resume_from=314`, the regenerated `tiles.list` would only have ~99 tiles → index 314 > total → **every tile skipped, nothing imported**, and the proxy rebuild runs on incomplete data.

**You must explicitly set `tile_deg=0.05`.**

---

## Conclusion — exact dispatch to finish in one run without duplication

Dispatch `osm-import.yml` (workflow_dispatch) with:

| Input | Value | Why |
|---|---|---|
| `mode` | **`append`** (or leave empty — auto-detects append) | Builds on the partial slim tables; never re-creates. |
| `force_resume_from` | **`314`** *(better: the exact `last_tile` value currently in `osm_import_state` — verify first)* | Skips tiles 1–314, imports 315–396. |
| `tile_deg` | **`0.05`** *(required — NOT the 0.10 default)* | Reproduces the same 396-tile indexing; avoids the mismatch state-reset. |

This imports only tiles 315–396 by OSM-id append → **complete, non-duplicated dataset**.

**Honest caveat (Q5):** this run will still **re-download all 396 tiles** from Overpass before importing — the ephemeral runner has no surviving tile cache. Resume saves the osm2pgsql import time on 314 tiles, not the download time. Budget for a full 396-tile Overpass fetch (and the prior cancellation suggests that fetch+import together exceeds the job's tolerance, so watch for another timeout/cancel).

### Alternative if you'd rather not re-download/re-import 396 every time

Because changing `tile_deg` resets state and re-numbers tiles, a larger `tile_deg` is **not** a resume — it's a full fresh `create`. Options:

- **Fewer, larger tiles (fresh create):** dispatch `mode=create`, `tile_deg=0.10` (99 tiles) or `0.15`. Far fewer osm2pgsql invocations and Overpass calls, much less likely to be canceled — but it re-downloads and re-imports everything from scratch (discards the partial 0.05 tables via `--create`). Best if a single resume run keeps getting canceled.
- **Stay at 0.05 and just complete via the append+resume above** — fewest wasted cycles if the one resume run fits in the job window.

---

*Investigation only — no files in the workflow or application were modified.*
