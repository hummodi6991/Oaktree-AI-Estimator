# Investigation Report: `planet_osm_line` = 596, `expansion_road_context` = 4

**Mode:** investigation only (no code edits, commits, or pushes).
**Date:** 2026-05-31

---

## 1. The Overpass query block in `.github/workflows/osm-import.yml`

The query template is written in the "Prepare Overpass query + download" step at `.github/workflows/osm-import.yml:67-91`:

```
67	          cat > data/osm/riyadh.overpass <<'Q'
68	          [out:xml][timeout:180];
69	          (
70	            way["building"]({{bbox}});
71	            relation["building"]({{bbox}});
72	            way["landuse"]({{bbox}});
73	            relation["landuse"]({{bbox}});
74	            way["amenity"]({{bbox}});
75	            relation["amenity"]({{bbox}});
76	            way["leisure"]({{bbox}});
77	            relation["leisure"]({{bbox}});
78	            way["shop"]({{bbox}});
79	            relation["shop"]({{bbox}});
80	            way["tourism"]({{bbox}});
81	            relation["tourism"]({{bbox}});
82	            way["natural"]({{bbox}});
83	            relation["natural"]({{bbox}});
84	            way["landcover"]({{bbox}});
85	            relation["landcover"]({{bbox}});
86	            way["man_made"]({{bbox}});
87	            relation["man_made"]({{bbox}});
88	          );
89	          (._;>;);
90	          out body;
91	          Q
```

**Every key requested** (way + relation for each):

| Key | Lines |
|-----|-------|
| `building` | 70–71 |
| `landuse` | 72–73 |
| `amenity` | 74–75 |
| `leisure` | 76–77 |
| `shop` | 78–79 |
| `tourism` | 80–81 |
| `natural` | 82–83 |
| `landcover` | 84–85 |
| `man_made` | 86–87 |

**`highway` is NOT present.** No road/line key (`highway`, `railway`, `waterway`) is requested anywhere in the block. These keys are overwhelmingly area/polygon-oriented. The handful of lines that do land in `planet_osm_line` (596 rows) are the *linear* members of these area-ish keys — e.g. `natural=cliff/ridge/coastline`, `man_made=pipeline/embankment`, linear `landuse`/`leisure` ways — plus way members pulled in by the recursion `(._;>;);` at line 89. That residual is exactly the kind of small count (596) you'd expect when roads are never fetched.

---

## 2. What `expansion_advisor_roads.py` reads and filters

**Source table** — `_detect_source_table` (`app/ingest/expansion_advisor_roads.py:59-65`) picks the first existing of `planet_osm_line`, `planet_osm_roads`, `osm_roads`. In this import only `planet_osm_line` exists, so that's the source.

**The SELECT/WHERE** (the `road_class` CASE keys entirely off `l.highway`, lines 102-138; the filter is at lines 134-137):

```
134	        FROM {source_table} l
135	        WHERE l.{geom_col} IS NOT NULL
136	          AND l.highway IS NOT NULL
137	          AND {bbox_filter}
```

The `WHERE` requires `l.highway IS NOT NULL` (line 136), and the entire `road_class` / `is_major_road` / `is_service_road` / `uturn_access_proxy` derivation reads only `l.highway` (lines 104-133). So the ingest selects **only rows that carry a `highway` tag**.

**Would a highway-less import starve it? Yes, completely.** If the Overpass query never asks for `highway`, then almost no `planet_osm_line` row has a non-NULL `highway`, so line 136 filters nearly everything out. The 4 rows that survived are the few incidental ways that happened to carry a `highway` tag *and* one of the requested keys (e.g. a `service` way also tagged `amenity=parking`, or `highway` ways dragged in as members by the `(._;>;)` recursion) — essentially noise, not a real road network.

---

## 3. Root cause — plainly stated

**Yes. The 596/4 result is explained by the Overpass query omitting the `highway` key.**

- `planet_osm_line` is small (596) because the import only requests area/feature keys; road geometry is never downloaded, so almost nothing lands in the line table.
- `expansion_road_context` has only 4 rows because `expansion_advisor_roads.py` filters `WHERE l.highway IS NOT NULL` (`app/ingest/expansion_advisor_roads.py:136`) against a `planet_osm_line` that has essentially no highway-tagged ways. Starved input → 4 rows out.

The two symptoms share one upstream cause: the missing `way["highway"]` / `relation["highway"]` in the Overpass query block.

---

## Candidate fix (NOT implemented)

Add the highway key to the Overpass union in `.github/workflows/osm-import.yml`. The natural insertion point is **inside the union block, immediately before the closing `);` at line 88** (i.e. after the `man_made` pair at line 87), adding:

```
            way["highway"]({{bbox}});
            relation["highway"]({{bbox}});
```

Then re-run the OSM import (workflow_dispatch with `mode=create`, or let the weekly cron pick it up) so `planet_osm_line` is repopulated with roads, and re-run the `expansion_advisor_roads` ingest.

**Exact line to edit:** `.github/workflows/osm-import.yml:88` (insert the two `highway` lines just above the `);` that currently sits on line 88, right after line 87). No code change is needed in `expansion_advisor_roads.py` — its `l.highway` logic is already correct and is simply being starved of input.
