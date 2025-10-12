import type { CSSProperties } from "react";
import { useCallback, useEffect, useMemo, useState } from "react";
import type { Polygon } from "geojson";
import Map from "./Map";
import { createEstimate, getFreshness, memoPdfUrl, runScenario, getComps, exportCsvUrl } from "./api";
import "./App.css";
import type { EstimateResponse } from "./lib/types";
import { pickRent } from "./lib/pickRent";
import RentSummary from "./components/RentSummary";

const DEFAULT_POLY: Polygon = {
  type: "Polygon",
  coordinates: [
    [
      [46.675, 24.713],
      [46.676, 24.713],
      [46.676, 24.714],
      [46.675, 24.714],
      [46.675, 24.713]
    ]
  ]
};

type PolygonStats = {
  areaSqm: number;
  perimeterMeters: number;
  vertexCount: number;
};

function computePolygonStats(polygon: Polygon | null | undefined): PolygonStats | null {
  if (!polygon?.coordinates?.[0]) return null;
  const ring = polygon.coordinates[0];
  if (!Array.isArray(ring) || ring.length < 4) return null;

  const isClosed =
    ring[0][0] === ring[ring.length - 1][0] && ring[0][1] === ring[ring.length - 1][1];
  const workingRing = isClosed ? ring.slice(0, -1) : [...ring];
  if (workingRing.length < 3) return null;

  const earthRadius = 6378137;
  const avgLatRad =
    workingRing.reduce((sum, [, lat]) => sum + (lat * Math.PI) / 180, 0) / workingRing.length;
  const cosLat = Math.cos(avgLatRad || 0);

  const projected = workingRing.map(([lng, lat]) => {
    const lngRad = (lng * Math.PI) / 180;
    const latRad = (lat * Math.PI) / 180;
    const x = earthRadius * lngRad * cosLat;
    const y = earthRadius * latRad;
    return [x, y] as [number, number];
  });

  let area = 0;
  let perimeter = 0;
  for (let i = 0; i < projected.length; i += 1) {
    const j = (i + 1) % projected.length;
    const [x1, y1] = projected[i];
    const [x2, y2] = projected[j];
    area += x1 * y2 - x2 * y1;
    const dx = x2 - x1;
    const dy = y2 - y1;
    perimeter += Math.sqrt(dx * dx + dy * dy);
  }

  return {
    areaSqm: Math.abs(area) / 2,
    perimeterMeters: perimeter,
    vertexCount: workingRing.length,
  };
}

export default function App() {
  const [freshness, setFreshness] = useState<any>(null);
  const [city, setCity] = useState("Riyadh");
  const [far, setFar] = useState(2.0);
  const [months, setMonths] = useState(18);
  const [geom, setGeom] = useState(JSON.stringify(DEFAULT_POLY, null, 2));
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | undefined>();
  const [estimate, setEstimate] = useState<EstimateResponse | null>(null);
  const [comps, setComps] = useState<any[]>([]);
  const [uplift, setUplift] = useState(0);

  const parsedGeom = useMemo(() => {
    try {
      const parsed = JSON.parse(geom);
      return parsed?.type === "Polygon" ? (parsed as Polygon) : null;
    } catch {
      return null;
    }
  }, [geom]);

  const polygonForMap = parsedGeom ?? DEFAULT_POLY;
  const polygonStats = useMemo(() => computePolygonStats(polygonForMap), [polygonForMap]);

  useEffect(() => {
    getFreshness().then(setFreshness).catch(() => {});
  }, []);

  const totals = estimate?.totals;
  const irr = estimate?.metrics?.irr_annual;
  const { rent, comps: rentComps, drivers: rentDrivers } = estimate
    ? pickRent(estimate)
    : { rent: null, comps: [], drivers: [] };

  const handlePolygon = useCallback(
    (geometry: Polygon | null) => {
      const next = geometry ?? DEFAULT_POLY;
      setGeom(JSON.stringify(next, null, 2));
    },
    []
  );

  function fmt(value: any, digits = 0) {
    const n = Number(value);
    return Number.isFinite(n)
      ? n.toLocaleString(undefined, { maximumFractionDigits: digits })
      : String(value ?? "—");
  }

  function badgeStyle(kind?: string): CSSProperties {
    const k = (kind || "").toLowerCase();
    const bg = k === "observed" ? "#e6ffed" : k === "manual" ? "#fff4e6" : "#eef2ff";
    const fg = k === "observed" ? "#066a2b" : k === "manual" ? "#8a4608" : "#2b3a67";
    return {
      background: bg,
      color: fg,
      padding: "2px 6px",
      borderRadius: 4,
      fontSize: 12,
      border: "1px solid rgba(0,0,0,0.05)",
      textTransform: "capitalize",
    };
  }

  async function onEstimate() {
    setError(undefined);
    setLoading(true);
    setComps([]);
    try {
      const geometry: Polygon = parsedGeom ?? DEFAULT_POLY;
      const today = new Date();
      const start = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-01`;
      const payload = {
        geometry,
        asset_program: "residential_midrise",
        unit_mix: [{ type: "1BR", count: 10, avg_m2: 60 }],
        finish_level: "mid" as const,
        timeline: { start, months },
        financing_params: { margin_bps: 250, ltv: 0.6 },
        strategy: "build_to_sell" as const,
        city,
        far,
        efficiency: 0.82,
      };
      const res = (await createEstimate(payload)) as EstimateResponse;
      setEstimate(res);
      const since = new Date();
      since.setMonth(since.getMonth() - 12);
      const compsRes = await getComps({ city, type: "land", since: since.toISOString().slice(0, 10) });
      setComps(compsRes?.items || []);
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  async function onScenario() {
    if (!estimate?.id) return;
    try {
      const res = await runScenario(estimate.id, { price_uplift_pct: uplift || 0 });
      alert(`Δ Profit (Saudi Riyal - SAR): ${Math.round(res.delta.p50_profit).toLocaleString()}`);
    } catch (e: any) {
      alert(e?.message || String(e));
    }
  }

  return (
    <div className="app-shell">
      <header className="page-header">
        <div>
          <h1 className="page-title">Oaktree Estimator — Operator Console (v0)</h1>
          <p className="page-subtitle">
            Configure market assumptions, draw the site boundary, and generate an investment estimate in minutes.
          </p>
        </div>
        {freshness && (
          <section className="card freshness-card" aria-label="Data Freshness">
            <h2 className="card-title">Data Freshness</h2>
            <dl className="freshness-grid">
              <div>
                <dt>Construction Cost Index (CCI)</dt>
                <dd>{freshness.cost_index_monthly || "–"}</dd>
              </div>
              <div>
                <dt>Financing Rates</dt>
                <dd>{freshness.rates || "–"}</dd>
              </div>
              <div>
                <dt>Market Indicators</dt>
                <dd>{freshness.market_indicator || "–"}</dd>
              </div>
              <div>
                <dt>Sale Comparables</dt>
                <dd>{freshness.sale_comp || "–"}</dd>
              </div>
            </dl>
          </section>
        )}
      </header>

      <div className="layout-grid">
        <div className="layout-column">
          <section className="card" aria-labelledby="project-inputs-heading">
            <div className="card-header">
              <div>
                <h2 id="project-inputs-heading" className="card-title">Project Inputs</h2>
                <p className="card-subtitle">Define the context for the analysis.</p>
              </div>
            </div>
            <div className="form-grid">
              <label className="form-field" htmlFor="city-input">
                <span>City</span>
                <input id="city-input" value={city} onChange={(e) => setCity(e.target.value)} />
              </label>
              <label className="form-field" htmlFor="far-input">
                <span>Floor Area Ratio (FAR)</span>
                <input
                  id="far-input"
                  type="number"
                  step="0.1"
                  value={far}
                  onChange={(e) => setFar(parseFloat(e.target.value))}
                />
              </label>
              <label className="form-field" htmlFor="timeline-input">
                <span>Development Timeline (months)</span>
                <input
                  id="timeline-input"
                  type="number"
                  value={months}
                  onChange={(e) => setMonths(parseInt(e.target.value || "18", 10))}
                />
              </label>
            </div>

            <div className="action-panel">
              <button className="primary-button" onClick={onEstimate} disabled={loading}>
                {loading ? "Calculating estimate…" : "Run Estimate"}
              </button>
              {estimate?.id && (
                <div className="scenario-panel">
                  <label className="scenario-field" htmlFor="uplift-input">
                    <span>Sale Price Uplift (%)</span>
                    <input
                      id="uplift-input"
                      type="number"
                      value={uplift}
                      onChange={(e) => setUplift(parseFloat(e.target.value || "0"))}
                    />
                  </label>
                  <button className="secondary-button" onClick={onScenario}>
                    Apply price uplift scenario
                  </button>
                  <div className="link-row">
                    <a className="text-link" href={memoPdfUrl(estimate.id)} target="_blank" rel="noreferrer">
                      Open Portable Document Format (PDF) memo
                    </a>
                    <a className="text-link" href={exportCsvUrl(estimate.id)} target="_blank" rel="noreferrer">
                      Download Comma-Separated Values (CSV) export
                    </a>
                  </div>
                </div>
              )}
            </div>
            {error && <p className="error-text">{error}</p>}
          </section>

          <section className="card" aria-labelledby="geometry-heading">
            <div className="card-header">
              <div>
                <h2 id="geometry-heading" className="card-title">Geometry (GeoJSON Polygon)</h2>
                <p className="card-subtitle">Paste or edit the geographic coordinates that define the parcel.</p>
              </div>
            </div>
            <textarea
              id="geometry-input"
              value={geom}
              onChange={(e) => setGeom(e.target.value)}
              className="code-textarea"
            />
            {polygonStats && (
              <dl className="metrics-grid">
                <div>
                  <dt>Approximate Area</dt>
                  <dd>{fmt(polygonStats.areaSqm)} square meters (m²)</dd>
                </div>
                <div>
                  <dt>Approximate Perimeter</dt>
                  <dd>{fmt(polygonStats.perimeterMeters)} meters (m)</dd>
                </div>
                <div>
                  <dt>Vertices</dt>
                  <dd>{polygonStats.vertexCount}</dd>
                </div>
              </dl>
            )}
          </section>
        </div>

        <div className="layout-column">
          <section className="card map-card" aria-labelledby="map-heading">
            <div className="card-header">
              <div>
                <h2 id="map-heading" className="card-title">Site Boundary</h2>
                <p className="card-subtitle">
                  Use the toolbar to draw or refine the site polygon. Click the map to add vertices and finish when the shape is
                  closed.
                </p>
              </div>
            </div>
            <Map polygon={polygonForMap} onPolygon={handlePolygon} />
          </section>
        </div>
      </div>

      {totals && (
        <section className="card full-width" aria-labelledby="financial-summary-heading">
          <div className="card-header">
            <div>
              <h2 id="financial-summary-heading" className="card-title">Financial Summary</h2>
              <p className="card-subtitle">Values are denominated in Saudi Riyal (SAR).</p>
            </div>
          </div>
          <dl className="stat-grid">
            {["land_value", "hard_costs", "soft_costs", "financing", "revenues"].map((key) => (
              <div key={key} className="stat">
                <dt>{
                  key === "land_value"
                    ? "Land value"
                    : key === "hard_costs"
                    ? "Hard costs"
                    : key === "soft_costs"
                    ? "Soft costs"
                    : key === "financing"
                    ? "Financing"
                    : "Revenues"
                }</dt>
                <dd>{Math.round(totals[key]).toLocaleString()}</dd>
              </div>
            ))}
            <div className="stat highlight">
              <dt>Percentile 50 (P50) profit</dt>
              <dd>{Math.round(totals.p50_profit).toLocaleString()}</dd>
            </div>
          </dl>
          {typeof irr === "number" && (
            <p className="metrics-note">Equity Internal Rate of Return (IRR): {(irr * 100).toFixed(1)}%</p>
          )}
          {estimate?.confidence_bands && (
            <p className="metrics-note">
              Percentile 5 (P5) / Percentile 50 (P50) / Percentile 95 (P95) profit: {Math.round(
                estimate.confidence_bands.p5
              ).toLocaleString()} / {Math.round(estimate.confidence_bands.p50).toLocaleString()} / {Math.round(
                estimate.confidence_bands.p95
              ).toLocaleString()}
            </p>
          )}

          {estimate?.land_value_breakdown && (
            <div className="card-subsection">
              <h3 className="section-heading">Land Value Breakdown</h3>
              <dl className="metrics-grid">
                <div>
                  <dt>Hedonic estimate</dt>
                  <dd>{fmt(estimate.land_value_breakdown.hedonic)}</dd>
                </div>
                <div>
                  <dt>Residual estimate</dt>
                  <dd>{fmt(estimate.land_value_breakdown.residual)}</dd>
                </div>
                <div>
                  <dt>Combined value</dt>
                  <dd>{fmt(estimate.land_value_breakdown.combined)}</dd>
                </div>
              </dl>
              <p className="metrics-note">
                Weights — hedonic {fmt(estimate.land_value_breakdown.weights?.hedonic, 2)}, residual {fmt(
                  estimate.land_value_breakdown.weights?.residual,
                  2
                )}; comparables used: {fmt(estimate.land_value_breakdown.comps_used)}
              </p>
            </div>
          )}

          <div className="card-subsection">
            <h3 className="section-heading">Key Assumptions</h3>
            <table className="data-table">
              <thead>
                <tr>
                  <th scope="col">Key</th>
                  <th scope="col">Value</th>
                  <th scope="col">Source</th>
                </tr>
              </thead>
              <tbody>
                {(estimate?.assumptions || []).map((a: any) => (
                  <tr key={a.key}>
                    <td>{a.key}</td>
                    <td className="numeric-cell">
                      {fmt(a.value)} {a.unit || ""}
                    </td>
                    <td>
                      <span style={badgeStyle(a.source_type)}>{a.source_type || "—"}</span>
                    </td>
                  </tr>
                ))}
                {(estimate?.notes?.revenue_lines || []).map((l: any, i: number) => (
                  <tr key={`rev-${i}`}>
                    <td>{l.key}</td>
                    <td className="numeric-cell">
                      {fmt(l.value)} {l.unit || ""}
                    </td>
                    <td>
                      <span style={badgeStyle(l.source_type)}>{l.source_type || "—"}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {(estimate?.explainability?.drivers || estimate?.explainability?.top_comps) && (
            <div className="card-subsection">
              <h3 className="section-heading">Explainability</h3>
              {Array.isArray(estimate?.explainability?.drivers) && estimate.explainability.drivers.length > 0 && (
                <div className="drivers-block">
                  <h4>Drivers</h4>
                  <ul>
                    {estimate.explainability.drivers.map((d: any, i: number) => (
                      <li key={i}>
                        {d.name}: {d.direction} (≈ {fmt(d.magnitude, d.unit === "ratio" ? 2 : 0)} {d.unit || ""})
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {Array.isArray(estimate?.explainability?.top_comps) && estimate.explainability.top_comps.length > 0 && (
                <div className="table-wrapper">
                  <h4>Top Comparables</h4>
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th scope="col">Identifier</th>
                        <th scope="col">Date</th>
                        <th scope="col">City / District</th>
                        <th scope="col">Saudi Riyal per square meter (SAR/m²)</th>
                        <th scope="col">Source</th>
                      </tr>
                    </thead>
                    <tbody>
                      {estimate.explainability.top_comps.map((c: any) => (
                        <tr key={c.id}>
                          <td>{c.id}</td>
                          <td>{c.date}</td>
                          <td>
                            {c.city}
                            {c.district ? ` / ${c.district}` : ""}
                          </td>
                          <td className="numeric-cell">{fmt(c.price_per_m2)}</td>
                          <td>
                            {c.source_url ? (
                              <a href={c.source_url} target="_blank" rel="noreferrer">
                                View source
                              </a>
                            ) : (
                              c.source || "—"
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </section>
      )}

      {rent && (
        <>
          <RentSummary rent={rent} />
          {(rentDrivers.length > 0 || rentComps.length > 0) && (
            <div className="card">
              <h3 className="card-title">Rent Explainability</h3>
              {rentDrivers.length > 0 && (
                <div className="card-subsection drivers-block">
                  <h4>Drivers</h4>
                  <ul>
                    {rentDrivers.map((d: any, i: number) => {
                      const magnitude =
                        typeof d.magnitude === "number"
                          ? fmt(d.magnitude, d.unit === "ratio" ? 2 : 0)
                          : d.magnitude ?? "—";
                      const unitLabel =
                        d.unit && d.unit !== "ratio"
                          ? ` ${d.unit}`
                          : d.unit === "ratio"
                          ? ""
                          : "";
                      return (
                        <li key={i}>
                          {d.name || `Driver ${i + 1}`}:
                          {" "}
                          {d.direction || "—"}
                          {d.magnitude != null || d.unit ? (
                            <>
                              {" "}(≈ {magnitude}
                              {unitLabel})
                            </>
                          ) : null}
                        </li>
                      );
                    })}
                  </ul>
                </div>
              )}
              {rentComps.length > 0 && (
                <div className="overflow-x-auto">
                  <h4 className="card-subtitle">Top Rent Indicators</h4>
                  <table className="table">
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Date</th>
                        <th>City / District</th>
                        <th>SAR/m²/mo</th>
                        <th>Source</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rentComps.map((r: any, index: number) => {
                        const identifier = r.identifier || r.id || r.Identifier || "";
                        const date = r.date || r.Date || "—";
                        const locationParts = [r.city || r.City, r.district || r.District].filter(Boolean);
                        const location = locationParts.length > 0 ? locationParts.join(" / ") : "—";
                        const rentValue =
                          r.rent_ppm2 ??
                          r.rent_per_m2 ??
                          r.sar_per_m2 ??
                          r.price_per_m2 ??
                          r.Rent_SAR_m2_mo ??
                          "—";
                        const source = r.source || r.Source || "—";
                        const sourceContent = r.source_url ? (
                          <a href={r.source_url} target="_blank" rel="noreferrer">
                            View source
                          </a>
                        ) : (
                          source
                        );
                        return (
                          <tr key={`${identifier || "rent"}-${index}`}>
                            <td>{identifier || "—"}</td>
                            <td>{date}</td>
                            <td>{location}</td>
                            <td>
                              {typeof rentValue === "number" ? fmt(rentValue) : rentValue}
                            </td>
                            <td>{sourceContent}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </>
      )}

      {estimate?.id && comps.length > 0 && (
        <section className="card full-width" aria-labelledby="recent-comps-heading">
          <div className="card-header">
            <div>
              <h2 id="recent-comps-heading" className="card-title">Recent Land Comparables in {city}</h2>
              <p className="card-subtitle">Recorded transactions expressed as Saudi Riyal per square meter (SAR/m²).</p>
            </div>
          </div>
          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th scope="col">Identifier</th>
                  <th scope="col">Date</th>
                  <th scope="col">District</th>
                  <th scope="col">Saudi Riyal per square meter (SAR/m²)</th>
                  <th scope="col">Source</th>
                </tr>
              </thead>
              <tbody>
                {comps.map((r) => (
                  <tr key={r.id}>
                    <td>{r.id}</td>
                    <td>{r.date}</td>
                    <td>{r.district || "—"}</td>
                    <td className="numeric-cell">{fmt(r.price_per_m2)}</td>
                    <td>
                      {r.source_url ? (
                        <a href={r.source_url} target="_blank" rel="noreferrer">
                          View source
                        </a>
                      ) : (
                        r.source || "—"
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  );
}
