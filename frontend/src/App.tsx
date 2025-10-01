import type { CSSProperties } from "react";
import { useCallback, useEffect, useMemo, useState } from "react";
import type { Polygon } from "geojson";
import Map from "./Map";
import { createEstimate, getFreshness, memoPdfUrl, runScenario, getComps } from "./api";

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

export default function App() {
  const [freshness, setFreshness] = useState<any>(null);
  const [city, setCity] = useState("Riyadh");
  const [far, setFar] = useState(2.0);
  const [months, setMonths] = useState(18);
  const [geom, setGeom] = useState(JSON.stringify(DEFAULT_POLY, null, 2));
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | undefined>();
  const [estimate, setEstimate] = useState<any>(null);
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

  useEffect(() => {
    getFreshness().then(setFreshness).catch(() => {});
  }, []);

  const totals = estimate?.totals;
  const irr = estimate?.metrics?.irr_annual;

  const handlePolygon = useCallback(
    (geometry: Polygon | null) => {
      const next = geometry ?? DEFAULT_POLY;
      setGeom(JSON.stringify(next, null, 2));
    },
    []
  );

  function fmt(x: any, digits = 0) {
    const n = Number(x);
    return isFinite(n)
      ? n.toLocaleString(undefined, { maximumFractionDigits: digits })
      : String(x ?? "—");
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
      const res = await createEstimate(payload);
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
      alert(`Δ Profit (SAR): ${Math.round(res.delta.p50_profit).toLocaleString()}`);
    } catch (e: any) {
      alert(e?.message || String(e));
    }
  }

  return (
    <div style={{ maxWidth: 960, margin: "2rem auto", padding: "0 1rem", fontFamily: "system-ui, Arial" }}>
      <h1>Oaktree Estimator — Operator Console (v0)</h1>
      {freshness && (
        <div style={{ padding: "8px 12px", border: "1px solid #ddd", borderRadius: 6, marginBottom: 12 }}>
          <strong>Data Freshness</strong>: CCI {freshness.cost_index_monthly || "–"} · Rates {freshness.rates || "–"} · Indicators {freshness.market_indicator || "–"} · Sale comps {freshness.sale_comp || "–"}
        </div>
      )}

      <section style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 16 }}>
        <div>
          <label htmlFor="city-input">City</label>
          <input id="city-input" value={city} onChange={(e) => setCity(e.target.value)} />
        </div>
        <div>
          <label htmlFor="far-input">FAR</label>
          <input
            id="far-input"
            type="number"
            step="0.1"
            value={far}
            onChange={(e) => setFar(parseFloat(e.target.value))}
          />
        </div>
        <div>
          <label htmlFor="timeline-input">Timeline (months)</label>
          <input
            id="timeline-input"
            type="number"
            value={months}
            onChange={(e) => setMonths(parseInt(e.target.value || "18", 10))}
          />
        </div>
      </section>

      <section style={{ marginTop: 16, marginBottom: 12 }}>
        <label>Draw Site Polygon</label>
        <Map polygon={polygonForMap} onPolygon={handlePolygon} />
      </section>

      <div style={{ marginTop: 16 }}>
        <label htmlFor="geometry-input">Geometry (GeoJSON Polygon)</label>
        <textarea
          id="geometry-input"
          value={geom}
          onChange={(e) => setGeom(e.target.value)}
          style={{ width: "100%", height: 180, fontFamily: "monospace" }}
        />
      </div>

      <div style={{ marginTop: 12, display: "flex", flexWrap: "wrap", alignItems: "center", gap: 12 }}>
        <button onClick={onEstimate} disabled={loading} style={{ padding: "8px 14px" }}>
          {loading ? "Estimating…" : "Run Estimate"}
        </button>
        {estimate?.id && (
          <>
            <button onClick={onScenario} style={{ padding: "8px 14px" }}>
              Scenario: +{uplift}% price
            </button>
            <input
              type="number"
              value={uplift}
              onChange={(e) => setUplift(parseFloat(e.target.value || "0"))}
              style={{ width: 80 }}
            />
            <a href={memoPdfUrl(estimate.id)} target="_blank" rel="noreferrer" style={{ marginLeft: 4 }}>
              Open PDF Memo
            </a>
          </>
        )}
      </div>

      {error && <p style={{ color: "crimson" }}>{error}</p>}

      {totals && (
        <div style={{ marginTop: 20, borderTop: "1px solid #eee", paddingTop: 12 }}>
          <h3>Totals (SAR)</h3>
          <ul>
            <li>Land value: {Math.round(totals.land_value).toLocaleString()}</li>
            <li>Hard costs: {Math.round(totals.hard_costs).toLocaleString()}</li>
            <li>Soft costs: {Math.round(totals.soft_costs).toLocaleString()}</li>
            <li>Financing: {Math.round(totals.financing).toLocaleString()}</li>
            <li>Revenues: {Math.round(totals.revenues).toLocaleString()}</li>
            <li>
              <strong>P50 Profit: {Math.round(totals.p50_profit).toLocaleString()}</strong>
            </li>
          </ul>
          {typeof irr === "number" && <p>Equity IRR (annual): {(irr * 100).toFixed(1)}%</p>}
          {estimate?.confidence_bands && (
            <p>
              P5 / P50 / P95 profit: {Math.round(estimate.confidence_bands.p5).toLocaleString()} / {Math.round(estimate.confidence_bands.p50).toLocaleString()} / {Math.round(estimate.confidence_bands.p95).toLocaleString()}
            </p>
          )}

          {estimate?.land_value_breakdown && (
            <div style={{ marginTop: 12 }}>
              <h4>Land Value Breakdown</h4>
              <ul>
                <li>Hedonic: {fmt(estimate.land_value_breakdown.hedonic)}</li>
                <li>Residual: {fmt(estimate.land_value_breakdown.residual)}</li>
                <li>
                  <strong>Combined: {fmt(estimate.land_value_breakdown.combined)}</strong>
                </li>
              </ul>
              <small>
                Weights — hedonic {fmt(estimate.land_value_breakdown.weights?.hedonic, 2)}, residual {fmt(estimate.land_value_breakdown.weights?.residual, 2)}; comps used: {fmt(estimate.land_value_breakdown.comps_used)}
              </small>
            </div>
          )}

          <div style={{ marginTop: 16 }}>
            <h4>Key Assumptions</h4>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={{ textAlign: "left", borderBottom: "1px solid #eee", padding: "6px 4px" }}>Key</th>
                  <th style={{ textAlign: "right", borderBottom: "1px solid #eee", padding: "6px 4px" }}>Value</th>
                  <th style={{ textAlign: "left", borderBottom: "1px solid #eee", padding: "6px 4px" }}>Source</th>
                </tr>
              </thead>
              <tbody>
                {(estimate?.assumptions || []).map((a: any) => (
                  <tr key={a.key}>
                    <td style={{ padding: "6px 4px" }}>{a.key}</td>
                    <td style={{ padding: "6px 4px", textAlign: "right" }}>
                      {fmt(a.value)} {a.unit || ""}
                    </td>
                    <td style={{ padding: "6px 4px" }}>
                      <span style={badgeStyle(a.source_type)}>{a.source_type || "—"}</span>
                    </td>
                  </tr>
                ))}
                {(estimate?.notes?.revenue_lines || []).map((l: any, i: number) => (
                  <tr key={`rev-${i}`}>
                    <td style={{ padding: "6px 4px" }}>{l.key}</td>
                    <td style={{ padding: "6px 4px", textAlign: "right" }}>
                      {fmt(l.value)} {l.unit || ""}
                    </td>
                    <td style={{ padding: "6px 4px" }}>
                      <span style={badgeStyle(l.source_type)}>{l.source_type || "—"}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {(estimate?.explainability?.drivers || estimate?.explainability?.top_comps) && (
            <div style={{ marginTop: 16 }}>
              <h4>Explainability</h4>
              {Array.isArray(estimate?.explainability?.drivers) && estimate.explainability.drivers.length > 0 && (
                <div style={{ marginBottom: 10 }}>
                  <strong>Drivers</strong>
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
                <div style={{ overflowX: "auto" }}>
                  <strong>Top Comps</strong>
                  <table style={{ width: "100%", borderCollapse: "collapse" }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left", borderBottom: "1px solid #eee", padding: "6px 4px" }}>ID</th>
                        <th style={{ textAlign: "left", borderBottom: "1px solid #eee", padding: "6px 4px" }}>Date</th>
                        <th style={{ textAlign: "left", borderBottom: "1px solid #eee", padding: "6px 4px" }}>City/District</th>
                        <th style={{ textAlign: "right", borderBottom: "1px solid #eee", padding: "6px 4px" }}>SAR/m²</th>
                        <th style={{ textAlign: "left", borderBottom: "1px solid #eee", padding: "6px 4px" }}>Source</th>
                      </tr>
                    </thead>
                    <tbody>
                      {estimate.explainability.top_comps.map((c: any) => (
                        <tr key={c.id}>
                          <td style={{ padding: "6px 4px" }}>{c.id}</td>
                          <td style={{ padding: "6px 4px" }}>{c.date}</td>
                          <td style={{ padding: "6px 4px" }}>
                            {c.city}
                            {c.district ? ` / ${c.district}` : ""}
                          </td>
                          <td style={{ padding: "6px 4px", textAlign: "right" }}>{fmt(c.price_per_m2)}</td>
                          <td style={{ padding: "6px 4px" }}>
                            {c.source_url ? (
                              <a href={c.source_url} target="_blank" rel="noreferrer">
                                link
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
        </div>
      )}

      {estimate?.id && comps.length > 0 && (
        <div style={{ marginTop: 20 }}>
          <h3>Recent Land Comps in {city}</h3>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr>
                <th style={{ textAlign: "left", borderBottom: "1px solid #eee", padding: "6px 4px" }}>ID</th>
                <th style={{ textAlign: "left", borderBottom: "1px solid #eee", padding: "6px 4px" }}>Date</th>
                <th style={{ textAlign: "left", borderBottom: "1px solid #eee", padding: "6px 4px" }}>District</th>
                <th style={{ textAlign: "right", borderBottom: "1px solid #eee", padding: "6px 4px" }}>SAR/m²</th>
                <th style={{ textAlign: "left", borderBottom: "1px solid #eee", padding: "6px 4px" }}>Source</th>
              </tr>
            </thead>
            <tbody>
              {comps.map((r) => (
                <tr key={r.id}>
                  <td style={{ padding: "6px 4px" }}>{r.id}</td>
                  <td style={{ padding: "6px 4px" }}>{r.date}</td>
                  <td style={{ padding: "6px 4px" }}>{r.district || "—"}</td>
                  <td style={{ padding: "6px 4px", textAlign: "right" }}>{fmt(r.price_per_m2)}</td>
                  <td style={{ padding: "6px 4px" }}>
                    {r.source_url ? (
                      <a href={r.source_url} target="_blank" rel="noreferrer">
                        link
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
      )}
    </div>
  );
}
