import { useEffect, useState } from "react";
import Map from "./Map";
import { createEstimate, getFreshness, memoPdfUrl, runScenario } from "./api";

const DEFAULT_POLY = {
  type: "Polygon",
  coordinates: [[[46.675,24.713],[46.676,24.713],[46.676,24.714],[46.675,24.714],[46.675,24.713]]]
};

export default function App() {
  const [freshness, setFreshness] = useState<any>(null);
  const [city, setCity] = useState("Riyadh");
  const [far, setFar] = useState(2.0);
  const [months, setMonths] = useState(18);
  const [geom, setGeom] = useState(JSON.stringify(DEFAULT_POLY, null, 2));
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string|undefined>();
  const [estimate, setEstimate] = useState<any>(null);
  const [uplift, setUplift] = useState(0);
  const parsedGeom = (() => { try { return JSON.parse(geom); } catch { return null; } })();

  useEffect(() => {
    getFreshness().then(setFreshness).catch(() => {});
  }, []);

  const totals = estimate?.totals;
  const irr = estimate?.metrics?.irr_annual;

  async function onEstimate() {
    setError(undefined);
    setLoading(true);
    try {
      const geometry = JSON.parse(geom);
      const today = new Date();
      const start = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,"0")}-01`;
      const payload = {
        geometry,
        asset_program: "residential_midrise",
        unit_mix: [{ type: "1BR", count: 10, avg_m2: 60 }],
        finish_level: "mid",
        timeline: { start, months },
        financing_params: { margin_bps: 250, ltv: 0.6 },
        strategy: "build_to_sell",
        city,
        far,
        efficiency: 0.82
      };
      const res = await createEstimate(payload);
      setEstimate(res);
    } catch (e:any) {
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
    } catch (e:any) {
      alert(e?.message || String(e));
    }
  }

  return (
    <div style={{ maxWidth: 920, margin: "2rem auto", padding: "0 1rem", fontFamily: "system-ui, Arial" }}>
      <h1>Oaktree Estimator — Operator Console (v0)</h1>
      {freshness && (
        <div style={{ padding: "8px 12px", border: "1px solid #ddd", borderRadius: 6, marginBottom: 12 }}>
          <strong>Data Freshness</strong>: CCI {freshness.cost_index_monthly || "–"} · Rates {freshness.rates || "–"} · Indicators {freshness.market_indicator || "–"} · Sale comps {freshness.sale_comp || "–"}
        </div>
      )}

      <section style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        <div>
          <label>City</label><br/>
          <input value={city} onChange={e => setCity(e.target.value)} />
        </div>
        <div>
          <label>FAR</label><br/>
          <input type="number" step="0.1" value={far} onChange={e => setFar(parseFloat(e.target.value))}/>
        </div>
        <div>
          <label>Timeline (months)</label><br/>
          <input type="number" value={months} onChange={e => setMonths(parseInt(e.target.value || "18",10))}/>
        </div>
      </section>

      <section style={{ marginTop: 8, marginBottom: 12 }}>
        <label>Draw Site Polygon</label>
        <Map initial={parsedGeom} onChange={(g:any) => g && setGeom(JSON.stringify(g, null, 2))} />
      </section>

      <div style={{ marginTop: 16 }}>
        <label>Geometry (GeoJSON Polygon)</label>
        <textarea value={geom} onChange={e => setGeom(e.target.value)} style={{ width: "100%", height: 160, fontFamily: "monospace" }}/>
      </div>

      <div style={{ marginTop: 12 }}>
        <button onClick={onEstimate} disabled={loading} style={{ padding: "8px 14px" }}>
          {loading ? "Estimating…" : "Run Estimate"}
        </button>
        {estimate?.id && (
          <>
            <button onClick={onScenario} style={{ padding: "8px 14px", marginLeft: 8 }}>
              Scenario: +{uplift}% price
            </button>
            <input type="number" value={uplift} onChange={e => setUplift(parseFloat(e.target.value||"0"))} style={{ width: 80, marginLeft: 8 }}/>
            <a href={memoPdfUrl(estimate.id)} target="_blank" rel="noreferrer" style={{ marginLeft: 12 }}>
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
            <li><strong>P50 Profit: {Math.round(totals.p50_profit).toLocaleString()}</strong></li>
          </ul>
          {typeof irr === "number" && <p>Equity IRR (annual): {(irr*100).toFixed(1)}%</p>}
          {estimate?.confidence_bands && (
            <p>P5 / P50 / P95 profit: {Math.round(estimate.confidence_bands.p5).toLocaleString()} / {Math.round(estimate.confidence_bands.p50).toLocaleString()} / {Math.round(estimate.confidence_bands.p95).toLocaleString()}</p>
          )}
        </div>
      )}
    </div>
  );
}
