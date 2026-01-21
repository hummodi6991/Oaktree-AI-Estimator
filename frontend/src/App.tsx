import type { CSSProperties } from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { Polygon } from "geojson";
import { useTranslation } from "react-i18next";
import Map from "./Map";
import { createEstimate, getFreshness, memoPdfUrl, runScenario, getComps, exportCsvUrl } from "./api";
import "./App.css";
import AccessCodeModal from "./components/AccessCodeModal";
import ParkingSummary from "./components/ParkingSummary";
import type { EstimateResponse, RentBlock } from "./lib/types";
import {
  formatAreaM2,
  formatCurrencySAR,
  formatInteger,
  formatNumber,
  formatPercent,
} from "./i18n/format";

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

const DEFAULT_EXCEL_INPUTS = {
  area_ratio: {},
  unit_cost: { residential: 2200, basement: 1200 },
  efficiency: { residential: 0.82 },
  cp_sqm_per_space: { basement: 30 },
  rent_sar_m2_yr: { residential: 2400 },
  fitout_rate: 400,
  contingency_pct: 0.05,
  consultants_pct: 0.06,
  transaction_pct: 0.03,
  land_price_sar_m2: 2800,
};

type PolygonStats = {
  areaSqm: number;
  perimeterMeters: number;
  vertexCount: number;
};

function clampNumber(value: number, min: number, max: number) {
  if (!Number.isFinite(value)) return min;
  return Math.max(min, Math.min(max, value));
}

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
  const { t, i18n } = useTranslation();
  const isArabic = i18n.language.startsWith("ar");
  const defaultCity = useMemo(() => t("ui.defaults.city"), [t]);
  const defaultCityRef = useRef(defaultCity);
  const [freshness, setFreshness] = useState<any>(null);
  const [city, setCity] = useState(defaultCity);
  const [far, setFar] = useState(2.0);
  const [farAuto, setFarAuto] = useState<number | null>(null);
  const [farManuallySet, setFarManuallySet] = useState(false);
  const [farSourceLabel, setFarSourceLabel] = useState<string | null>(null);
  const [months, setMonths] = useState(18);
  const [geom, setGeom] = useState(JSON.stringify(DEFAULT_POLY, null, 2));
  const [geomVer, setGeomVer] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | undefined>();
  const [estimate, setEstimate] = useState<EstimateResponse | null>(null);
  const [comps, setComps] = useState<any[]>([]);
  const [uplift, setUplift] = useState(0);
  const [avgApartmentSize, setAvgApartmentSize] = useState(120);
  const [hasApiKey, setHasApiKey] = useState(() => {
    if (typeof window === "undefined") return false;
    return Boolean(window.localStorage.getItem("oaktree_api_key"));
  });

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
  const excelInputs = useMemo(
    () => ({
      ...DEFAULT_EXCEL_INPUTS,
      parking_assumed_avg_apartment_m2: clampNumber(avgApartmentSize, 20, 600),
    }),
    [avgApartmentSize],
  );

  useEffect(() => {
    getFreshness().then(setFreshness).catch(() => {});
  }, []);

  useEffect(() => {
    function handleStorage(event: StorageEvent) {
      if (event.key === "oaktree_api_key") {
        setHasApiKey(Boolean(event.newValue));
      }
    }
    window.addEventListener("storage", handleStorage);
    return () => window.removeEventListener("storage", handleStorage);
  }, []);

  useEffect(() => {
    if (city === defaultCityRef.current) {
      setCity(defaultCity);
    }
    defaultCityRef.current = defaultCity;
  }, [city, defaultCity]);

  const totals = estimate?.totals;
  const irr = estimate?.metrics?.irr_annual;
  const rentBlock: RentBlock | null = estimate?.rent ?? null;
  const rentDrivers = rentBlock?.drivers ?? [];
  const rentComps = rentBlock?.top_comps?.length
    ? rentBlock.top_comps
    : rentBlock?.top_rent_comparables?.length
    ? rentBlock.top_rent_comparables
    : rentBlock?.rent_comparables ?? [];
  const rentHeadline = rentBlock?.rent_price_per_m2 ?? null;
  const rentUnitRate = rentBlock?.rent_unit_rate ?? null;
  const rentVacancy = rentBlock?.rent_vacancy_pct ?? null;
  const rentGrowth = rentBlock?.rent_growth_pct ?? null;
  const rentHasDrivers = rentDrivers.length > 0;
  const rentHasComps = rentComps.length > 0;
  const strategy = estimate?.strategy ?? "build_to_sell";
  const farNotes = (estimate?.notes as any)?.far_inference ?? {};
  const farUsed = Number.isFinite(farNotes?.far_used) ? farNotes.far_used : null;
  const farMax = Number.isFinite(farNotes?.far_max) ? farNotes.far_max : null;
  const typicalFar = Number.isFinite(farNotes?.typical_far_proxy) ? farNotes.typical_far_proxy : null;
  const overtureSite = (estimate?.notes as any)?.overture_buildings?.site_metrics ?? {};
  const overtureContext = (estimate?.notes as any)?.overture_buildings?.context_metrics ?? {};
  const existingFootprint =
    (estimate?.notes as any)?.existing_footprint_area_m2 ?? overtureSite.footprint_area_m2 ?? null;
  const existingBua = (estimate?.notes as any)?.existing_bua_m2 ?? overtureSite.existing_bua_m2 ?? null;
  const potentialBua = (estimate?.notes as any)?.potential_bua_m2 ?? null;
  const coveragePct =
    typeof overtureSite.coverage_ratio === "number" ? overtureSite.coverage_ratio * 100 : null;

  const handlePolygon = useCallback(
    (geometry: Polygon | null) => {
      const next = geometry ?? DEFAULT_POLY;
      setGeom(JSON.stringify(next, null, 2));
      setGeomVer((v) => v + 1);
    },
    []
  );

  const fallbackValue = t("common.notAvailable");

  const formatNumberValue = (value: any, digits = 0) =>
    formatNumber(value, { maximumFractionDigits: digits, minimumFractionDigits: digits }, fallbackValue);

  const formatIntegerValue = (value: any) => formatInteger(value, fallbackValue);

  const formatPercentValue = (value: number | null | undefined, digits = 1) =>
    formatPercent(value ?? null, { maximumFractionDigits: digits, minimumFractionDigits: digits }, fallbackValue);

  const formatPercentFromUnknown = (value: number | null | undefined, digits = 1) => {
    if (value == null || !Number.isFinite(value)) return fallbackValue;
    const normalized = Math.abs(value) <= 1 ? value : value / 100;
    return formatPercentValue(normalized, digits);
  };

  const formatMaybeNumber = (value: any, digits = 0) => {
    if (value == null) return fallbackValue;
    const num = Number(value);
    if (Number.isFinite(num)) {
      return formatNumberValue(num, digits);
    }
    return String(value);
  };

  const handleAccessCodeSubmit = useCallback((code: string) => {
    window.localStorage.setItem("oaktree_api_key", code);
    setHasApiKey(true);
  }, []);

  const handleAccessCodeClear = useCallback(() => {
    window.localStorage.removeItem("oaktree_api_key");
    setHasApiKey(false);
  }, []);

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
        excel_inputs: excelInputs,
      };
      const res = (await createEstimate(payload)) as EstimateResponse;
      setEstimate(res);
      const farInfo = (res as any)?.notes?.far_inference ?? {};
      const inferredFarRaw = Number.isFinite(farInfo.far_used) ? farInfo.far_used : farInfo.suggested_far;
      const inferredFar = Number.isFinite(inferredFarRaw) ? inferredFarRaw : null;
      setFarAuto(inferredFar);
      if (!farManuallySet && inferredFar != null) {
        setFar(inferredFar);
        setFarSourceLabel("ui.projectInputs.autoPill");
      } else if (farManuallySet) {
        setFarSourceLabel(null);
      }
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
      alert(
        t("ui.scenario.deltaProfitAlert", {
          value: formatCurrencySAR(res.delta.p50_profit),
        }),
      );
    } catch (e: any) {
      alert(e?.message || String(e));
    }
  }

  return (
    <div className={`app-shell${isArabic ? " rtl" : ""}`}>
      <header className="page-header">
        {hasApiKey && (
          <div className="header-actions">
            <button
              type="button"
              className="tertiary-button access-code-control"
              onClick={handleAccessCodeClear}
            >
              Change access code
            </button>
          </div>
        )}
        <div className="page-hero">
          <div className="brand-block">
            <span className="brand-emblem" aria-hidden="true">
              O
            </span>
            <span className="brand-wordmark">{t("ui.brandName")}</span>
          </div>
          <div className="page-intro">
            <h1 className="page-title">{t("ui.heroTitle")}</h1>
            <p className="page-subtitle">
              {t("ui.heroSubtitle")}
            </p>
            <div className="page-meta">
              <span className="version-pill">{t("ui.versionPill")}</span>
              <span>{t("ui.confidentialNote")}</span>
            </div>
          </div>
        </div>
        {freshness && (
          <section className="card freshness-card" aria-label={t("ui.dataFreshness")}>
            <h2 className="card-title">{t("ui.dataFreshness")}</h2>
            <dl className="freshness-grid">
              <div>
                <dt>{t("ui.dataFreshnessLabels.financingRates")}</dt>
                <dd>{freshness.rates || fallbackValue}</dd>
              </div>
              <div>
                <dt>{t("ui.dataFreshnessLabels.marketIndicators")}</dt>
                <dd>{freshness.market_indicator || fallbackValue}</dd>
              </div>
              <div>
                <dt>{t("ui.dataFreshnessLabels.saleComparables")}</dt>
                <dd>{freshness.sale_comp || fallbackValue}</dd>
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
                <h2 id="project-inputs-heading" className="card-title">{t("ui.projectInputs.title")}</h2>
                <p className="card-subtitle">{t("ui.projectInputs.subtitle")}</p>
              </div>
            </div>
            <div className="form-grid">
              <label className="form-field" htmlFor="city-input">
                <span>{t("ui.projectInputs.cityLabel")}</span>
                <input id="city-input" value={city} onChange={(e) => setCity(e.target.value)} />
              </label>
              <label className="form-field" htmlFor="far-input">
                <span className="far-label">
                  {t("ui.projectInputs.farLabel")}
                  {farSourceLabel && <span className="pill auto-pill">{t(farSourceLabel)}</span>}
                </span>
                <input
                  id="far-input"
                  type="number"
                  step="0.1"
                  value={far}
                  onChange={(e) => {
                    const next = parseFloat(e.target.value);
                    setFar(Number.isFinite(next) ? next : 0);
                    setFarManuallySet(true);
                    setFarSourceLabel(null);
                  }}
                />
                {farAuto != null && (
                  <button
                    type="button"
                    className="tertiary-button"
                    onClick={() => {
                      setFar(farAuto);
                      setFarManuallySet(false);
                      setFarSourceLabel("ui.projectInputs.autoPill");
                    }}
                  >
                    {t("ui.projectInputs.useAutoFar", {
                      value: formatNumberValue(farAuto, 2),
                    })}
                  </button>
                )}
              </label>
              <label className="form-field" htmlFor="timeline-input">
                <span>{t("ui.projectInputs.timelineLabel")}</span>
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
                {loading ? t("ui.actions.calculatingEstimate") : t("ui.actions.runEstimate")}
              </button>
              {estimate?.id && (
                <div className="scenario-panel">
                  <label className="scenario-field" htmlFor="uplift-input">
                    <span>{t("ui.scenario.salePriceUpliftLabel")}</span>
                    <input
                      id="uplift-input"
                      type="number"
                      value={uplift}
                      onChange={(e) => setUplift(parseFloat(e.target.value || "0"))}
                    />
                  </label>
                  <button className="secondary-button" onClick={onScenario}>
                    {t("ui.scenario.applyScenario")}
                  </button>
                  <div className="link-row">
                    <a className="text-link" href={memoPdfUrl(estimate.id)} target="_blank" rel="noreferrer">
                      {t("ui.scenario.openPdfMemo")}
                    </a>
                    <a className="text-link" href={exportCsvUrl(estimate.id)} target="_blank" rel="noreferrer">
                      {t("ui.scenario.downloadCsvExport")}
                    </a>
                  </div>
                </div>
              )}
            </div>
            {error && <p className="error-text">{error}</p>}
          </section>

          <section className="card" aria-labelledby="parking-inputs-heading">
            <div className="card-header">
              <div>
                <h2 id="parking-inputs-heading" className="card-title">{t("ui.parking.title")}</h2>
                <p className="card-subtitle">{t("ui.parking.subtitle")}</p>
              </div>
            </div>
            <div className="form-grid">
              <label className="form-field" htmlFor="avg-apartment-size-input">
                <span>{t("ui.parking.apartmentSizeLabel")}</span>
                <input
                  id="avg-apartment-size-input"
                  type="number"
                  min={20}
                  max={600}
                  step="1"
                  value={avgApartmentSize}
                  onChange={(e) => {
                    const next = parseFloat(e.target.value);
                    setAvgApartmentSize(Number.isFinite(next) ? next : 0);
                  }}
                />
                <p className="form-helper-text">
                  {t("ui.parking.helper")}
                </p>
              </label>
            </div>
          </section>

          <section className="card" aria-labelledby="geometry-heading">
            <div className="card-header">
              <div>
                <h2 id="geometry-heading" className="card-title">{t("ui.geometry.title")}</h2>
                <p className="card-subtitle">{t("ui.geometry.subtitle")}</p>
              </div>
            </div>
            <textarea
              key={geomVer}
              id="geometry-input"
              value={geom}
              onChange={(e) => setGeom(e.target.value)}
              className="code-textarea"
            />
            {polygonStats && (
              <dl className="metrics-grid">
                <div>
                  <dt>{t("ui.geometry.approxArea")}</dt>
                  <dd className="numeric-value">{formatAreaM2(polygonStats.areaSqm, { maximumFractionDigits: 0 }, fallbackValue)}</dd>
                </div>
                <div>
                  <dt>{t("ui.geometry.approxPerimeter")}</dt>
                  <dd className="numeric-value">
                    {formatNumberValue(polygonStats.perimeterMeters, 0)} {t("ui.units.meters")}
                  </dd>
                </div>
                <div>
                  <dt>{t("ui.geometry.vertices")}</dt>
                  <dd className="numeric-value">{formatIntegerValue(polygonStats.vertexCount)}</dd>
                </div>
              </dl>
            )}
          </section>
        </div>

        <div className="layout-column">
          <section className="card map-card" aria-labelledby="map-heading">
            <div className="card-header">
              <div>
                <h2 id="map-heading" className="card-title">{t("ui.map.title")}</h2>
                <p className="card-subtitle">
                  {t("ui.map.subtitle")}
                </p>
              </div>
            </div>
            <Map polygon={polygonForMap} onPolygon={handlePolygon} />
          </section>
        </div>
      </div>

      {estimate && (existingFootprint || farUsed || typicalFar || overtureContext?.far_proxy_existing) && (
        <section className="card full-width" aria-labelledby="built-form-heading">
          <div className="card-header">
            <div>
              <h2 id="built-form-heading" className="card-title">{t("ui.builtForm.title")}</h2>
              <p className="card-subtitle">
                {t("ui.builtForm.subtitle", {
                  buffer: formatNumberValue(overtureContext?.buffer_m ?? 500, 0),
                })}
              </p>
            </div>
          </div>
          <dl className="metrics-grid">
            <div>
              <dt>{t("ui.builtForm.builtUpFootprint")}</dt>
              <dd className="numeric-value">
                {existingFootprint != null ? formatAreaM2(existingFootprint, { maximumFractionDigits: 0 }, fallbackValue) : fallbackValue}
              </dd>
            </div>
            <div>
              <dt>{t("ui.builtForm.coverageRatio")}</dt>
              <dd className="numeric-value">
                {coveragePct != null ? formatPercent(coveragePct / 100, { maximumFractionDigits: 1, minimumFractionDigits: 1 }, fallbackValue) : fallbackValue}
              </dd>
            </div>
            <div>
              <dt>{t("ui.builtForm.floorsProxy")}</dt>
              <dd>
                {overtureSite?.floors_median != null || overtureSite?.floors_mean != null
                  ? t("ui.builtForm.floorsProxyValue", {
                    median: formatNumberValue(overtureSite?.floors_median, 1),
                    average: formatNumberValue(overtureSite?.floors_mean, 1),
                  })
                  : fallbackValue}
              </dd>
            </div>
            <div>
              <dt>{t("ui.builtForm.existingBua")}</dt>
              <dd className="numeric-value">
                {existingBua != null ? formatAreaM2(existingBua, { maximumFractionDigits: 0 }, fallbackValue) : fallbackValue}
              </dd>
            </div>
            <div>
              <dt>{t("ui.builtForm.builtDensity")}</dt>
              <dd>
                {overtureSite?.built_density_m2_per_ha != null
                  ? `${formatNumberValue(overtureSite?.built_density_m2_per_ha, 1)} ${t("ui.units.m2PerHa")}`
                  : fallbackValue}
              </dd>
            </div>
            <div>
              <dt>{t("ui.builtForm.suggestedFar")}</dt>
              <dd>
                {farUsed != null ? formatNumberValue(farUsed, 2) : fallbackValue}
                {farMax != null
                  ? t("ui.builtForm.suggestedFarMax", { value: formatNumberValue(farMax, 2) })
                  : ""}
                {typicalFar != null
                  ? t("ui.builtForm.contextProxy", { value: formatNumberValue(typicalFar, 2) })
                  : ""}
              </dd>
            </div>
            {potentialBua != null && (
              <div>
                <dt>{t("ui.builtForm.potentialBua")}</dt>
                <dd className="numeric-value">{formatAreaM2(potentialBua, { maximumFractionDigits: 0 }, fallbackValue)}</dd>
              </div>
            )}
          </dl>
        </section>
      )}

      {totals && (
        <section className="card full-width" aria-labelledby="financial-summary-heading">
          <div className="card-header">
            <div>
              <h2 id="financial-summary-heading" className="card-title">{t("ui.financialSummary.title")}</h2>
              <p className="card-subtitle">{t("ui.financialSummary.subtitle")}</p>
            </div>
          </div>
          <dl className="stat-grid">
            {(["land_value", "hard_costs", "soft_costs", "financing", "revenues"] as const).map((key) => (
              <div key={key} className="stat">
                <dt>
                  {t(
                    key === "land_value"
                      ? "ui.financialSummary.statLabels.landValue"
                      : key === "hard_costs"
                      ? "ui.financialSummary.statLabels.hardCosts"
                      : key === "soft_costs"
                      ? "ui.financialSummary.statLabels.softCosts"
                      : key === "financing"
                      ? "ui.financialSummary.statLabels.financing"
                      : "ui.financialSummary.statLabels.revenues",
                  )}
                </dt>
                <dd className="numeric-value">{formatCurrencySAR(totals[key])}</dd>
              </div>
            ))}
            <div className="stat highlight">
              <dt>{t("ui.financialSummary.p50Profit")}</dt>
              <dd className="numeric-value">{formatCurrencySAR(totals.p50_profit)}</dd>
            </div>
          </dl>
          {typeof irr === "number" && (
            <p className="metrics-note">{t("ui.financialSummary.irrNote", { value: formatPercent(irr, { maximumFractionDigits: 1, minimumFractionDigits: 1 }, fallbackValue) })}</p>
          )}
          {estimate?.confidence_bands && (
            <p className="metrics-note">
              {t("ui.financialSummary.confidenceNote", {
                p5: formatCurrencySAR(estimate.confidence_bands.p5),
                p50: formatCurrencySAR(estimate.confidence_bands.p50),
                p95: formatCurrencySAR(estimate.confidence_bands.p95),
              })}
            </p>
          )}

          {estimate?.land_value_breakdown && (
            <div className="card-subsection">
              <h3 className="section-heading">{t("ui.landValueBreakdown.title")}</h3>
              <dl className="metrics-grid">
                <div>
                  <dt>{t("ui.landValueBreakdown.hedonic")}</dt>
                  <dd className="numeric-value">{formatCurrencySAR(estimate.land_value_breakdown.hedonic)}</dd>
                </div>
                <div>
                  <dt>{t("ui.landValueBreakdown.residual")}</dt>
                  <dd className="numeric-value">{formatCurrencySAR(estimate.land_value_breakdown.residual)}</dd>
                </div>
                <div>
                  <dt>{t("ui.landValueBreakdown.combined")}</dt>
                  <dd className="numeric-value">{formatCurrencySAR(estimate.land_value_breakdown.combined)}</dd>
                </div>
              </dl>
              <p className="metrics-note">
                {t("ui.landValueBreakdown.weightsNote", {
                  hedonic: formatNumberValue(estimate.land_value_breakdown.weights?.hedonic, 2),
                  residual: formatNumberValue(estimate.land_value_breakdown.weights?.residual, 2),
                  comps: formatIntegerValue(estimate.land_value_breakdown.comps_used),
                })}
              </p>
            </div>
          )}

          <div className="card-subsection">
            <h3 className="section-heading">{t("ui.keyAssumptions.title")}</h3>
            <table className="data-table">
              <thead>
                <tr>
                  <th scope="col">{t("ui.keyAssumptions.headers.key")}</th>
                  <th scope="col">{t("ui.keyAssumptions.headers.value")}</th>
                  <th scope="col">{t("ui.keyAssumptions.headers.source")}</th>
                </tr>
              </thead>
              <tbody>
                {(estimate?.assumptions || []).map((a: any) => (
                  <tr key={a.key}>
                    <td>{a.key}</td>
                    <td className="numeric-cell">
                      {formatMaybeNumber(a.value)} {a.unit || ""}
                    </td>
                    <td>
                      <span style={badgeStyle(a.source_type)}>{a.source_type || fallbackValue}</span>
                    </td>
                  </tr>
                ))}
                {(estimate?.notes?.revenue_lines || []).map((l: any, i: number) => (
                  <tr key={`rev-${i}`}>
                    <td>{l.key}</td>
                    <td className="numeric-cell">
                      {formatMaybeNumber(l.value)} {l.unit || ""}
                    </td>
                    <td>
                      <span style={badgeStyle(l.source_type)}>{l.source_type || fallbackValue}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {(estimate?.explainability?.drivers || estimate?.explainability?.top_comps) && (
            <div className="card-subsection">
              <h3 className="section-heading">{t("ui.explainability.title")}</h3>
              {Array.isArray(estimate?.explainability?.drivers) && estimate.explainability.drivers.length > 0 && (
                <div className="drivers-block">
                  <h4>{t("ui.explainability.drivers")}</h4>
                  <ul>
                    {estimate.explainability.drivers.map((d: any, i: number) => (
                      <li key={i}>
                        {d.name}: {d.direction} (≈ {formatNumberValue(d.magnitude, d.unit === "ratio" ? 2 : 0)} {d.unit || ""})
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {Array.isArray(estimate?.explainability?.top_comps) && estimate.explainability.top_comps.length > 0 && (
                <div className="table-wrapper">
                  <h4>{t("ui.explainability.topComparables")}</h4>
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th scope="col">{t("ui.explainability.headers.identifier")}</th>
                        <th scope="col">{t("ui.explainability.headers.date")}</th>
                        <th scope="col">{t("ui.explainability.headers.cityDistrict")}</th>
                        <th scope="col">{t("ui.explainability.headers.sarPerM2")}</th>
                        <th scope="col">{t("ui.explainability.headers.source")}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {estimate.explainability.top_comps.map((c: any) => (
                        <tr key={c.id}>
                          <td>{c.id}</td>
                          <td>{c.date || fallbackValue}</td>
                          <td>
                            {c.city}
                            {c.district ? ` / ${c.district}` : ""}
                          </td>
                          <td className="numeric-cell">{formatNumberValue(c.price_per_m2)}</td>
                          <td>
                            {c.source_url ? (
                              <a href={c.source_url} target="_blank" rel="noreferrer">
                                {t("ui.explainability.viewSource")}
                              </a>
                            ) : (
                              c.source || fallbackValue
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

          {rentBlock && (
            <div className="card-subsection">
              <h3 className="section-heading">
                {strategy === "build_to_rent"
                  ? t("ui.rentSnapshot.title")
                  : t("ui.rentSnapshot.referenceTitle")}
              </h3>
              <dl className="metrics-grid">
                <div>
                  <dt>{t("ui.rentSnapshot.headlineRent")}</dt>
                  <dd className="numeric-value">{rentHeadline != null ? formatNumberValue(rentHeadline) : fallbackValue}</dd>
                </div>
                {rentUnitRate != null && (
                  <div>
                    <dt>{t("ui.rentSnapshot.averageUnitRent")}</dt>
                    <dd className="numeric-value">{formatNumberValue(rentUnitRate)}</dd>
                  </div>
                )}
                {rentVacancy != null && (
                  <div>
                    <dt>{t("ui.rentSnapshot.vacancy")}</dt>
                    <dd className="numeric-value">{formatPercentFromUnknown(rentVacancy, 1)}</dd>
                  </div>
                )}
                {rentGrowth != null && (
                  <div>
                    <dt>{t("ui.rentSnapshot.rentGrowth")}</dt>
                    <dd className="numeric-value">{formatPercentFromUnknown(rentGrowth, 1)}</dd>
                  </div>
                )}
              </dl>
              {rentHasDrivers && (
                <div className="drivers-block">
                  <h4>{t("ui.rentSnapshot.drivers")}</h4>
                  <ul>
                    {rentDrivers.map((d, i) => {
                      const unitLabel = d.unit && d.unit !== "ratio" ? d.unit : "";
                      const digits = d.unit === "ratio" ? 2 : unitLabel === "SAR/m2" ? 0 : 2;
                      return (
                        <li key={`${d.name}-${i}`}>
                          {d.name}: {d.direction} (≈ {formatNumberValue(d.magnitude, digits)} {unitLabel})
                        </li>
                      );
                    })}
                  </ul>
                </div>
              )}
              {rentHasComps && (
                <div className="table-wrapper">
                  <h4>{t("ui.rentSnapshot.topComparables")}</h4>
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th scope="col">{t("ui.rentSnapshot.headers.identifier")}</th>
                        <th scope="col">{t("ui.rentSnapshot.headers.date")}</th>
                        <th scope="col">{t("ui.rentSnapshot.headers.district")}</th>
                        <th scope="col">{t("ui.rentSnapshot.headers.sarPerM2Month")}</th>
                        <th scope="col">{t("ui.rentSnapshot.headers.source")}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rentComps.map((r, index) => (
                        <tr key={`${r.id}-${index}`}>
                          <td>{r.id}</td>
                          <td>{r.date ?? fallbackValue}</td>
                          <td>
                            {r.city}
                            {r.district ? ` / ${r.district}` : ""}
                          </td>
                          <td className="numeric-cell">{formatNumberValue(r.sar_per_m2)}</td>
                          <td>
                            {r.source_url ? (
                              <a href={r.source_url} target="_blank" rel="noreferrer">
                                {t("ui.rentSnapshot.viewSource")}
                              </a>
                            ) : (
                              r.source || fallbackValue
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {!rentHasDrivers && !rentHasComps && rentHeadline == null && rentUnitRate == null &&
                rentVacancy == null && rentGrowth == null && (
                  <p className="metrics-note">{t("ui.rentSnapshot.noIndicators")}</p>
                )}
            </div>
          )}
          <ParkingSummary totals={estimate?.totals} notes={estimate?.notes} />
        </section>
      )}

      {estimate?.id && comps.length > 0 && (
        <section className="card full-width" aria-labelledby="recent-comps-heading">
          <div className="card-header">
            <div>
              <h2 id="recent-comps-heading" className="card-title">
                {t("ui.recentComps.title", { city })}
              </h2>
              <p className="card-subtitle">{t("ui.recentComps.subtitle")}</p>
            </div>
          </div>
          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th scope="col">{t("ui.recentComps.headers.identifier")}</th>
                  <th scope="col">{t("ui.recentComps.headers.date")}</th>
                  <th scope="col">{t("ui.recentComps.headers.district")}</th>
                  <th scope="col">{t("ui.recentComps.headers.sarPerM2")}</th>
                  <th scope="col">{t("ui.recentComps.headers.source")}</th>
                </tr>
              </thead>
              <tbody>
                {comps.map((r) => (
                  <tr key={r.id}>
                    <td>{r.id}</td>
                    <td>{r.date || fallbackValue}</td>
                    <td>{r.district || fallbackValue}</td>
                    <td className="numeric-cell">{formatNumberValue(r.price_per_m2)}</td>
                    <td>
                      {r.source_url ? (
                        <a href={r.source_url} target="_blank" rel="noreferrer">
                          {t("ui.recentComps.viewSource")}
                        </a>
                      ) : (
                        r.source || fallbackValue
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
      <AccessCodeModal isOpen={!hasApiKey} onSubmit={handleAccessCodeSubmit} />
    </div>
  );
}
