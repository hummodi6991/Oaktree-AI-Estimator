import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { fetchWithAuth, buildApiUrl } from "../api";
import { formatNumber } from "../i18n/format";

type Category = { key: string; name_en: string; name_ar: string };

type ScoreResult = {
  opportunity_score: number;
  demand_score: number;
  cost_penalty: number;
  factors: Record<string, number>;
  contributions: Array<{
    factor: string;
    score: number;
    weight: number;
    weighted_contribution: number;
  }>;
  confidence: number;
  nearby_competitors: Array<{
    id: string;
    name: string;
    category: string;
    rating: number | null;
    source: string;
    chain_name?: string;
    distance_m: number;
  }>;
  model_version: string;
  ai_weights_used: boolean;
};

type TopParcel = {
  h3_index: string;
  lat: number;
  lon: number;
  opportunity_score: number;
  demand_score: number;
  cost_penalty: number;
  confidence: number;
  top_factors: Array<{
    factor: string;
    score: number;
    weight: number;
    weighted_contribution: number;
  }>;
  competitor_count: number;
  model_version: string;
};

type TopParcelsResult = {
  category: string;
  chain_name: string | null;
  total_cells_evaluated: number;
  resolution: number;
  parcels: TopParcel[];
};

type DataSource = {
  source: string;
  label: string;
  url: string;
  poi_count: number;
  status: string;
};

type DataSourcesResult = {
  total_pois: number;
  sources: DataSource[];
  platform_count: number;
};

type Props = {
  lat: number | null;
  lon: number | null;
  onHeatmapData: (data: GeoJSON.FeatureCollection | null) => void;
};

const FACTOR_LABELS: Record<string, { en: string; ar: string }> = {
  competition: { en: "Competition", ar: "المنافسة" },
  complementary: { en: "Dining Cluster", ar: "تجمع المطاعم" },
  population: { en: "Population", ar: "الكثافة السكانية" },
  traffic: { en: "Traffic", ar: "حركة المرور" },
  road_frontage: { en: "Road Frontage", ar: "واجهة الطريق" },
  commercial_density: { en: "Commercial Area", ar: "المنطقة التجارية" },
  delivery_demand: { en: "Delivery Demand", ar: "طلب التوصيل" },
  competitor_rating: { en: "Competitor Quality", ar: "جودة المنافسين" },
  anchor_proximity: { en: "Anchor Proximity", ar: "قرب المراكز" },
  foot_traffic: { en: "Foot Traffic", ar: "حركة المشاة" },
  chain_gap: { en: "Chain Gap", ar: "فجوة السلسلة" },
  income_proxy: { en: "Income Level", ar: "مستوى الدخل" },
  rent: { en: "Rent Level", ar: "مستوى الإيجار" },
  parking: { en: "Parking", ar: "المواقف" },
  zoning_fit: { en: "Zoning Fit", ar: "ملاءمة التنظيم" },
};

function scoreColor(score: number): string {
  if (score >= 75) return "#1a9850";
  if (score >= 50) return "#91cf60";
  if (score >= 25) return "#fc8d59";
  return "#d73027";
}

type Tab = "score" | "top-parcels" | "data-sources";

export default function RestaurantLocationPanel({ lat, lon, onHeatmapData }: Props) {
  const { t, i18n } = useTranslation();
  const isArabic = i18n.language.startsWith("ar");

  const [categories, setCategories] = useState<Category[]>([]);
  const [categoriesLoading, setCategoriesLoading] = useState(true);
  const [selectedCategory, setSelectedCategory] = useState<string>("burger");
  const [chainName, setChainName] = useState<string>("");
  const [scoreResult, setScoreResult] = useState<ScoreResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [heatmapLoading, setHeatmapLoading] = useState(false);
  const [showHeatmap, setShowHeatmap] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<Tab>("score");

  // Top parcels state
  const [topParcels, setTopParcels] = useState<TopParcelsResult | null>(null);
  const [topParcelsLoading, setTopParcelsLoading] = useState(false);

  // Data sources state
  const [dataSources, setDataSources] = useState<DataSourcesResult | null>(null);
  const [dataSourcesLoading, setDataSourcesLoading] = useState(false);

  // Fetch categories on mount
  useEffect(() => {
    setCategoriesLoading(true);
    fetchWithAuth(buildApiUrl("/v1/restaurant/categories"))
      .then((res) => res.json())
      .then((data) => {
        if (Array.isArray(data) && data.length > 0) {
          setCategories(data);
          setSelectedCategory(data[0].key);
        }
      })
      .catch(() => {})
      .finally(() => setCategoriesLoading(false));
  }, []);

  const handleScore = useCallback(async () => {
    if (lat == null || lon == null) return;
    setLoading(true);
    setError(null);
    try {
      const res = await fetchWithAuth(buildApiUrl("/v1/restaurant/score"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          lat,
          lon,
          category: selectedCategory,
          chain_name: chainName || undefined,
          use_ai_weights: true,
        }),
      });
      if (!res.ok) {
        const detail = await res.text();
        throw new Error(detail || `HTTP ${res.status}`);
      }
      const data = await res.json();
      setScoreResult(data);
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setLoading(false);
    }
  }, [lat, lon, selectedCategory, chainName]);

  const handleToggleHeatmap = useCallback(async () => {
    if (showHeatmap) {
      setShowHeatmap(false);
      onHeatmapData(null);
      return;
    }
    if (lat == null || lon == null) return;
    setHeatmapLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        category: selectedCategory,
        min_lon: String(lon - 0.05),
        min_lat: String(lat - 0.05),
        max_lon: String(lon + 0.05),
        max_lat: String(lat + 0.05),
        resolution: "8",
      });
      const res = await fetchWithAuth(
        buildApiUrl(`/v1/restaurant/heatmap?${params.toString()}`),
      );
      if (!res.ok) {
        const detail = await res.text();
        throw new Error(detail || `HTTP ${res.status}`);
      }
      const data = await res.json();
      onHeatmapData(data);
      setShowHeatmap(true);
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setHeatmapLoading(false);
    }
  }, [lat, lon, selectedCategory, showHeatmap, onHeatmapData]);

  const handleFindTopParcels = useCallback(async () => {
    if (lat == null || lon == null) return;
    setTopParcelsLoading(true);
    setError(null);
    try {
      const res = await fetchWithAuth(buildApiUrl("/v1/restaurant/top-parcels"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          category: selectedCategory,
          chain_name: chainName || undefined,
          limit: 15,
          min_lat: lat - 0.15,
          max_lat: lat + 0.15,
          min_lon: lon - 0.15,
          max_lon: lon + 0.15,
          resolution: 8,
        }),
      });
      if (!res.ok) {
        const detail = await res.text();
        throw new Error(detail || `HTTP ${res.status}`);
      }
      const data = await res.json();
      setTopParcels(data);
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setTopParcelsLoading(false);
    }
  }, [lat, lon, selectedCategory, chainName]);

  const handleLoadDataSources = useCallback(async () => {
    if (dataSources) return;
    setDataSourcesLoading(true);
    try {
      const res = await fetchWithAuth(buildApiUrl("/v1/restaurant/data-sources"));
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setDataSources(data);
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setDataSourcesLoading(false);
    }
  }, [dataSources]);

  useEffect(() => {
    if (activeTab === "data-sources") {
      handleLoadDataSources();
    }
  }, [activeTab, handleLoadDataSources]);

  const fallback = t("common.notAvailable");
  const fmtNum = (v: number | null | undefined, digits = 0) =>
    formatNumber(
      v ?? null,
      { maximumFractionDigits: digits, minimumFractionDigits: digits },
      fallback,
    );

  return (
    <section className="card" aria-labelledby="restaurant-location-heading">
      <div className="card-header">
        <div>
          <h2 id="restaurant-location-heading" className="card-title">
            {t("restaurant.title")}
          </h2>
          <p className="card-subtitle">{t("restaurant.subtitle")}</p>
        </div>
      </div>

      {/* Tab bar */}
      <div style={{ display: "flex", gap: 0, borderBottom: "1px solid #e5e7eb", marginBottom: 12 }}>
        {(["score", "top-parcels", "data-sources"] as Tab[]).map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            style={{
              padding: "8px 16px",
              border: "none",
              background: "none",
              cursor: "pointer",
              borderBottom: activeTab === tab ? "2px solid #2563eb" : "2px solid transparent",
              color: activeTab === tab ? "#2563eb" : "#6b7280",
              fontWeight: activeTab === tab ? 600 : 400,
              fontSize: 13,
            }}
          >
            {tab === "score" && t("restaurant.tabScore")}
            {tab === "top-parcels" && t("restaurant.tabTopParcels")}
            {tab === "data-sources" && t("restaurant.tabDataSources")}
          </button>
        ))}
      </div>

      {/* Category + chain selector */}
      <div className="form-grid">
        <label className="form-field" htmlFor="restaurant-category">
          <span>{t("restaurant.categoryLabel")}</span>
          <select
            id="restaurant-category"
            value={selectedCategory}
            disabled={categoriesLoading || categories.length === 0}
            onChange={(e) => {
              setSelectedCategory(e.target.value);
              setScoreResult(null);
              setTopParcels(null);
            }}
          >
            {categoriesLoading && <option>{t("common.loading", "Loading...")}</option>}
            {!categoriesLoading && categories.length === 0 && (
              <option>{t("common.notAvailable")}</option>
            )}
            {categories.map((c) => (
              <option key={c.key} value={c.key}>
                {isArabic ? c.name_ar : c.name_en}
              </option>
            ))}
          </select>
        </label>

        <label className="form-field" htmlFor="restaurant-chain">
          <span>{t("restaurant.chainNameLabel")}</span>
          <input
            id="restaurant-chain"
            type="text"
            value={chainName}
            onChange={(e) => setChainName(e.target.value)}
            placeholder={t("restaurant.chainNamePlaceholder")}
            style={{ padding: "6px 10px", border: "1px solid #d1d5db", borderRadius: 6, fontSize: 13 }}
          />
        </label>
      </div>

      {error && <p className="error-text">{error}</p>}

      {/* ========== SCORE TAB ========== */}
      {activeTab === "score" && (
        <>
          <div className="action-panel" style={{ gap: 8, flexWrap: "wrap" }}>
            <button
              className="primary-button"
              onClick={handleScore}
              disabled={loading || lat == null || categoriesLoading || categories.length === 0}
            >
              {loading ? t("restaurant.scoring") : t("restaurant.scoreLocation")}
            </button>
            <button
              className="secondary-button"
              onClick={handleToggleHeatmap}
              disabled={heatmapLoading || lat == null || categoriesLoading || categories.length === 0}
            >
              {heatmapLoading
                ? t("restaurant.loadingHeatmap")
                : showHeatmap
                  ? t("restaurant.hideHeatmap")
                  : t("restaurant.showHeatmap")}
            </button>
          </div>

          {scoreResult && (
            <div className="card-subsection" style={{ marginTop: 16 }}>
              {/* Headline score circle + sub-scores */}
              <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 12 }}>
                <div
                  style={{
                    width: 56,
                    height: 56,
                    borderRadius: "50%",
                    background: scoreColor(scoreResult.opportunity_score),
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    color: "#fff",
                    fontWeight: 700,
                    fontSize: 20,
                  }}
                >
                  {Math.round(scoreResult.opportunity_score)}
                </div>
                <div>
                  <div style={{ fontWeight: 600, fontSize: 16 }}>
                    {t("restaurant.overallScore")}
                  </div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>
                    {t("restaurant.demandScore")}: {fmtNum(scoreResult.demand_score, 0)}
                    {" · "}
                    {t("restaurant.costScore")}: {fmtNum(scoreResult.cost_penalty, 0)}
                    {" · "}
                    {t("restaurant.confidence")}: {fmtNum(scoreResult.confidence * 100, 0)}%
                  </div>
                  {scoreResult.ai_weights_used && (
                    <div style={{ fontSize: 11, color: "#2563eb", marginTop: 2 }}>
                      {t("restaurant.aiWeightsActive")}
                    </div>
                  )}
                </div>
              </div>

              <h3 className="section-heading">{t("restaurant.factorBreakdown")}</h3>
              <div style={{ display: "grid", gap: 4 }}>
                {Object.entries(scoreResult.factors).map(([key, value]) => {
                  const label = FACTOR_LABELS[key];
                  return (
                    <div
                      key={key}
                      style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13 }}
                    >
                      <span style={{ minWidth: 130 }}>
                        {label ? (isArabic ? label.ar : label.en) : key}
                      </span>
                      <div
                        style={{
                          flex: 1,
                          height: 8,
                          background: "#eee",
                          borderRadius: 4,
                          overflow: "hidden",
                        }}
                      >
                        <div
                          style={{
                            width: `${value}%`,
                            height: "100%",
                            background: scoreColor(value),
                            borderRadius: 4,
                          }}
                        />
                      </div>
                      <span style={{ minWidth: 32, textAlign: "right", fontWeight: 500 }}>
                        {Math.round(value)}
                      </span>
                    </div>
                  );
                })}
              </div>

              {scoreResult.nearby_competitors.length > 0 && (
                <>
                  <h3 className="section-heading" style={{ marginTop: 16 }}>
                    {t("restaurant.nearbyCompetitors")} ({scoreResult.nearby_competitors.length})
                  </h3>
                  <div className="table-wrapper">
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>{t("restaurant.competitorName")}</th>
                          <th>{t("restaurant.competitorRating")}</th>
                          <th>{t("restaurant.competitorDistance")}</th>
                          <th>{t("restaurant.competitorSource")}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {scoreResult.nearby_competitors.slice(0, 10).map((c) => (
                          <tr key={c.id}>
                            <td>{c.name}</td>
                            <td className="numeric-cell">
                              {c.rating != null ? fmtNum(c.rating, 1) : fallback}
                            </td>
                            <td className="numeric-cell">{fmtNum(c.distance_m, 0)} m</td>
                            <td>{c.source}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </div>
          )}
        </>
      )}

      {/* ========== TOP PARCELS TAB ========== */}
      {activeTab === "top-parcels" && (
        <div>
          <div className="action-panel" style={{ gap: 8 }}>
            <button
              className="primary-button"
              onClick={handleFindTopParcels}
              disabled={topParcelsLoading || lat == null || categoriesLoading || categories.length === 0}
            >
              {topParcelsLoading ? t("restaurant.findingTopParcels") : t("restaurant.findTopParcels")}
            </button>
          </div>

          {topParcels && (
            <div className="card-subsection" style={{ marginTop: 12 }}>
              <div style={{ fontSize: 12, opacity: 0.7, marginBottom: 8 }}>
                {t("restaurant.cellsEvaluated")}: {topParcels.total_cells_evaluated}
              </div>

              <div className="table-wrapper">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>{t("restaurant.topScore")}</th>
                      <th>{t("restaurant.demandScore")}</th>
                      <th>{t("restaurant.costScore")}</th>
                      <th>{t("restaurant.topCompetitors")}</th>
                      <th>{t("restaurant.topKeyFactor")}</th>
                      <th>{t("restaurant.topCoordinates")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topParcels.parcels.map((p, idx) => (
                      <tr key={p.h3_index}>
                        <td>{idx + 1}</td>
                        <td>
                          <span
                            style={{
                              display: "inline-block",
                              width: 28,
                              height: 28,
                              borderRadius: "50%",
                              background: scoreColor(p.opportunity_score),
                              color: "#fff",
                              textAlign: "center",
                              lineHeight: "28px",
                              fontWeight: 600,
                              fontSize: 12,
                            }}
                          >
                            {Math.round(p.opportunity_score)}
                          </span>
                        </td>
                        <td className="numeric-cell">{fmtNum(p.demand_score, 0)}</td>
                        <td className="numeric-cell">{fmtNum(p.cost_penalty, 0)}</td>
                        <td className="numeric-cell">{p.competitor_count}</td>
                        <td style={{ fontSize: 12 }}>
                          {p.top_factors[0]
                            ? (FACTOR_LABELS[p.top_factors[0].factor]
                                ? (isArabic
                                    ? FACTOR_LABELS[p.top_factors[0].factor].ar
                                    : FACTOR_LABELS[p.top_factors[0].factor].en)
                                : p.top_factors[0].factor)
                            : fallback}
                        </td>
                        <td style={{ fontSize: 11, fontFamily: "monospace" }}>
                          {p.lat.toFixed(4)}, {p.lon.toFixed(4)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}

      {/* ========== DATA SOURCES TAB ========== */}
      {activeTab === "data-sources" && (
        <div>
          {dataSourcesLoading && (
            <p style={{ fontSize: 13, opacity: 0.6 }}>{t("common.loading", "Loading...")}</p>
          )}

          {dataSources && (
            <div className="card-subsection">
              <div style={{ fontSize: 13, marginBottom: 12 }}>
                <strong>{t("restaurant.totalPOIs")}:</strong> {dataSources.total_pois.toLocaleString()}
                {" · "}
                <strong>{t("restaurant.platformCount")}:</strong> {dataSources.platform_count}
              </div>

              <div className="table-wrapper">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>{t("restaurant.sourceName")}</th>
                      <th>{t("restaurant.sourcePOICount")}</th>
                      <th>{t("restaurant.sourceStatus")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dataSources.sources.map((s) => (
                      <tr key={s.source}>
                        <td>
                          {s.url ? (
                            <a
                              href={s.url}
                              target="_blank"
                              rel="noopener noreferrer"
                              style={{ color: "#2563eb", textDecoration: "none" }}
                            >
                              {s.label}
                            </a>
                          ) : (
                            s.label
                          )}
                        </td>
                        <td className="numeric-cell">{s.poi_count.toLocaleString()}</td>
                        <td>
                          <span
                            style={{
                              display: "inline-block",
                              padding: "2px 8px",
                              borderRadius: 12,
                              fontSize: 11,
                              fontWeight: 500,
                              background: s.status === "active" ? "#dcfce7" : "#fef9c3",
                              color: s.status === "active" ? "#166534" : "#854d0e",
                            }}
                          >
                            {s.status === "active"
                              ? t("restaurant.statusActive")
                              : t("restaurant.statusPending")}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}
    </section>
  );
}
