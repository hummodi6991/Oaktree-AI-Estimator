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
    distance_m: number;
  }>;
  model_version: string;
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
  rent: { en: "Rent Level", ar: "مستوى الإيجار" },
  parking: { en: "Parking", ar: "المواقف" },
};

function scoreColor(score: number): string {
  if (score >= 75) return "#1a9850";
  if (score >= 50) return "#91cf60";
  if (score >= 25) return "#fc8d59";
  return "#d73027";
}

export default function RestaurantLocationPanel({ lat, lon, onHeatmapData }: Props) {
  const { t, i18n } = useTranslation();
  const isArabic = i18n.language.startsWith("ar");

  const [categories, setCategories] = useState<Category[]>([]);
  const [categoriesLoading, setCategoriesLoading] = useState(true);
  const [selectedCategory, setSelectedCategory] = useState<string>("burger");
  const [scoreResult, setScoreResult] = useState<ScoreResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [heatmapLoading, setHeatmapLoading] = useState(false);
  const [showHeatmap, setShowHeatmap] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Fetch categories on mount — graceful degradation if API unavailable
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
      .catch(() => {
        // API unavailable — keep empty categories, user will see disabled selector
      })
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
        body: JSON.stringify({ lat, lon, category: selectedCategory }),
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
  }, [lat, lon, selectedCategory]);

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
      </div>

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

      {error && <p className="error-text">{error}</p>}

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
                {t("restaurant.demandScore", "Demand")}: {fmtNum(scoreResult.demand_score, 0)}
                {" · "}
                {t("restaurant.costScore", "Cost")}: {fmtNum(scoreResult.cost_penalty, 0)}
                {" · "}
                {t("restaurant.confidence")}: {fmtNum(scoreResult.confidence * 100, 0)}%
              </div>
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
    </section>
  );
}
