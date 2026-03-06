import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  fetchCategories,
  fetchOpportunityHeatmap,
  fetchTopCells,
  scoreLocation,
  type RestaurantCategory,
  type TopCell,
  type ScoreResult,
} from "../lib/api/restaurant";

type Props = {
  onHeatmapData: (data: GeoJSON.FeatureCollection | null) => void;
  onFlyTo: (lng: number, lat: number) => void;
  onHighlightCell: (lng: number, lat: number) => void;
  clickedLocation: { lat: number; lng: number } | null;
};

function scoreColor(score: number): string {
  if (score >= 75) return "var(--oak-success, #16a34a)";
  if (score >= 50) return "var(--oak-warning, #f59e0b)";
  if (score >= 25) return "#fc8d59";
  return "var(--oak-error, #d4183d)";
}

export default function RestaurantFinderPanel({
  onHeatmapData,
  onFlyTo,
  onHighlightCell,
  clickedLocation,
}: Props) {
  const { t, i18n } = useTranslation();
  const isArabic = i18n.language.startsWith("ar");

  const [categories, setCategories] = useState<RestaurantCategory[]>([]);
  const [categoriesLoading, setCategoriesLoading] = useState(true);
  const [selectedCategory, setSelectedCategory] = useState("");
  const [heatmapLoading, setHeatmapLoading] = useState(false);
  const [topCells, setTopCells] = useState<TopCell[]>([]);
  const [topCellsLoading, setTopCellsLoading] = useState(false);
  const [scoreResult, setScoreResult] = useState<ScoreResult | null>(null);
  const [scoreLoading, setScoreLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTopCellIdx, setActiveTopCellIdx] = useState<number | null>(null);

  // Debounce timer ref for category changes
  const debounceRef = useRef<number | null>(null);
  const lastCategoryRef = useRef("");

  // Load categories on mount
  useEffect(() => {
    setCategoriesLoading(true);
    fetchCategories()
      .then((cats) => {
        setCategories(cats);
        if (cats.length > 0) setSelectedCategory(cats[0].key);
      })
      .catch(() => setError("Failed to load restaurant categories"))
      .finally(() => setCategoriesLoading(false));
  }, []);

  // Fetch heatmap + top cells when category changes (debounced)
  useEffect(() => {
    if (!selectedCategory || selectedCategory === lastCategoryRef.current) return;
    lastCategoryRef.current = selectedCategory;

    if (debounceRef.current) window.clearTimeout(debounceRef.current);
    debounceRef.current = window.setTimeout(() => {
      loadCategoryData(selectedCategory);
    }, 300);

    return () => {
      if (debounceRef.current) window.clearTimeout(debounceRef.current);
    };
  }, [selectedCategory]);

  const loadCategoryData = useCallback(
    async (category: string) => {
      setError(null);
      setHeatmapLoading(true);
      setTopCellsLoading(true);
      setActiveTopCellIdx(null);

      try {
        const [heatmap, cells] = await Promise.all([
          fetchOpportunityHeatmap(category),
          fetchTopCells(category),
        ]);
        onHeatmapData(heatmap);
        setTopCells(cells);
      } catch (e: any) {
        setError(e?.message || "Failed to load heatmap data");
        onHeatmapData(null);
      } finally {
        setHeatmapLoading(false);
        setTopCellsLoading(false);
      }
    },
    [onHeatmapData],
  );

  // Click-to-score: trigger when a location is clicked on the map
  useEffect(() => {
    if (!clickedLocation || !selectedCategory) return;
    setScoreLoading(true);
    setError(null);
    scoreLocation(clickedLocation.lat, clickedLocation.lng, selectedCategory)
      .then((result) => setScoreResult(result))
      .catch((e: any) => setError(e?.message || "Failed to score location"))
      .finally(() => setScoreLoading(false));
  }, [clickedLocation, selectedCategory]);

  const handleTopCellClick = (cell: TopCell, idx: number) => {
    setActiveTopCellIdx(idx);
    onFlyTo(cell.lon, cell.lat);
    onHighlightCell(cell.lon, cell.lat);
  };

  return (
    <div className="ui-v2-form-wrap">
      {/* Category selector */}
      <div className="oak-card" style={{ marginBottom: 16 }}>
        <div className="oak-card-title">
          {t("restaurant.categoryLabel", { defaultValue: "Restaurant Category" })}
        </div>
        <div className="oak-field">
          <select
            className="oak-select"
            value={selectedCategory}
            disabled={categoriesLoading}
            onChange={(e) => {
              setSelectedCategory(e.target.value);
              setScoreResult(null);
              setTopCells([]);
            }}
          >
            {categoriesLoading && (
              <option>{t("common.loading", { defaultValue: "Loading..." })}</option>
            )}
            {categories.map((c) => (
              <option key={c.key} value={c.key}>
                {isArabic ? c.name_ar : c.name_en}
              </option>
            ))}
          </select>
        </div>
        {(heatmapLoading || topCellsLoading) && (
          <div
            style={{
              marginTop: 8,
              fontSize: "var(--oak-fs-xs)",
              color: "var(--oak-text-light)",
            }}
          >
            {t("restaurant.loadingHeatmap", { defaultValue: "Loading heatmap..." })}
          </div>
        )}
      </div>

      {/* Error display */}
      {error && (
        <div
          style={{
            padding: "8px 12px",
            marginBottom: 12,
            borderRadius: "var(--oak-radius)",
            background: "#fef2f2",
            color: "var(--oak-error)",
            fontSize: "var(--oak-fs-xs)",
            border: "1px solid rgba(212, 24, 61, 0.15)",
          }}
        >
          {error}
        </div>
      )}

      {/* Click-to-score result */}
      {(scoreResult || scoreLoading) && (
        <div
          style={{
            marginBottom: 16,
            background: "#fff",
            borderRadius: 10,
            border: "1px solid rgba(0,0,0,0.07)",
            boxShadow: "0 1px 3px rgba(0,0,0,0.06), 0 1px 10px rgba(0,0,0,0.03)",
            overflow: "hidden",
          }}
        >
          {scoreLoading ? (
            <div style={{ padding: 20, fontSize: "var(--oak-fs-sm)", color: "var(--oak-text-light)" }}>
              {t("restaurant.scoring", { defaultValue: "Scoring..." })}
            </div>
          ) : scoreResult ? (
            <>
              {/* ── Summary header ── */}
              <div
                style={{
                  padding: "14px 16px",
                  background: "var(--oak-secondary)",
                  borderBottom: "1px solid rgba(0,0,0,0.06)",
                  display: "flex",
                  alignItems: "center",
                  gap: 14,
                }}
              >
                {/* Score badge */}
                <div
                  style={{
                    width: 52,
                    height: 52,
                    borderRadius: "50%",
                    background: scoreColor(Number.isFinite(scoreResult.final_score) ? scoreResult.final_score : 0),
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    color: "#fff",
                    fontWeight: 700,
                    fontSize: 20,
                    flexShrink: 0,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.15)",
                  }}
                >
                  {Math.round(Number.isFinite(scoreResult.final_score) ? scoreResult.final_score : 0)}
                </div>

                {/* Title + sub-scores */}
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div
                    style={{
                      fontWeight: 700,
                      fontSize: "var(--oak-fs-base)",
                      color: "var(--oak-text-dark)",
                      lineHeight: 1.2,
                    }}
                  >
                    {t("restaurant.overallScore", { defaultValue: "Opportunity Score" })}
                  </div>
                  <div
                    style={{
                      display: "flex",
                      gap: 16,
                      marginTop: 4,
                      fontSize: "var(--oak-fs-xs)",
                      color: "var(--oak-text-gray)",
                    }}
                  >
                    <span>
                      {t("restaurant.opportunityLabel", { defaultValue: "Opportunity" })}{" "}
                      <strong style={{ color: "var(--oak-text-dark)", fontWeight: 600 }}>
                        {Math.round(Number.isFinite(scoreResult.opportunity_score) ? scoreResult.opportunity_score : 0)}
                      </strong>
                    </span>
                    <span>
                      {t("restaurant.parcelConfidence", { defaultValue: "Confidence" })}{" "}
                      <strong style={{ color: "var(--oak-text-dark)", fontWeight: 600 }}>
                        {Math.round(Number.isFinite(scoreResult.confidence_score) ? scoreResult.confidence_score : 0)}
                      </strong>
                    </span>
                  </div>
                </div>
              </div>

              {/* ── Factor breakdown ── */}
              {scoreResult.contributions && scoreResult.contributions.length > 0 && (
                <div style={{ padding: "12px 16px 14px" }}>
                  <div
                    style={{
                      fontSize: 11,
                      fontWeight: 600,
                      color: "var(--oak-text-light)",
                      textTransform: "uppercase",
                      letterSpacing: "0.04em",
                      marginBottom: 10,
                    }}
                  >
                    {t("restaurant.factorBreakdown", { defaultValue: "Factor Breakdown" })}
                  </div>
                  <div style={{ display: "grid", gap: 8 }}>
                    {scoreResult.contributions.map((c) => (
                      <div
                        key={c.factor}
                        style={{
                          display: "grid",
                          gridTemplateColumns: "110px 1fr 32px",
                          alignItems: "center",
                          gap: 10,
                          fontSize: "var(--oak-fs-xs)",
                        }}
                      >
                        <span
                          style={{
                            color: "var(--oak-text-gray)",
                            fontWeight: 500,
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {c.factor}
                        </span>
                        <div
                          style={{
                            height: 7,
                            background: "#f0f0f0",
                            borderRadius: 4,
                            overflow: "hidden",
                          }}
                        >
                          <div
                            style={{
                              width: `${Math.min(100, Math.max(0, c.score))}%`,
                              height: "100%",
                              background: scoreColor(c.score),
                              borderRadius: 4,
                              transition: "width 300ms ease",
                            }}
                          />
                        </div>
                        <span
                          style={{
                            textAlign: "right",
                            fontWeight: 600,
                            color: "var(--oak-text-dark)",
                            fontVariantNumeric: "tabular-nums",
                          }}
                        >
                          {Math.round(c.score)}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* ── Nearby competitors ── */}
              {scoreResult.nearby_competitors && scoreResult.nearby_competitors.length > 0 && (
                <div
                  style={{
                    padding: "10px 16px 14px",
                    borderTop: "1px solid rgba(0,0,0,0.06)",
                  }}
                >
                  <div
                    style={{
                      fontSize: 11,
                      fontWeight: 600,
                      color: "var(--oak-text-light)",
                      textTransform: "uppercase",
                      letterSpacing: "0.04em",
                      marginBottom: 8,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                    }}
                  >
                    <span>
                      {t("restaurant.nearbyCompetitors", { defaultValue: "Nearby Competitors" })}
                    </span>
                    <span
                      style={{
                        fontSize: 11,
                        fontWeight: 500,
                        color: "var(--oak-text-light)",
                        textTransform: "none",
                        letterSpacing: "normal",
                      }}
                    >
                      {scoreResult.nearby_competitors.length}
                    </span>
                  </div>
                  <div style={{ display: "grid", gap: 0 }}>
                    {scoreResult.nearby_competitors.slice(0, 5).map((comp, i) => (
                      <div
                        key={comp.id}
                        style={{
                          display: "flex",
                          justifyContent: "space-between",
                          alignItems: "center",
                          fontSize: "var(--oak-fs-xs)",
                          padding: "6px 0",
                          borderTop: i === 0 ? "none" : "1px solid rgba(0,0,0,0.05)",
                        }}
                      >
                        <span style={{ color: "var(--oak-text-dark)", fontWeight: 500 }}>
                          {comp.name}
                        </span>
                        <span
                          style={{
                            color: "var(--oak-text-light)",
                            fontVariantNumeric: "tabular-nums",
                            flexShrink: 0,
                            marginLeft: 12,
                          }}
                        >
                          {Math.round(comp.distance_m)}m
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </>
          ) : null}
        </div>
      )}

      {/* Top Opportunity Areas */}
      <div className="oak-card">
        <div className="oak-card-title">
          {t("restaurant.topOpportunityAreas", {
            defaultValue: "Top Opportunity Areas",
          })}
        </div>
        {topCellsLoading ? (
          <div style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)" }}>
            {t("common.loading", { defaultValue: "Loading..." })}
          </div>
        ) : topCells.length === 0 ? (
          <div style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)" }}>
            {t("restaurant.noTopCells", {
              defaultValue: "Select a category to see top opportunity areas",
            })}
          </div>
        ) : (
          <div style={{ display: "grid", gap: 8 }}>
            {topCells.map((cell, idx) => (
              <button
                key={cell.h3_index || idx}
                type="button"
                onClick={() => handleTopCellClick(cell, idx)}
                style={{
                  display: "grid",
                  gridTemplateColumns: "48px 1fr",
                  gap: 10,
                  padding: "10px 12px",
                  borderRadius: "var(--oak-radius)",
                  border:
                    activeTopCellIdx === idx
                      ? "2px solid var(--oak-primary)"
                      : "1px solid var(--oak-outlines)",
                  background:
                    activeTopCellIdx === idx
                      ? "var(--oak-secondary)"
                      : "var(--oak-bg-surface)",
                  cursor: "pointer",
                  textAlign: "left",
                  width: "100%",
                  transition: "border-color 120ms ease, background 120ms ease",
                }}
              >
                <div
                  style={{
                    width: 40,
                    height: 40,
                    borderRadius: "50%",
                    background: scoreColor(Number.isFinite(cell.final_score) ? cell.final_score : 0),
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    color: "#fff",
                    fontWeight: 700,
                    fontSize: "var(--oak-fs-sm)",
                    flexShrink: 0,
                  }}
                >
                  {Math.round(Number.isFinite(cell.final_score) ? cell.final_score : 0)}
                </div>
                <div>
                  <div
                    style={{
                      fontSize: "var(--oak-fs-xs)",
                      fontWeight: 600,
                      color: "var(--oak-text-dark)",
                    }}
                  >
                    #{idx + 1} ·{" "}
                    {cell.area_label ||
                      `${cell.lat.toFixed(4)}, ${cell.lon.toFixed(4)}`}
                  </div>
                  <div
                    style={{
                      fontSize: "var(--oak-fs-xs)",
                      color: "var(--oak-text-light)",
                      marginTop: 2,
                    }}
                  >
                    {t("restaurant.opportunityLabel", { defaultValue: "Opportunity" })}:{" "}
                    {Math.round(Number.isFinite(cell.opportunity_score) ? cell.opportunity_score : 0)} ·{" "}
                    {t("restaurant.cellConfidence", { defaultValue: "Cell confidence" })}:{" "}
                    {Math.round(Number.isFinite(cell.confidence_score) ? cell.confidence_score : 0)}
                  </div>
                </div>
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Instruction hint */}
      <div
        style={{
          marginTop: 12,
          fontSize: "var(--oak-fs-xs)",
          color: "var(--oak-text-light)",
          lineHeight: 1.5,
        }}
      >
        {t("restaurant.clickToScoreHint", {
          defaultValue: "Click anywhere on the map to score a location for this category.",
        })}
      </div>
    </div>
  );
}
