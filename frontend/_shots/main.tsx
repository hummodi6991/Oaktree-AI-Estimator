/*
 * Screenshot harness (dev-only, not shipped in the app build).
 *
 * Renders the REAL Expansion Advisor components with representative Riyadh
 * sample data so we can capture authentic UI screenshots for the user guide.
 * Pick a panel with ?panel=brief|results|compare|report.
 */
import React from "react";
import { createRoot } from "react-dom/client";
import "../src/i18n";

// Mirror the global stylesheets that src/main.tsx loads, so components render
// with the production design tokens / look-and-feel.
import "../src/App.css";
import "../src/index.css";
import "../src/ui-fixes.css";
import "../src/styles/figma-tokens.css";
import "../src/styles/theme.css";
import "../src/styles/global.css";
import "../src/styles/design-system.css";
import "../src/styles/ui-v2.css";
import "../src/styles/ui-figma.css";
import "../src/styles/atlas-ui.css";
import "../src/features/expansion-advisor/expansion-advisor.css";

import ExpansionResultsPanel from "../src/features/expansion-advisor/ExpansionResultsPanel";
import ExpansionComparePanel from "../src/features/expansion-advisor/ExpansionComparePanel";
import ExpansionReportPanel from "../src/features/expansion-advisor/ExpansionReportPanel";
import ExpansionBriefForm, { defaultBrief } from "../src/features/expansion-advisor/ExpansionBriefForm";

// ── Stub network so the Brief form's district fetch resolves instantly. ──
const districts = [
  { value: "al_olaya", label: "Al Olaya", label_ar: "العليا", aliases: [] },
  { value: "al_malqa", label: "Al Malqa", label_ar: "الملقا", aliases: [] },
  { value: "hittin", label: "Hittin", label_ar: "حطين", aliases: [] },
  { value: "al_narjis", label: "Al Narjis", label_ar: "النرجس", aliases: [] },
  { value: "al_yasmin", label: "Al Yasmin", label_ar: "الياسمين", aliases: [] },
];
window.fetch = (async (url: RequestInfo | URL) => {
  const u = String(url);
  let body: unknown = { items: [] };
  if (u.includes("districts")) body = { items: districts };
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}) as typeof window.fetch;

// ── Sample candidates (Riyadh) ──────────────────────────────────────────
const mk = (o: Record<string, unknown>) => ({
  search_id: "search_demo",
  parcel_id: "p",
  lat: 24.71,
  lon: 46.67,
  rerank_applied: false,
  rerank_delta: 0,
  rerank_status: null,
  rerank_reason: null,
  top_positives_json: [],
  top_risks_json: [],
  feature_snapshot_json: {},
  ...o,
});

const candidates = [
  mk({
    id: "c1",
    rank_position: 1,
    final_rank: 1,
    final_score: 87,
    confidence_grade: "A",
    gate_status_json: { overall_pass: true },
    source_type: "commercial_unit",
    source_tier: 1,
    platform: "aqar",
    rent_confidence: "actual",
    listing_url: "https://example.com/listing",
    district_display: "Al Olaya",
    area_m2: 220,
    unit_area_sqm: 220,
    unit_price_sar_annual: 480000,
    display_annual_rent_sar: 480000,
    estimated_annual_rent_sar: 480000,
    unit_street_width_m: 25,
    estimated_fitout_cost_sar: 650000,
    value_band: "best_value",
    distance_to_nearest_branch_m: 3200,
    top_positives_json: ["High footfall near offices & retail core"],
    top_risks_json: ["Premium rent in a competitive corridor"],
    feature_snapshot_json: {
      listing_age: { created_days: 3, updated_days: 1 },
      district_momentum: { momentum_score: 82, sample_floor_applied: false },
    },
  }),
  mk({
    id: "c2",
    rank_position: 2,
    final_rank: 2,
    final_score: 79,
    confidence_grade: "B",
    gate_status_json: { overall_pass: true },
    source_type: "poi",
    source_tier: 2,
    cl_avg_rating: 4.3,
    district_display: "Al Malqa",
    area_m2: 180,
    display_annual_rent_sar: 360000,
    estimated_annual_rent_sar: 360000,
    estimated_fitout_cost_sar: 520000,
    distance_to_nearest_branch_m: 4100,
    top_positives_json: ["Strong residential demand, few direct rivals"],
    top_risks_json: ["Moderate visibility from the main road"],
  }),
  mk({
    id: "c3",
    rank_position: 3,
    final_rank: 3,
    final_score: 72,
    confidence_grade: "B",
    gate_status_json: { overall_pass: true },
    source_type: "arcgis",
    source_tier: 3,
    district_display: "Hittin",
    area_m2: 300,
    display_annual_rent_sar: 410000,
    estimated_annual_rent_sar: 410000,
    estimated_fitout_cost_sar: 600000,
    top_positives_json: ["Open whitespace for the category"],
    top_risks_json: ["Rent estimate based on area comparables"],
  }),
  mk({
    id: "c4",
    rank_position: 4,
    final_rank: 4,
    final_score: 58,
    confidence_grade: "C",
    gate_status_json: { overall_pass: null },
    source_type: "arcgis",
    source_tier: 3,
    district_display: "Al Narjis",
    area_m2: 140,
    display_annual_rent_sar: 300000,
    estimated_annual_rent_sar: 300000,
    estimated_fitout_cost_sar: 430000,
    value_band: "above_market",
    value_band_low_confidence: true,
    top_positives_json: ["Emerging residential catchment"],
    top_risks_json: ["Some site checks need a field visit"],
  }),
];

// ── Compare fixture ──────────────────────────────────────────────────────
const compareItem = (c: Record<string, unknown>, extra: Record<string, unknown>) => ({
  candidate_id: c.id,
  rank_position: c.rank_position,
  final_score: c.final_score,
  confidence_grade: c.confidence_grade,
  gate_status_json: c.gate_status_json,
  gate_verdict: (c.gate_status_json as { overall_pass?: boolean }).overall_pass === true ? "pass"
    : (c.gate_status_json as { overall_pass?: boolean }).overall_pass === false ? "fail" : "unknown",
  district_display: c.district_display,
  display_annual_rent_sar: c.display_annual_rent_sar,
  ...extra,
});

const compareResult = {
  summary: {
    best_overall_candidate_id: "c1",
    best_value_candidate_id: "c1",
    best_economics_candidate_id: "c2",
    lowest_rent_burden_candidate_id: "c4",
    best_brand_fit_candidate_id: "c1",
    highest_demand_candidate_id: "c1",
    strongest_delivery_market_candidate_id: "c2",
    strongest_whitespace_candidate_id: "c3",
    lowest_cannibalization_candidate_id: "c3",
    most_confident_candidate_id: "c1",
  },
  items: [
    compareItem(candidates[0] as Record<string, unknown>, {
      demand_score: 88, fit_score: 84, brand_fit_score: 86, provider_density_score: 72,
      provider_whitespace_score: 64, delivery_competition_score: 58, multi_platform_presence_score: 70,
      economics_score: 80, estimated_rent_sar_m2_year: 2182, cannibalization_score: 22,
      zoning_fit_score: 90, frontage_score: 82, access_score: 85, parking_score: 70, access_visibility_score: 88,
    }),
    compareItem(candidates[1] as Record<string, unknown>, {
      demand_score: 76, fit_score: 80, brand_fit_score: 74, provider_density_score: 80,
      provider_whitespace_score: 70, delivery_competition_score: 66, multi_platform_presence_score: 78,
      economics_score: 82, estimated_rent_sar_m2_year: 2000, cannibalization_score: 28,
      zoning_fit_score: 84, frontage_score: 70, access_score: 78, parking_score: 74, access_visibility_score: 72,
    }),
    compareItem(candidates[2] as Record<string, unknown>, {
      demand_score: 70, fit_score: 76, brand_fit_score: 72, provider_density_score: 60,
      provider_whitespace_score: 82, delivery_competition_score: 74, multi_platform_presence_score: 62,
      economics_score: 71, estimated_rent_sar_m2_year: 1367, cannibalization_score: 14,
      zoning_fit_score: 78, frontage_score: 76, access_score: 72, parking_score: 80, access_visibility_score: 70,
    }),
  ],
};

// ── Report fixture ─────────────────────────────────────────────────────
const report = {
  meta: { version: "2.1" },
  recommendation: {
    summary:
      "Al Olaya is the strongest place to open next: dense daytime footfall, a confident rent read from a live listing, and healthy economics. Al Malqa is a solid runner-up with strong delivery demand.",
    why_best:
      "Highest overall score with grade-A confidence — backed by an actual market rent and strong brand fit for a quick-service format.",
    main_risk:
      "Rent sits at the premium end of the corridor, so protect margins with a compact, high-throughput layout.",
    best_format: "Compact 200–230 m² unit with a strong frontage and a pickup/delivery lane.",
    best_candidate_id: "c1",
    runner_up_candidate_id: "c2",
    best_value_candidate_id: "c1",
    highest_demand_candidate_id: "c1",
    best_economics_candidate_id: "c2",
    best_brand_fit_candidate_id: "c1",
    strongest_whitespace_candidate_id: "c3",
    most_confident_candidate_id: "c1",
    best_pass_candidate_id: "c1",
    pass_count: 3,
  },
  top_candidates: candidates.slice(0, 3).map((c) => ({
    ...c,
    gate_verdict: "pass",
  })),
  assumptions: {
    service_model: "Quick-service (QSR)",
    target_area_m2: "200",
    cannibalization_radius_m: "1800",
    price_tier: "Mid-range",
  },
};

// ── Shell + router ───────────────────────────────────────────────────────
function Shell({ width, children }: { width: number; children: React.ReactNode }) {
  return (
    <div
      style={{
        width,
        background: "var(--oak-bg-surface, #fff)",
        padding: 16,
        boxSizing: "border-box",
        minHeight: 100,
      }}
      className="ea-panel"
    >
      {children}
    </div>
  );
}

const panel = new URLSearchParams(window.location.search).get("panel") || "results";
const root = createRoot(document.getElementById("root")!);

const noop = () => undefined;

let view: React.ReactNode = null;
if (panel === "brief") {
  view = (
    <Shell width={460}>
      <ExpansionBriefForm
        initialValue={{
          ...defaultBrief,
          brand_name: "Tannour Grill",
          category: "Grills & Shawarma",
          target_districts: ["al_olaya", "al_malqa"],
          existing_branches: [
            { name: "Tannour — Al Yasmin", lat: 24.83, lon: 46.63, district: "Al Yasmin" } as never,
          ],
        }}
        onSubmit={noop as never}
        loading={false}
      />
    </Shell>
  );
} else if (panel === "results") {
  view = (
    <Shell width={460}>
      <ExpansionResultsPanel
        items={candidates as never}
        selectedCandidateId={"c1"}
        shortlistIds={[]}
        compareIds={["c2"]}
        leadCandidateId={"c1"}
        onSelectCandidate={noop}
        onToggleCompare={noop}
        onOpenMemo={noop}
        onShowOnMap={noop}
      />
    </Shell>
  );
} else if (panel === "compare") {
  view = (
    <ExpansionComparePanel
      compareIds={["c1", "c2", "c3"]}
      result={compareResult as never}
      loading={false}
      error={null}
      leadCandidateId={"c1"}
      onCompare={noop}
      onSelectCandidateId={noop}
      onClose={noop}
    />
  );
} else if (panel === "report") {
  view = (
    <ExpansionReportPanel
      report={report as never}
      loading={false}
      error={null}
      leadCandidateId={"c1"}
      leadCandidate={candidates[0] as never}
      memo={null}
      onSelectCandidateId={noop}
      onClose={noop}
    />
  );
}

root.render(<>{view}</>);
