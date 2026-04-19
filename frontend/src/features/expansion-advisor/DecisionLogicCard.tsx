import { useTranslation } from "react-i18next";
import type {
  CandidateGateReasons,
  CandidateScoreBreakdown,
  RerankReason,
  RerankStatus,
} from "../../lib/api/expansionAdvisor";
import { humanGateLabel } from "./formatHelpers";

type GateStatusBool = boolean | null | undefined;

type Props = {
  gateStatus?: Record<string, GateStatusBool>;
  gateReasons?: CandidateGateReasons;
  scoreBreakdown?: CandidateScoreBreakdown;
  deterministicRank?: number | null;
  finalRank?: number | null;
  rerankStatus?: RerankStatus | null;
  rerankReason?: RerankReason | null;
  rerankDelta?: number;
};

/* ─── Score-component display metadata ──────────────────────────────────── */

// Canonical component order + labels matching
// app/services/expansion_advisor.py:2409-2419 (9 components summing to 100%).
const SCORE_COMPONENT_ORDER: readonly string[] = [
  "occupancy_economics",
  "listing_quality",
  "brand_fit",
  "landlord_signal",
  "competition_whitespace",
  "demand_potential",
  "access_visibility",
  "delivery_demand",
  "confidence",
] as const;

const SCORE_COMPONENT_LABEL: Record<string, string> = {
  occupancy_economics: "Economics",
  listing_quality: "Listing Quality",
  brand_fit: "Brand Fit",
  landlord_signal: "Landlord Signal",
  competition_whitespace: "Competitor Openness",
  demand_potential: "Demand Strength",
  access_visibility: "Access & Visibility",
  delivery_demand: "Delivery Market",
  confidence: "Data Quality",
};

function labelForComponent(key: string): string {
  return (
    SCORE_COMPONENT_LABEL[key] ||
    key.replace(/_/g, " ").replace(/^\w/, (c) => c.toUpperCase())
  );
}

/* ─── Inline gate-status icons (no icon library) ────────────────────────── */

function GateIcon({ status }: { status: "pass" | "fail" | "unknown" }) {
  if (status === "pass") {
    return (
      <span
        className="ea-decision-logic__gate-icon ea-decision-logic__gate-icon--pass"
        aria-hidden="true"
      >
        <svg viewBox="0 0 16 16" width="14" height="14" focusable="false">
          <path
            d="M13.5 4.5 6.25 11.75 2.5 8"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </span>
    );
  }
  if (status === "fail") {
    return (
      <span
        className="ea-decision-logic__gate-icon ea-decision-logic__gate-icon--fail"
        aria-hidden="true"
      >
        <svg viewBox="0 0 16 16" width="14" height="14" focusable="false">
          <path
            d="M4 4 12 12 M12 4 4 12"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
          />
        </svg>
      </span>
    );
  }
  return (
    <span
      className="ea-decision-logic__gate-icon ea-decision-logic__gate-icon--unknown"
      aria-hidden="true"
    >
      <svg viewBox="0 0 16 16" width="14" height="14" focusable="false">
        <path
          d="M6 6.25a2 2 0 1 1 3.2 1.6c-.8.6-1.2 1-1.2 2M8 12.25h.01"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.75"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </span>
  );
}

/* ─── Gate-row helpers ──────────────────────────────────────────────────── */

type GateRow = {
  key: string;
  label: string;
  explanation: string;
};

function titleCase(raw: string): string {
  return raw
    .split(/\s+/)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

function displayGateName(raw: string): string {
  // Bucket arrays arrive as raw keys ("parking_pass") from the backend;
  // humanGateLabel handles both raw and already-humanized forms.
  const label = humanGateLabel(raw);
  return titleCase(label);
}

function lookupExplanation(
  name: string,
  explanations: Record<string, unknown> | undefined,
): string {
  if (!explanations || !name) return "";
  // Direct hit on the bucket-array key.
  const direct = explanations[name];
  if (typeof direct === "string" && direct.trim()) return direct.trim();
  // Fallback to raw key form: "parking" -> "parking_pass".
  const rawKey = name.replace(/\s+/g, "_").replace(/\//g, "_") + "_pass";
  const byRaw = explanations[rawKey];
  if (typeof byRaw === "string" && byRaw.trim()) return byRaw.trim();
  // Last attempt: humanize each explanation key and compare.
  for (const [k, v] of Object.entries(explanations)) {
    if (typeof v !== "string" || !v.trim()) continue;
    const humanized = k.replace(/_pass$/, "").replace(/_/g, " ").replace(/\//g, " ");
    if (humanized.trim().toLowerCase() === name.trim().toLowerCase()) return v.trim();
  }
  return "";
}

function buildGateRows(
  bucket: string[],
  explanations: Record<string, unknown> | undefined,
): GateRow[] {
  return bucket.map((raw) => ({
    key: raw,
    label: displayGateName(raw),
    explanation: lookupExplanation(raw, explanations),
  }));
}

/* ─── Sub-component: gates section ──────────────────────────────────────── */

function GatesSection({
  reasons,
  t,
}: {
  reasons: CandidateGateReasons | undefined;
  t: ReturnType<typeof useTranslation>["t"];
}) {
  const failed = reasons?.failed ?? [];
  const unknown = reasons?.unknown ?? [];
  const passed = reasons?.passed ?? [];
  const explanations = (reasons?.explanations as Record<string, unknown> | undefined) || {};

  const failedRows = buildGateRows(failed, explanations);
  const unknownRows = buildGateRows(unknown, explanations);
  const passedRows = buildGateRows(passed, explanations);

  // Compose summary, omitting zero-count segments.
  const summaryParts: string[] = [];
  if (failed.length > 0) summaryParts.push(`${failed.length} ${t("expansionAdvisor.gatesFailed").toLowerCase()}`);
  if (unknown.length > 0) summaryParts.push(`${unknown.length} ${t("expansionAdvisor.gateVerdictUnknown").toLowerCase()}`);
  if (passed.length > 0) summaryParts.push(`${passed.length} ${t("expansionAdvisor.gatesPassed").toLowerCase()}`);
  const summary = summaryParts.join(" · ");

  const renderBucket = (
    rows: GateRow[],
    status: "fail" | "unknown" | "pass",
    headerKey: string,
  ) => {
    if (rows.length === 0) return null;
    return (
      <div
        className={`ea-decision-logic__bucket ea-decision-logic__bucket--${status}`}
      >
        <h6 className="ea-decision-logic__bucket-title">{t(headerKey)}</h6>
        <ul className="ea-decision-logic__gate-list">
          {rows.map((row) => (
            <li key={row.key} className="ea-decision-logic__gate-row">
              <GateIcon status={status} />
              <div className="ea-decision-logic__gate-body">
                <span className="ea-decision-logic__gate-name">{row.label}</span>
                {row.explanation && (
                  <span className="ea-decision-logic__gate-explanation">
                    {row.explanation}
                  </span>
                )}
              </div>
            </li>
          ))}
        </ul>
      </div>
    );
  };

  return (
    <details
      className="ea-decision-logic__subsection ea-decision-logic__subsection--gates"
      open
    >
      <summary className="ea-decision-logic__subsection-summary">
        <span className="ea-decision-logic__subsection-title">
          {t("expansionAdvisor.decisionLogicGates")}
        </span>
        {summary && (
          <span className="ea-decision-logic__subsection-status">{summary}</span>
        )}
      </summary>
      <div className="ea-decision-logic__subsection-body">
        {renderBucket(failedRows, "fail", "expansionAdvisor.gatesFailed")}
        {renderBucket(unknownRows, "unknown", "expansionAdvisor.gateVerdictUnknown")}
        {renderBucket(passedRows, "pass", "expansionAdvisor.gatesPassed")}
      </div>
    </details>
  );
}

/* ─── Sub-component: score contributions ────────────────────────────────── */

function ContributionsSection({
  breakdown,
  t,
}: {
  breakdown: CandidateScoreBreakdown | undefined;
  t: ReturnType<typeof useTranslation>["t"];
}) {
  const weightedRaw = (breakdown?.weighted_components ||
    {}) as Record<string, unknown>;
  const weights = (breakdown?.weights || {}) as Record<string, unknown>;
  const finalScore = Number(
    breakdown?.display_score ?? breakdown?.final_score ?? 0,
  );

  const components = SCORE_COMPONENT_ORDER
    .filter((key) => key in weightedRaw)
    .map((key) => {
      const points = Number(weightedRaw[key]) || 0;
      const weight = Number(weights[key]) || 0;
      return { key, points, weight };
    });

  const totalPoints = components.reduce((acc, c) => acc + c.points, 0);

  const renderSegment = (key: string, points: number, weight: number) => {
    const widthPct =
      totalPoints > 0 ? Math.max(0.01, (points / totalPoints) * 100) : 0;
    return (
      <span
        key={`seg-${key}`}
        className={`ea-decision-logic__bar-segment ea-decision-logic__bar-segment--${key}`}
        style={{ flexBasis: `${widthPct}%` }}
        title={`${labelForComponent(key)}: ${points.toFixed(2)} / ${weight}`}
        data-component={key}
        data-points={points.toFixed(2)}
      />
    );
  };

  return (
    <details className="ea-decision-logic__subsection ea-decision-logic__subsection--contributions">
      <summary className="ea-decision-logic__subsection-summary">
        <span className="ea-decision-logic__subsection-title">
          {t("expansionAdvisor.decisionLogicContributions")}
        </span>
        <span className="ea-decision-logic__subsection-status">
          {t("expansionAdvisor.decisionLogicContributionsSummary", {
            score: finalScore.toFixed(1),
          })}
        </span>
      </summary>
      <div className="ea-decision-logic__subsection-body">
        {components.length > 0 ? (
          <>
            <div
              className="ea-decision-logic__bar"
              role="img"
              aria-label={t("expansionAdvisor.decisionLogicContributions")}
            >
              {components.map((c) => renderSegment(c.key, c.points, c.weight))}
            </div>
            <ul className="ea-decision-logic__legend">
              {components.map((c) => (
                <li
                  key={`leg-${c.key}`}
                  className="ea-decision-logic__legend-item"
                  data-component={c.key}
                >
                  <span
                    className={`ea-decision-logic__legend-swatch ea-decision-logic__bar-segment--${c.key}`}
                    aria-hidden="true"
                  />
                  <span className="ea-decision-logic__legend-label">
                    {labelForComponent(c.key)}
                  </span>
                  <span className="ea-decision-logic__legend-value">
                    {t("expansionAdvisor.decisionLogicWeightedPoints", {
                      points: c.points.toFixed(1),
                    })}
                  </span>
                </li>
              ))}
            </ul>
          </>
        ) : null}
      </div>
    </details>
  );
}

/* ─── Sub-component: ranking decision ───────────────────────────────────── */

function RankingDecisionSection({
  deterministicRank,
  finalRank,
  rerankStatus,
  rerankReason,
  rerankDelta,
  t,
}: {
  deterministicRank: number | null | undefined;
  finalRank: number | null | undefined;
  rerankStatus: RerankStatus | null | undefined;
  rerankReason: RerankReason | null | undefined;
  rerankDelta: number | undefined;
  t: ReturnType<typeof useTranslation>["t"];
}) {
  const detRank = deterministicRank ?? null;
  const finRank = finalRank ?? detRank;

  const detDisplay =
    detRank != null
      ? t("expansionAdvisor.decisionLogicDeterministicRank", { rank: detRank })
      : t("expansionAdvisor.decisionLogicDeterministicRank", { rank: "—" });

  // Resolve status with defensive fallbacks.
  // Unknown / missing / operational-fallback statuses render as flag_off.
  const status: RerankStatus =
    rerankStatus === "applied" ||
    rerankStatus === "unchanged" ||
    rerankStatus === "outside_rerank_cap"
      ? rerankStatus
      : "flag_off";

  // "applied" needs a non-empty reason.summary to actually display the reason
  // block; an empty summary falls through to the redirect-only rendering.
  const hasNonEmptySummary =
    typeof rerankReason?.summary === "string" &&
    rerankReason.summary.trim().length > 0;

  let body: React.ReactNode;
  let summaryStatus = "";

  if (status === "applied") {
    const delta = typeof rerankDelta === "number" ? rerankDelta : 0;
    summaryStatus = t("expansionAdvisor.decisionLogicRerankRedirect", {
      det: detRank ?? "—",
      final: finRank ?? "—",
    });
    body = (
      <>
        <p className="ea-decision-logic__ranking-line ea-decision-logic__ranking-line--redirect">
          {t("expansionAdvisor.decisionLogicRerankRedirect", {
            det: detRank ?? "—",
            final: finRank ?? "—",
          })}
          {delta !== 0 && (
            <span
              className={`ea-decision-logic__delta ea-decision-logic__delta--${
                delta < 0 ? "up" : "down"
              }`}
              aria-label={`rerank delta ${delta > 0 ? "+" : ""}${delta}`}
            >
              {delta < 0 ? "↑" : "↓"}
              {Math.abs(delta)}
            </span>
          )}
        </p>
        {hasNonEmptySummary && (
          <div className="ea-decision-logic__reason-block">
            <p className="ea-decision-logic__reason">
              <span className="ea-decision-logic__reason-label">
                {t("expansionAdvisor.decisionLogicRerankReasonLabel")}
              </span>{" "}
              <span className="ea-decision-logic__reason-text">
                {rerankReason!.summary.trim()}
              </span>
            </p>
            {Array.isArray(rerankReason?.positives_cited) &&
              rerankReason!.positives_cited.length > 0 && (
                <p className="ea-decision-logic__reason-sub">
                  <span className="ea-decision-logic__reason-sub-label">
                    {t("expansionAdvisor.decisionLogicPositivesCited")}
                  </span>{" "}
                  <span className="ea-decision-logic__reason-sub-text">
                    {rerankReason!.positives_cited.join(", ")}
                  </span>
                </p>
              )}
            {Array.isArray(rerankReason?.negatives_cited) &&
              rerankReason!.negatives_cited.length > 0 && (
                <p className="ea-decision-logic__reason-sub">
                  <span className="ea-decision-logic__reason-sub-label">
                    {t("expansionAdvisor.decisionLogicNegativesCited")}
                  </span>{" "}
                  <span className="ea-decision-logic__reason-sub-text">
                    {rerankReason!.negatives_cited.join(", ")}
                  </span>
                </p>
              )}
            {typeof rerankReason?.comparison_to_displaced_candidate === "string" &&
              rerankReason!.comparison_to_displaced_candidate.trim() !== "" && (
                <p className="ea-decision-logic__reason-sub">
                  <span className="ea-decision-logic__reason-sub-label">
                    {t("expansionAdvisor.decisionLogicComparisonLabel")}
                  </span>{" "}
                  <span className="ea-decision-logic__reason-sub-text">
                    {rerankReason!.comparison_to_displaced_candidate.trim()}
                  </span>
                </p>
              )}
          </div>
        )}
      </>
    );
  } else if (status === "unchanged") {
    summaryStatus = detDisplay;
    body = (
      <>
        <p className="ea-decision-logic__ranking-line">{detDisplay}</p>
        <p className="ea-decision-logic__ranking-note">
          {t("expansionAdvisor.decisionLogicDeterministicAcceptedNote")}
        </p>
      </>
    );
  } else if (status === "outside_rerank_cap") {
    summaryStatus = detDisplay;
    body = (
      <>
        <p className="ea-decision-logic__ranking-line">{detDisplay}</p>
        <p className="ea-decision-logic__ranking-note">
          {t("expansionAdvisor.decisionLogicOutsideWindowNote")}
        </p>
      </>
    );
  } else {
    // flag_off (and the catch-all: shortlist_below_minimum, llm_failed,
    // null, or any unexpected value). Honest labeling — no mention of the
    // flag, no implication that an LLM touched this ranking.
    summaryStatus = detDisplay;
    body = (
      <>
        <p className="ea-decision-logic__ranking-line">{detDisplay}</p>
        <p className="ea-decision-logic__ranking-note">
          {t("expansionAdvisor.decisionLogicDeterministicOnly")}
        </p>
      </>
    );
  }

  return (
    <details className="ea-decision-logic__subsection ea-decision-logic__subsection--ranking">
      <summary className="ea-decision-logic__subsection-summary">
        <span className="ea-decision-logic__subsection-title">
          {t("expansionAdvisor.decisionLogicRanking")}
        </span>
        {summaryStatus && (
          <span className="ea-decision-logic__subsection-status">
            {summaryStatus}
          </span>
        )}
      </summary>
      <div className="ea-decision-logic__subsection-body">{body}</div>
    </details>
  );
}

/* ─── Root card ─────────────────────────────────────────────────────────── */

export default function DecisionLogicCard({
  gateStatus: _gateStatus,
  gateReasons,
  scoreBreakdown,
  deterministicRank,
  finalRank,
  rerankStatus,
  rerankReason,
  rerankDelta,
}: Props) {
  // gateStatus is accepted for future use (e.g. when a gate appears in the
  // status map but not in any bucket); current rendering sources gates from
  // the bucketed arrays, which already preserve tri-state semantics.
  void _gateStatus;
  const { t } = useTranslation();

  return (
    <section className="ea-decision-logic" aria-label={t("expansionAdvisor.decisionLogicTitle")}>
      <h4 className="ea-decision-logic__title">
        {t("expansionAdvisor.decisionLogicTitle")}
      </h4>
      <GatesSection reasons={gateReasons} t={t} />
      <ContributionsSection breakdown={scoreBreakdown} t={t} />
      <RankingDecisionSection
        deterministicRank={deterministicRank}
        finalRank={finalRank}
        rerankStatus={rerankStatus}
        rerankReason={rerankReason}
        rerankDelta={rerankDelta}
        t={t}
      />
    </section>
  );
}
