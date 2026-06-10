import { useTranslation } from "react-i18next";
import type {
  CandidateFeatureSnapshot,
  CandidateGateReasons,
  CandidateScoreBreakdown,
  ExpansionCandidate,
  RerankReason,
  RerankStatus,
} from "../../lib/api/expansionAdvisor";
import { humanGateLabel } from "./formatHelpers";
import {
  DEMAND_DG_INPUTS,
  PER_COMPONENT_INPUTS,
  VIABILITY_LEG_ORDER,
  isDgIndexDemand,
  type ResolvedInputValue,
  type SourceToken,
} from "./scoreComponentMeta";

// The memo response embeds a loose candidate shape (feature_snapshot,
// not feature_snapshot_json). Accept a permissive partial so this component
// can be fed from the list endpoint or the memo endpoint.
type LooseCandidate = Partial<ExpansionCandidate> &
  Record<string, unknown> & {
    feature_snapshot?: CandidateFeatureSnapshot | Record<string, unknown>;
    feature_snapshot_json?: CandidateFeatureSnapshot | Record<string, unknown>;
  };

type Props = {
  gateReasons?: CandidateGateReasons;
  scoreBreakdown?: CandidateScoreBreakdown;
  deterministicRank?: number | null;
  finalRank?: number | null;
  rerankStatus?: RerankStatus | null;
  rerankReason?: RerankReason | null;
  rerankDelta?: number;
  /** Whole candidate (ExpansionCandidate or memo.candidate). Optional — when
   * omitted, contributions accordion still renders summaries but per-input
   * values fall back to em-dashes. */
  candidate?: LooseCandidate;
};

/* ─── Score-component display metadata ──────────────────────────────────── */

// Canonical component order + labels matching
// app/services/expansion_advisor.py:_score_breakdown (10 components summing
// to 100% after the Patch B chain_strength split-off).
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
  "chain_strength",
] as const;

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

/* ─── Score contributions: input-value formatting ───────────────────────── */

function formatInputValue(
  v: ResolvedInputValue,
  t: ReturnType<typeof useTranslation>["t"],
): string {
  if (v == null) return "—";
  if (typeof v === "boolean") {
    return v ? t("expansionAdvisor.decisionLogic.boolYes") : t("expansionAdvisor.decisionLogic.boolNo");
  }
  if (typeof v === "number") {
    if (!Number.isFinite(v)) return "—";
    return Number.isInteger(v) ? String(v) : v.toFixed(2);
  }
  return v;
}

function sourceLabel(
  token: SourceToken,
  t: ReturnType<typeof useTranslation>["t"],
): string {
  return t(`expansionAdvisor.scoreSources.${token}`, { defaultValue: token });
}

/* ─── Sub-component: score contributions ────────────────────────────────── */

function ContributionsSection({
  breakdown,
  candidate,
  t,
}: {
  breakdown: CandidateScoreBreakdown | undefined;
  candidate: LooseCandidate | undefined;
  t: ReturnType<typeof useTranslation>["t"];
}) {
  const weightedRaw = (breakdown?.weighted_components ||
    {}) as Record<string, unknown>;
  const weights = (breakdown?.weights || {}) as Record<string, unknown>;
  const inputs = (breakdown?.inputs || {}) as Record<string, unknown>;
  const finalScore = Number(
    breakdown?.display_score ?? breakdown?.final_score ?? 0,
  );

  // Feature snapshot may arrive as `feature_snapshot` (memo response) or
  // `feature_snapshot_json` (list/detail responses). Defensive: try both.
  const featureSnapshot =
    (candidate?.feature_snapshot_json as CandidateFeatureSnapshot | undefined) ||
    (candidate?.feature_snapshot as CandidateFeatureSnapshot | undefined);
  const contextSources =
    (featureSnapshot?.context_sources as Record<string, unknown> | undefined) ||
    {};

  const components = SCORE_COMPONENT_ORDER
    .filter((key) => key in weightedRaw)
    .map((key) => {
      const points = Number(weightedRaw[key]) || 0;
      const weight = Number(weights[key]) || 0;
      const subScore = Number(inputs[key]) || 0;
      return { key, points, weight, subScore };
    });

  const totalPoints = components.reduce((acc, c) => acc + c.points, 0);

  const renderSegment = (
    key: string,
    points: number,
    weight: number,
    label: string,
  ) => {
    const widthPct =
      totalPoints > 0 ? Math.max(0.01, (points / totalPoints) * 100) : 0;
    return (
      <span
        key={`seg-${key}`}
        className={`ea-decision-logic__bar-segment ea-decision-logic__bar-segment--${key}`}
        style={{ flexBasis: `${widthPct}%` }}
        title={`${label}: ${points.toFixed(2)} / ${weight}`}
        data-component={key}
        data-points={points.toFixed(2)}
      />
    );
  };

  const renderComponentRow = (c: {
    key: string;
    points: number;
    weight: number;
    subScore: number;
  }) => {
    const label = t(`expansionAdvisor.scoreComponents.${c.key}.label`, {
      defaultValue: c.key.replace(/_/g, " "),
    });
    // Demand Strength is engine-aware: candidates whose demand component was
    // scored off the L1 demand-generator composite (demand_score_source ===
    // "dg_index") show the dg input set and its definition. A missing field
    // or "pop_score" selects the legacy rows unchanged (cafe, delivery_first,
    // flags-off environments, historical rows).
    const dgDemand =
      c.key === "demand_potential" &&
      isDgIndexDemand(featureSnapshot as Record<string, unknown> | undefined);
    const definition = t(
      dgDemand
        ? `expansionAdvisor.scoreComponents.${c.key}.definition_dg_index`
        : `expansionAdvisor.scoreComponents.${c.key}.definition`,
      { defaultValue: "" },
    );
    const descriptors = dgDemand
      ? DEMAND_DG_INPUTS
      : PER_COMPONENT_INPUTS[c.key] ?? [];
    const resolved = descriptors.map((d) => ({
      key: d.key,
      ...d.resolve({
        candidate: (candidate as Record<string, unknown>) || {},
        scoreBreakdown: breakdown,
        featureSnapshot,
        contextSources,
      }),
    }));

    return (
      <details
        key={`comp-${c.key}`}
        className="ea-decision-logic__subsection ea-decision-logic__subsection--component-row"
        data-component={c.key}
      >
        <summary className="ea-decision-logic__subsection-summary">
          <span
            className={`ea-decision-logic__legend-swatch ea-decision-logic__bar-segment--${c.key}`}
            aria-hidden="true"
          />
          <span className="ea-decision-logic__subsection-title">{label}</span>
          <span className="ea-decision-logic__subsection-status">
            {t("expansionAdvisor.decisionLogic.weightAndPoints", {
              weight: c.weight.toFixed(1),
              points: c.points.toFixed(1),
            })}
          </span>
        </summary>
        <div className="ea-decision-logic__subsection-body">
          {definition && (
            <p className="ea-decision-logic__component-definition">
              {definition}
            </p>
          )}
          <dl className="ea-decision-logic__component-meta">
            <div className="ea-decision-logic__component-meta-row">
              <dt>{t("expansionAdvisor.decisionLogic.subScoreLabel")}</dt>
              <dd>{c.subScore.toFixed(1)} / 100</dd>
            </div>
            <div className="ea-decision-logic__component-meta-row">
              <dt>{t("expansionAdvisor.decisionLogic.weightLabel")}</dt>
              <dd>{c.weight.toFixed(1)}%</dd>
            </div>
            <div className="ea-decision-logic__component-meta-row">
              <dt>{t("expansionAdvisor.decisionLogic.contributionLabel")}</dt>
              <dd>{t("expansionAdvisor.decisionLogicWeightedPoints", { points: c.points.toFixed(2) })}</dd>
            </div>
          </dl>
          {resolved.length > 0 && (
            <>
              <h6 className="ea-decision-logic__component-inputs-title">
                {t("expansionAdvisor.decisionLogic.inputsHeading")}
              </h6>
              <dl className="ea-decision-logic__component-inputs">
                {resolved.map((r) => {
                  const inputLabel = t(
                    `expansionAdvisor.scoreComponents.${c.key}.inputs.${r.key}.label`,
                    { defaultValue: r.key.replace(/_/g, " ") },
                  );
                  return (
                    <div
                      key={`${c.key}-${r.key}`}
                      className="ea-decision-logic__component-input-row"
                      data-input={r.key}
                    >
                      <dt className="ea-decision-logic__component-input-label">
                        {inputLabel}
                      </dt>
                      <dd className="ea-decision-logic__component-input-value">
                        {formatInputValue(r.value, t)}
                      </dd>
                      <dd className="ea-decision-logic__component-input-source">
                        {sourceLabel(r.source, t)}
                      </dd>
                    </div>
                  );
                })}
              </dl>
            </>
          )}
        </div>
      </details>
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
            <div className="ea-decision-logic__bar" aria-hidden="true">
              {components.map((c) =>
                renderSegment(
                  c.key,
                  c.points,
                  c.weight,
                  t(`expansionAdvisor.scoreComponents.${c.key}.label`, {
                    defaultValue: c.key,
                  }),
                ),
              )}
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
                    {t(`expansionAdvisor.scoreComponents.${c.key}.label`, {
                      defaultValue: c.key,
                    })}
                  </span>
                  <span className="ea-decision-logic__legend-value">
                    {t("expansionAdvisor.decisionLogicWeightedPoints", {
                      points: c.points.toFixed(1),
                    })}
                  </span>
                </li>
              ))}
            </ul>
            <div className="ea-decision-logic__component-rows">
              {components.map((c) => renderComponentRow(c))}
            </div>
            <BonusesSubBlock
              breakdown={breakdown}
              candidate={candidate}
              featureSnapshot={featureSnapshot}
              t={t}
            />
          </>
        ) : null}
      </div>
    </details>
  );
}

/* ─── Sub-block: bonuses & adjustments ──────────────────────────────────── */

type BonusDetail = {
  base_deterministic?: number;
  value_band_delta?: number;
  viability_legs_fired?: string[];
  viability_delta?: number;
  freshness_bonus?: number;
  freshness_label?: string | null;
  momentum_bonus?: number;
  total_delta?: number;
  final_score_clamped?: boolean;
};

type ChipKind = "up" | "down" | "suppressed";

function Chip({
  kind,
  label,
  magnitude,
}: {
  kind: ChipKind;
  label: string;
  magnitude?: number | null;
}) {
  const cls = `ea-decision-logic__delta ea-decision-logic__delta--${kind}`;
  return (
    <span className={cls}>
      {kind === "up" && magnitude != null && (
        <span aria-hidden="true">↑</span>
      )}
      {kind === "down" && magnitude != null && (
        <span aria-hidden="true">↓</span>
      )}
      <span className="ea-decision-logic__delta-label">{label}</span>
      {kind !== "suppressed" && magnitude != null && (
        <span className="ea-decision-logic__delta-magnitude">
          {kind === "up" ? "+" : "−"}
          {Math.abs(magnitude)}
        </span>
      )}
    </span>
  );
}

function BonusesSubBlock({
  breakdown,
  candidate,
  featureSnapshot,
  t,
}: {
  breakdown: CandidateScoreBreakdown | undefined;
  candidate: LooseCandidate | undefined;
  featureSnapshot: CandidateFeatureSnapshot | Record<string, unknown> | undefined;
  t: ReturnType<typeof useTranslation>["t"];
}) {
  const bd = ((breakdown as unknown as { bonus_detail?: BonusDetail } | undefined)
    ?.bonus_detail || null) as BonusDetail | null;
  if (!bd) return null;

  const base = typeof bd.base_deterministic === "number" ? bd.base_deterministic : null;
  const valueBandDelta = typeof bd.value_band_delta === "number" ? bd.value_band_delta : 0;
  const viabilityLegs = Array.isArray(bd.viability_legs_fired) ? bd.viability_legs_fired : [];
  const freshness = bd.freshness_label || null;
  const momentumBonus = typeof bd.momentum_bonus === "number" ? bd.momentum_bonus : 0;
  const totalDelta = typeof bd.total_delta === "number" ? bd.total_delta : 0;
  const clamped = bd.final_score_clamped === true;
  const finalScore = Number(breakdown?.final_score ?? 0);

  const valueBand = (candidate?.value_band as string | undefined) || null;
  const valueBandLowConfidence = candidate?.value_band_low_confidence === true;

  // Order chips by source order: value_band → freshness → momentum → viability.
  type ChipDef = { key: string; kind: ChipKind; label: string; magnitude?: number | null };
  const chips: ChipDef[] = [];

  if (valueBandDelta === 4) {
    chips.push({
      key: "best_value",
      kind: "up",
      label: t("expansionAdvisor.bonuses.bestValue"),
      magnitude: 4,
    });
  } else if (valueBandDelta === -6) {
    chips.push({
      key: "above_market",
      kind: "down",
      label: t("expansionAdvisor.bonuses.aboveMarket"),
      magnitude: -6,
    });
  } else if (
    valueBandDelta === 0 &&
    valueBandLowConfidence &&
    (valueBand === "best_value" || valueBand === "above_market")
  ) {
    chips.push({
      key: "value_band_suppressed",
      kind: "suppressed",
      label:
        valueBand === "above_market"
          ? t("expansionAdvisor.bonuses.aboveMarketSuppressed")
          : t("expansionAdvisor.bonuses.bestValueSuppressed"),
    });
  }

  if (freshness === "new") {
    chips.push({
      key: "freshness_new",
      kind: "up",
      label: t("expansionAdvisor.bonuses.freshnessNew"),
      magnitude: 2,
    });
  } else if (freshness === "updated") {
    chips.push({
      key: "freshness_updated",
      kind: "up",
      label: t("expansionAdvisor.bonuses.freshnessUpdated"),
      magnitude: 1,
    });
  }

  // Momentum: derive client-side from district_momentum, but only display
  // when the backend awarded the bonus.
  const momentum = (featureSnapshot as Record<string, unknown> | undefined)?.district_momentum as
    | Record<string, unknown>
    | undefined;
  const momentumScore = typeof momentum?.momentum_score === "number" ? momentum.momentum_score : null;
  const sampleFloorApplied = momentum?.sample_floor_applied === true;
  if (
    momentumBonus === 2 &&
    momentumScore != null &&
    momentumScore >= 70 &&
    !sampleFloorApplied
  ) {
    chips.push({
      key: "momentum_top_tier",
      kind: "up",
      label: t("expansionAdvisor.bonuses.momentumTopTier"),
      magnitude: 2,
    });
  }

  // Viability legs in stable backend order.
  const firedSet = new Set(viabilityLegs);
  for (const leg of VIABILITY_LEG_ORDER) {
    if (!firedSet.has(leg)) continue;
    chips.push({
      key: `viability_${leg}`,
      kind: "down",
      label: t(`expansionAdvisor.bonuses.viability.${leg}`, {
        defaultValue: leg.replace(/_/g, " "),
      }),
      magnitude: -10,
    });
  }
  // Surface any unexpected legs the backend may have added, defensively.
  for (const leg of viabilityLegs) {
    if (VIABILITY_LEG_ORDER.includes(leg)) continue;
    chips.push({
      key: `viability_${leg}`,
      kind: "down",
      label: t(`expansionAdvisor.bonuses.viability.${leg}`, {
        defaultValue: leg.replace(/_/g, " "),
      }),
      magnitude: -10,
    });
  }

  return (
    <div className="ea-decision-logic__bonuses">
      <h5 className="ea-decision-logic__bonuses-title">
        {t("expansionAdvisor.decisionLogic.bonusesHeading")}
      </h5>
      <dl className="ea-decision-logic__bonuses-meta">
        <div className="ea-decision-logic__bonuses-row">
          <dt>{t("expansionAdvisor.decisionLogic.subtotal")}</dt>
          <dd>{base != null ? base.toFixed(2) : "—"}</dd>
        </div>
        {chips.length > 0 && (
          <div className="ea-decision-logic__bonuses-row ea-decision-logic__bonuses-row--chips">
            <dt>{t("expansionAdvisor.decisionLogic.adjustmentsLabel")}</dt>
            <dd className="ea-decision-logic__bonuses-chips">
              {chips.map((c) => (
                <Chip
                  key={c.key}
                  kind={c.kind}
                  label={c.label}
                  magnitude={c.magnitude}
                />
              ))}
            </dd>
          </div>
        )}
        <div className="ea-decision-logic__bonuses-row">
          <dt>{t("expansionAdvisor.decisionLogic.totalAdjustment")}</dt>
          <dd>
            {totalDelta > 0 ? "+" : ""}
            {totalDelta.toFixed(2)}
          </dd>
        </div>
        <div className="ea-decision-logic__bonuses-row ea-decision-logic__bonuses-row--final">
          <dt>{t("expansionAdvisor.decisionLogic.finalScore")}</dt>
          <dd>
            {finalScore.toFixed(2)}
            {clamped && (
              <span className="ea-decision-logic__clamped-badge">
                {t("expansionAdvisor.decisionLogic.clamped")}
              </span>
            )}
          </dd>
        </div>
      </dl>
    </div>
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
            // Convention: rerank_delta < 0 means the candidate moved UP the
            // ranking (numerically smaller rank is better, e.g. #5 → #3 yields
            // delta = -2).
            <span
              className={`ea-decision-logic__delta ea-decision-logic__delta--${
                delta < 0 ? "up" : "down"
              }`}
              aria-label={t("expansionAdvisor.decisionLogicDeltaAria", {
                direction:
                  delta < 0
                    ? t("expansionAdvisor.decisionLogicDeltaUp")
                    : t("expansionAdvisor.decisionLogicDeltaDown"),
                magnitude: Math.abs(delta),
              })}
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
  gateReasons,
  scoreBreakdown,
  deterministicRank,
  finalRank,
  rerankStatus,
  rerankReason,
  rerankDelta,
  candidate,
}: Props) {
  const { t } = useTranslation();

  return (
    <section className="ea-decision-logic" aria-label={t("expansionAdvisor.decisionLogicTitle")}>
      <h4 className="ea-decision-logic__title">
        {t("expansionAdvisor.decisionLogicTitle")}
      </h4>
      <GatesSection reasons={gateReasons} t={t} />
      <ContributionsSection breakdown={scoreBreakdown} candidate={candidate} t={t} />
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
