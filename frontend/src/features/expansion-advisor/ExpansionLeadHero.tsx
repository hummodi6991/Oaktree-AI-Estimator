import { useState } from "react";
import { useTranslation } from "react-i18next";
import type {
  CandidateMemoResponse,
  ExpansionCandidate,
  StructuredMemo,
  StructuredMemoEvidence,
} from "../../lib/api/expansionAdvisor";
import { isValidStructuredMemo } from "./DecisionMemoNarrative";
import { SkeletonCard } from "./SkeletonLoaders";
import { candidateDistrictLabel, fmtSARCompact, getDisplayScore } from "./formatHelpers";

type ExpansionLeadHeroProps = {
  candidate: ExpansionCandidate;
  memo: CandidateMemoResponse | null;
  loading: boolean;
  onOpenMemo: () => void;
  onShowOnMap: () => void;
  lang: "en" | "ar";
};

const RANKING_TRUNCATE_CHARS = 120;

function formatScore(value: number | null | undefined): string | null {
  if (value === null || value === undefined || !Number.isFinite(value)) return null;
  return Number(value).toFixed(1).replace(/\.0$/, "");
}

function evidenceChipClass(polarity?: "positive" | "negative" | "neutral"): string {
  const resolved =
    polarity === "positive" || polarity === "negative" ? polarity : "neutral";
  return `ea-lead-hero__evidence-chip ea-lead-hero__evidence-chip--${resolved}`;
}

export default function ExpansionLeadHero({
  candidate,
  memo,
  loading,
  onOpenMemo,
  onShowOnMap,
  lang,
}: ExpansionLeadHeroProps) {
  const { t } = useTranslation();
  const dir = lang === "ar" ? "rtl" : "ltr";
  const [rankingExpanded, setRankingExpanded] = useState(false);

  const headerId = `ea-lead-hero-header-${candidate.id}`;
  const districtLabel = candidateDistrictLabel(candidate, t("common.notAvailable"));
  const score = formatScore(getDisplayScore(candidate));
  const allGatesPass = candidate.gate_status_json?.overall_pass === true;
  const eyebrow = allGatesPass
    ? t("expansionAdvisor.leadSiteAnalysis")
    : t("expansionAdvisor.topCandidateAnalysis");

  const structured: StructuredMemo | null | undefined =
    memo?.candidate?.decision_memo_json ?? null;
  const useStructured = isValidStructuredMemo(structured ?? undefined);

  const header = (
    <div className="ea-lead-hero__header" id={headerId}>
      <div className="ea-lead-hero__header-text">
        <span className="ea-lead-hero__eyebrow">{eyebrow}</span>
        <div className="ea-lead-hero__title-row">
          <span className="ea-lead-hero__district">{districtLabel}</span>
          {score !== null && (
            <span className="ea-lead-hero__score">
              {" · "}
              {t("expansionAdvisor.summaryTopScore", { score })}
            </span>
          )}
        </div>
      </div>
      {candidate.image_url && (
        <div className="ea-lead-hero__image">
          <img
            src={candidate.image_url}
            alt={candidate.unit_neighborhood || districtLabel}
            loading="lazy"
            onError={(e) => {
              (e.target as HTMLImageElement).style.display = "none";
            }}
          />
        </div>
      )}
    </div>
  );

  const actions = (
    <div className="ea-lead-hero__actions">
      <button
        type="button"
        className="oak-btn oak-btn--sm oak-btn--primary"
        onClick={onOpenMemo}
      >
        {t("expansionAdvisor.heroReadFullMemo")}
      </button>
      <button
        type="button"
        className="oak-btn oak-btn--sm oak-btn--tertiary"
        onClick={onShowOnMap}
      >
        {t("expansionAdvisor.heroShowOnMap")}
      </button>
    </div>
  );

  if (loading && !memo) {
    return (
      <section
        className="ea-lead-hero ea-lead-hero--loading"
        dir={dir}
        aria-labelledby={headerId}
      >
        {header}
        <SkeletonCard />
      </section>
    );
  }

  if (useStructured && structured) {
    const rankingExplanation =
      typeof structured.ranking_explanation === "string"
        ? structured.ranking_explanation.trim()
        : "";
    const bottomLine =
      typeof structured.bottom_line === "string" ? structured.bottom_line.trim() : "";
    const evidenceItems: StructuredMemoEvidence[] = Array.isArray(structured.key_evidence)
      ? structured.key_evidence.slice(0, 2)
      : [];

    const showTruncatedRanking =
      rankingExplanation.length > RANKING_TRUNCATE_CHARS && !rankingExpanded;
    const displayedRanking = showTruncatedRanking
      ? `${rankingExplanation.slice(0, RANKING_TRUNCATE_CHARS).trimEnd()}…`
      : rankingExplanation;

    return (
      <section
        className="ea-lead-hero ea-lead-hero--structured"
        dir={dir}
        aria-labelledby={headerId}
      >
        {header}
        <p className="ea-lead-hero__headline">{structured.headline_recommendation}</p>
        {rankingExplanation && (
          <p className="ea-lead-hero__ranking">
            {displayedRanking}
            {showTruncatedRanking && (
              <button
                type="button"
                className="ea-lead-hero__read-more"
                onClick={() => {
                  setRankingExpanded(true);
                  onOpenMemo();
                }}
              >
                {t("expansionAdvisor.heroReadMore")}
              </button>
            )}
          </p>
        )}
        {evidenceItems.length > 0 && (
          <ul className="ea-lead-hero__evidence-list">
            {evidenceItems.map((item, i) => (
              <li key={i} className={evidenceChipClass(item.polarity)}>
                <span className="ea-lead-hero__evidence-signal">{item.signal}</span>
                <span className="ea-lead-hero__evidence-value">{String(item.value)}</span>
              </li>
            ))}
          </ul>
        )}
        {bottomLine && <p className="ea-lead-hero__bottom-line">{bottomLine}</p>}
        {actions}
      </section>
    );
  }

  // Deterministic fallback
  const positives = (candidate.top_positives_json || []).filter(
    (p) => typeof p === "string" && p.trim() !== "" && p !== "—",
  );
  const risks = (candidate.top_risks_json || []).filter(
    (r) => typeof r === "string" && r.trim() !== "" && r !== "—",
  );
  const decisionSummary =
    typeof candidate.decision_summary === "string" && candidate.decision_summary.trim() !== ""
      ? candidate.decision_summary.trim()
      : null;
  const rent = candidate.display_annual_rent_sar ?? candidate.estimated_annual_rent_sar;
  const rentClause =
    typeof rent === "number" && Number.isFinite(rent)
      ? t("expansionAdvisor.heroFallbackRentClause", { rent: fmtSARCompact(rent) })
      : "";
  const fallbackHeadline = t("expansionAdvisor.heroFallbackHeadline", {
    district: districtLabel,
    score: score ?? "—",
    rentClause,
  });

  return (
    <section
      className="ea-lead-hero ea-lead-hero--fallback"
      dir={dir}
      aria-labelledby={headerId}
    >
      {header}
      <p className="ea-lead-hero__headline">{fallbackHeadline}</p>
      {positives.length > 0 && (
        <div className="ea-lead-hero__insight ea-lead-hero__insight--positive">
          <span className="ea-lead-hero__insight-icon" aria-hidden="true">
            +
          </span>
          <span className="ea-lead-hero__insight-text">{positives[0]}</span>
        </div>
      )}
      {risks.length > 0 && (
        <div className="ea-lead-hero__insight ea-lead-hero__insight--risk">
          <span className="ea-lead-hero__insight-icon" aria-hidden="true">
            !
          </span>
          <span className="ea-lead-hero__insight-text">{risks[0]}</span>
        </div>
      )}
      {decisionSummary && (
        <p className="ea-lead-hero__decision-summary">{decisionSummary}</p>
      )}
      <div className="ea-lead-hero__fallback-cta">
        <button
          type="button"
          className="oak-btn oak-btn--sm oak-btn--secondary"
          onClick={onOpenMemo}
        >
          {t("expansionAdvisor.heroGenerateMemo")}
        </button>
      </div>
      {actions}
    </section>
  );
}
