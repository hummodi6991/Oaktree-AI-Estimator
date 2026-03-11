import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { ExpansionCandidate, RecommendationReportResponse, CandidateMemoResponse } from "../../lib/api/expansionAdvisor";
import { buildCopySummary, formatCopySummaryText, formatLandlordBriefingText } from "./studyAdapters";

type Props = {
  candidate: ExpansionCandidate | null;
  report: RecommendationReportResponse | null;
  memo: CandidateMemoResponse | null;
};

export default function CopySummaryBlock({ candidate, report, memo }: Props) {
  const { t } = useTranslation();
  const [copiedExec, setCopiedExec] = useState(false);
  const [copiedBriefing, setCopiedBriefing] = useState(false);

  const summary = buildCopySummary(candidate, report, memo);
  const text = formatCopySummaryText(summary);

  const handleCopyExec = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedExec(true);
      setTimeout(() => setCopiedExec(false), 2000);
    } catch { /* fallback */ }
  };

  const handleCopyBriefing = async () => {
    if (!candidate) return;
    try {
      const briefingText = formatLandlordBriefingText(candidate, report, memo);
      await navigator.clipboard.writeText(briefingText);
      setCopiedBriefing(true);
      setTimeout(() => setCopiedBriefing(false), 2000);
    } catch { /* fallback */ }
  };

  return (
    <div className="ea-copy-summary">
      <div className="ea-copy-summary__header">
        <h5 className="ea-detail__section-title">{t("expansionAdvisor.executiveSummaryBlock")}</h5>
        <div style={{ display: "flex", gap: 4 }}>
          <button type="button" className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={handleCopyExec}>
            {copiedExec ? t("expansionAdvisor.copied") : t("expansionAdvisor.copyToClipboard")}
          </button>
          {candidate && (
            <button type="button" className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={handleCopyBriefing}>
              {copiedBriefing ? t("expansionAdvisor.copied") : t("expansionAdvisor.copySiteVisitBriefing")}
            </button>
          )}
        </div>
      </div>
      <div className="ea-copy-summary__body">
        <div className="ea-copy-summary__row">
          <span className="ea-copy-summary__label">{t("expansionAdvisor.leadSite")}</span>
          <span className="ea-copy-summary__value">{summary.bestCandidate}</span>
        </div>
        <div className="ea-copy-summary__row">
          <span className="ea-copy-summary__label">{t("expansionAdvisor.topReason")}</span>
          <span className="ea-copy-summary__value">{summary.topReason}</span>
        </div>
        <div className="ea-copy-summary__row">
          <span className="ea-copy-summary__label">{t("expansionAdvisor.mainRisk")}</span>
          <span className="ea-copy-summary__value">{summary.mainRisk}</span>
        </div>
        <div className="ea-copy-summary__row">
          <span className="ea-copy-summary__label">{t("expansionAdvisor.reportBestFormat")}</span>
          <span className="ea-copy-summary__value">{summary.bestFormat}</span>
        </div>
        <div className="ea-copy-summary__row">
          <span className="ea-copy-summary__label">{t("expansionAdvisor.nextValidation")}</span>
          <span className="ea-copy-summary__value">{summary.nextValidation}</span>
        </div>
      </div>
    </div>
  );
}
