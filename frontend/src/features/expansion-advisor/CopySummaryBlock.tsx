import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { ExpansionCandidate, RecommendationReportResponse, CandidateMemoResponse } from "../../lib/api/expansionAdvisor";
import { buildCopySummary, formatCopySummaryText } from "./studyAdapters";

type Props = {
  candidate: ExpansionCandidate | null;
  report: RecommendationReportResponse | null;
  memo: CandidateMemoResponse | null;
};

export default function CopySummaryBlock({ candidate, report, memo }: Props) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);

  const summary = buildCopySummary(candidate, report, memo);
  const text = formatCopySummaryText(summary);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // fallback: select text
    }
  };

  return (
    <div className="ea-copy-summary">
      <div className="ea-copy-summary__header">
        <h5 className="ea-detail__section-title">{t("expansionAdvisor.executiveSummaryBlock")}</h5>
        <button type="button" className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={handleCopy}>
          {copied ? t("expansionAdvisor.copied") : t("expansionAdvisor.copyToClipboard")}
        </button>
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
