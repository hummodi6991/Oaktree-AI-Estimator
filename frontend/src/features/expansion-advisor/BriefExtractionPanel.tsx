import { useTranslation } from "react-i18next";
import type { BriefExtractionResult } from "../../lib/api/expansionAdvisor";
import {
  buildBriefChips,
  confidenceBadgeColor,
} from "./briefExtraction";

type Props = {
  result: BriefExtractionResult;
  discarded: ReadonlySet<string>;
  onToggleDiscard: (field: string) => void;
  onApply: () => void;
  onDismiss: () => void;
  disabled?: boolean;
};

/** "Reading your brief as:" confirmation panel (design §5.1).
 *
 * Strictly presentational: renders the extraction PROPOSAL with per-chip
 * confidence + evidence and per-chip discard. Nothing reaches the form
 * until the user presses Apply (locked decision L2). */
export default function BriefExtractionPanel({
  result,
  discarded,
  onToggleDiscard,
  onApply,
  onDismiss,
  disabled,
}: Props) {
  const { t } = useTranslation();
  const chips = buildBriefChips(result.proposal);
  const activeChips = chips.filter((c) => !discarded.has(c.field));
  const nothingExtracted =
    chips.length === 0 &&
    result.conflicts.length === 0 &&
    result.unrecognized_districts.length === 0;

  if (nothingExtracted) {
    return (
      <div className="ea-brief-panel" data-testid="brief-panel-empty">
        <p className="ea-brief-panel__empty">{t("expansionAdvisor.briefNothingExtracted")}</p>
        {result.memo_color.length > 0 && (
          <p className="ea-brief-panel__memo-note">
            {t("expansionAdvisor.briefMemoColorNote")} {result.memo_color.join(" · ")}
          </p>
        )}
      </div>
    );
  }

  return (
    <div className="ea-brief-panel" data-testid="brief-panel">
      {chips.length > 0 && (
        <>
          <p className="ea-brief-panel__title">{t("expansionAdvisor.briefReadingAs")}</p>
          <div className="ea-brief-panel__chips">
            {chips.map((chip) => {
              const isDiscarded = discarded.has(chip.field);
              const valueLabel =
                chip.valueText ?? chip.valueLabelKeys.map((k) => t(k)).join(" ");
              return (
                <span
                  key={chip.field}
                  className={`ea-brief-chip ea-brief-chip--${chip.confidence}${
                    isDiscarded ? " ea-brief-chip--discarded" : ""
                  }`}
                  title={chip.evidence}
                >
                  <span className="ea-brief-chip__label">{t(chip.labelKey)}:</span>
                  <span className="ea-brief-chip__value">{valueLabel}</span>
                  <span
                    className={`ea-badge ea-badge--${confidenceBadgeColor(chip.confidence)}`}
                  >
                    {t(
                      chip.confidence === "high"
                        ? "expansionAdvisor.briefConfidenceHigh"
                        : chip.confidence === "medium"
                          ? "expansionAdvisor.briefConfidenceMedium"
                          : "expansionAdvisor.briefConfidenceLow",
                    )}
                  </span>
                  <button
                    type="button"
                    className="ea-brief-chip__discard"
                    aria-label={t("expansionAdvisor.briefDiscardChip")}
                    onClick={() => onToggleDiscard(chip.field)}
                    disabled={disabled}
                  >
                    ×
                  </button>
                </span>
              );
            })}
          </div>
        </>
      )}

      {result.conflicts.length > 0 && (
        <div className="ea-brief-panel__conflict" data-testid="brief-conflict">
          <p className="ea-brief-panel__conflict-title">
            {t("expansionAdvisor.briefConflictTitle")}
          </p>
          {result.conflicts.map((conflict, i) => (
            <p key={`${conflict.field}-${i}`} className="ea-brief-panel__conflict-note">
              {conflict.note || conflict.field}
              {conflict.evidence ? ` — "${conflict.evidence}"` : ""}
            </p>
          ))}
        </div>
      )}

      {result.unrecognized_districts.length > 0 && (
        <div className="ea-brief-panel__unrecognized" data-testid="brief-unrecognized">
          <span>{t("expansionAdvisor.briefUnrecognizedDistricts")}</span>
          {result.unrecognized_districts.map((name) => (
            <span key={name} className="ea-district-ms__chip ea-district-ms__chip--fallback">
              {name}
            </span>
          ))}
        </div>
      )}

      {result.memo_color.length > 0 && (
        <p className="ea-brief-panel__memo-note">
          {t("expansionAdvisor.briefMemoColorNote")} {result.memo_color.join(" · ")}
        </p>
      )}

      <div className="ea-brief-panel__actions">
        <button
          type="button"
          className="oak-btn oak-btn--sm oak-btn--primary"
          onClick={onApply}
          disabled={disabled || activeChips.length === 0}
        >
          {t("expansionAdvisor.briefApply")}
        </button>
        <button
          type="button"
          className="oak-btn oak-btn--sm oak-btn--tertiary"
          onClick={onDismiss}
          disabled={disabled}
        >
          {t("expansionAdvisor.briefDismiss")}
        </button>
      </div>
    </div>
  );
}
