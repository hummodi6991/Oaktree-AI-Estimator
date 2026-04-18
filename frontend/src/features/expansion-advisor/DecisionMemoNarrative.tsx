import { useState, useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";
import type { LLMDecisionMemo } from "../../lib/api/expansionAdvisor";
import { generateDecisionMemo } from "../../lib/api/expansionAdvisor";

type Props = {
  candidate: Record<string, unknown>;
  brief: Record<string, unknown>;
  lang: string;
};

export default function DecisionMemoNarrative({ candidate, brief, lang }: Props) {
  const { t } = useTranslation();
  const [memo, setMemo] = useState<LLMDecisionMemo | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Cache by candidate id so re-renders don't re-fetch
  const cacheRef = useRef<Map<string, LLMDecisionMemo>>(new Map());
  const candidateId = String((candidate as Record<string, unknown>).id ?? "");

  useEffect(() => {
    if (!candidateId) return;

    const cached = cacheRef.current.get(candidateId);
    if (cached) {
      setMemo(cached);
      setError(null);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);
    setMemo(null);

    generateDecisionMemo(candidate, brief, lang)
      .then((result) => {
        if (cancelled) return;
        cacheRef.current.set(candidateId, result.memo);
        setMemo(result.memo);
      })
      .catch(() => {
        if (cancelled) return;
        setError(t("decisionMemo.error"));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [candidateId]); // eslint-disable-line react-hooks/exhaustive-deps

  if (loading) {
    return (
      <div className="ea-memo-narrative ea-memo-narrative--loading">
        <div className="ea-skeleton ea-skeleton--headline" />
        <div className="ea-skeleton ea-skeleton--paragraph" />
        <div className="ea-skeleton ea-skeleton--list" />
        <div className="ea-skeleton ea-skeleton--list" />
        <p className="ea-memo-narrative__loading-text">{t("decisionMemo.loading")}</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="ea-memo-narrative ea-memo-narrative--error">
        <p>{error}</p>
      </div>
    );
  }

  if (!memo) return null;

  return (
    <div className="ea-memo-narrative" dir={lang === "ar" ? "rtl" : "ltr"}>
      {/* Headline */}
      <h3 className="ea-memo-narrative__headline">{memo.headline}</h3>

      {/* Fit summary */}
      <p className="ea-memo-narrative__summary">{memo.fit_summary}</p>

      {/* Why pursue */}
      {memo.top_reasons_to_pursue.length > 0 && (
        <div className="ea-memo-narrative__section">
          <h5 className="ea-memo-narrative__section-title ea-memo-narrative__section-title--positive">
            {t("decisionMemo.whyPursue")}
          </h5>
          <ul className="ea-memo-narrative__list">
            {memo.top_reasons_to_pursue.map((reason, i) => (
              <li key={i}>{reason}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Risks to weigh */}
      {memo.top_risks.length > 0 && (
        <div className="ea-memo-narrative__section">
          <h5 className="ea-memo-narrative__section-title ea-memo-narrative__section-title--risk">
            {t("decisionMemo.risksToWeigh")}
          </h5>
          <ul className="ea-memo-narrative__list">
            {memo.top_risks.map((risk, i) => (
              <li key={i}>{risk}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Recommended next action */}
      {memo.recommended_next_action && memo.recommended_next_action !== "—" && (
        <div className="ea-memo-narrative__callout">
          <span className="ea-memo-narrative__callout-label">{t("decisionMemo.nextAction")}</span>
          <span>{memo.recommended_next_action}</span>
        </div>
      )}

      {/* Rent context */}
      {memo.rent_context && memo.rent_context !== "—" && (
        <p className="ea-memo-narrative__rent-context">{memo.rent_context}</p>
      )}
    </div>
  );
}
