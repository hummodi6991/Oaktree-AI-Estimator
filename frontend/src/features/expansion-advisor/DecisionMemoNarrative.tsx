import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import type {
  GeneratedDecisionMemo,
  LLMDecisionMemo,
  StructuredMemo,
  StructuredMemoEvidence,
  StructuredMemoRisk,
} from "../../lib/api/expansionAdvisor";
import { generateDecisionMemo } from "../../lib/api/expansionAdvisor";

// Module-level cache shared across all DecisionMemoNarrative instances. Lets
// callers (and tests) read the fetched memo synchronously after it has
// resolved once for a given candidate id.
const memoModuleCache = new Map<string, GeneratedDecisionMemo>();

export function _seedDecisionMemoCacheForTest(
  candidateId: string,
  value: GeneratedDecisionMemo,
): void {
  memoModuleCache.set(candidateId, value);
}

export function _clearDecisionMemoCacheForTest(): void {
  memoModuleCache.clear();
}

type Props = {
  candidate: Record<string, unknown>;
  brief: Record<string, unknown>;
  lang: string;
};

export function isValidStructuredMemo(
  memo: StructuredMemo | null | undefined,
): memo is StructuredMemo {
  if (!memo) return false;
  if (typeof memo.headline_recommendation !== "string" || memo.headline_recommendation.trim() === "") {
    return false;
  }
  if (!Array.isArray(memo.key_evidence)) return false;
  if (!Array.isArray(memo.risks)) return false;
  for (const r of memo.risks) {
    if (r === null || typeof r !== "object") return false;
    if (typeof (r as StructuredMemoRisk).risk !== "string") return false;
  }
  return true;
}

function PolarityMarker({ polarity }: { polarity?: "positive" | "negative" | "neutral" }) {
  const resolved = polarity === "positive" || polarity === "negative" ? polarity : "neutral";
  const cls = `ea-memo-structured__polarity ea-memo-structured__polarity--${resolved}`;
  if (resolved === "positive") {
    return (
      <span className={cls} data-polarity={resolved} aria-hidden="true">
        <svg viewBox="0 0 16 16" width="12" height="12" focusable="false">
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
  if (resolved === "negative") {
    return (
      <span className={cls} data-polarity={resolved} aria-hidden="true">
        <svg viewBox="0 0 16 16" width="12" height="12" focusable="false">
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
    <span className={cls} data-polarity={resolved} aria-hidden="true">
      <svg viewBox="0 0 16 16" width="12" height="12" focusable="false">
        <circle cx="8" cy="8" r="3" fill="currentColor" />
      </svg>
    </span>
  );
}

export function StructuredNarrative({ memo, lang }: { memo: StructuredMemo; lang: string }) {
  const { t } = useTranslation();
  const dir = lang === "ar" ? "rtl" : "ltr";
  // Trim at render time: top 4 evidence, top 3 risks. The LLM may produce
  // more; we display only the highest-priority items by position.
  const evidenceItems = (Array.isArray(memo.key_evidence) ? memo.key_evidence : []).slice(0, 4);
  const risks = (Array.isArray(memo.risks) ? memo.risks : []).slice(0, 3);
  const comparison = typeof memo.comparison === "string" ? memo.comparison.trim() : "";
  const bottomLine = typeof memo.bottom_line === "string" ? memo.bottom_line.trim() : "";
  const rankingExplanation =
    typeof memo.ranking_explanation === "string" ? memo.ranking_explanation.trim() : "";

  return (
    <div className="ea-memo-narrative ea-memo-structured" dir={dir}>
      <section
        className="ea-memo-structured__headline-section"
        aria-label={t("expansionAdvisor.theRecommendation")}
      >
        <h3 className="ea-memo-structured__headline">{memo.headline_recommendation}</h3>
      </section>

      {rankingExplanation && (
        <p className="ea-memo-structured__ranking">{rankingExplanation}</p>
      )}

      {evidenceItems.length > 0 && (
        <section className="ea-memo-structured__section ea-memo-structured__section--evidence">
          <h5 className="ea-memo-structured__section-title">
            {t("expansionAdvisor.keyEvidence")}
          </h5>
          <ul className="ea-memo-structured__evidence-list">
            {evidenceItems.map((item: StructuredMemoEvidence, i: number) => (
              <li key={i} className="ea-memo-structured__evidence-item">
                <PolarityMarker polarity={item.polarity} />
                <div className="ea-memo-structured__evidence-body">
                  <div className="ea-memo-structured__evidence-head">
                    <span className="ea-memo-structured__evidence-signal">{item.signal}</span>
                    <span
                      className={
                        typeof item.value === "number"
                          ? "ea-memo-structured__evidence-value ea-memo-structured__evidence-value--numeric"
                          : "ea-memo-structured__evidence-value"
                      }
                    >
                      {String(item.value)}
                    </span>
                  </div>
                  <div className="ea-memo-structured__evidence-implication">{item.implication}</div>
                </div>
              </li>
            ))}
          </ul>
        </section>
      )}

      {risks.length > 0 && (
        <section className="ea-memo-structured__section ea-memo-structured__section--risks">
          <h5 className="ea-memo-structured__section-title ea-memo-structured__section-title--risk">
            {t("expansionAdvisor.risksToWatch")}
          </h5>
          <ul className="ea-memo-structured__risks-list">
            {risks.map((r: StructuredMemoRisk, i: number) => {
              const mitigation =
                typeof r.mitigation === "string" && r.mitigation.trim() !== ""
                  ? r.mitigation
                  : null;
              return (
                <li key={i} className="ea-memo-structured__risks-item">
                  <span className="ea-memo-structured__risks-text">{r.risk}</span>
                  {mitigation && (
                    <span className="ea-memo-structured__risks-mitigation">
                      {mitigation}
                    </span>
                  )}
                </li>
              );
            })}
          </ul>
        </section>
      )}

      {comparison && (
        <section className="ea-memo-structured__section ea-memo-structured__section--comparison">
          <h5 className="ea-memo-structured__section-title">
            {t("expansionAdvisor.howItCompares")}
          </h5>
          <p className="ea-memo-structured__comparison">{comparison}</p>
        </section>
      )}

      {bottomLine && (
        <section
          className="ea-memo-structured__bottom-line"
          aria-label={t("expansionAdvisor.bottomLine")}
        >
          <p className="ea-memo-structured__bottom-line-text">{bottomLine}</p>
        </section>
      )}
    </div>
  );
}

export function LegacyNarrative({ memo, lang }: { memo: LLMDecisionMemo; lang: string }) {
  const { t } = useTranslation();
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

export default function DecisionMemoNarrative({ candidate, brief, lang }: Props) {
  const { t } = useTranslation();
  const candidateId = String((candidate as Record<string, unknown>).id ?? "");
  // Seed initial state from the module cache so tests (and same-session
  // re-mounts) can render synchronously without re-fetching.
  const [result, setResult] = useState<GeneratedDecisionMemo | null>(
    () => (candidateId ? memoModuleCache.get(candidateId) ?? null : null),
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!candidateId) return;

    const cached = memoModuleCache.get(candidateId);
    if (cached) {
      setResult(cached);
      setError(null);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);
    setResult(null);

    generateDecisionMemo(candidate, brief, lang)
      .then((fetched) => {
        if (cancelled) return;
        memoModuleCache.set(candidateId, fetched);
        setResult(fetched);
      })
      .catch(() => {
        if (cancelled) return;
        setError(t("decisionMemo.error"));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
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

  if (!result) return null;

  const structured = result.memo_json;
  const legacy = result.memo;

  if (structured) {
    if (isValidStructuredMemo(structured)) {
      return <StructuredNarrative memo={structured} lang={lang} />;
    }
    // Malformed structured memo — warn once and fall through to legacy.
    // eslint-disable-next-line no-console
    console.warn(
      `[DecisionMemoNarrative] malformed decision_memo_json; falling back to legacy render (candidate_id=${candidateId})`,
    );
  }

  if (legacy) {
    return <LegacyNarrative memo={legacy} lang={lang} />;
  }

  return null;
}
