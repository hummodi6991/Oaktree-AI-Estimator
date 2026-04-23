import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";

/**
 * Visual tiering for the Expansion Advisor shortlist.
 *
 * Three tiers, derived at render time from fields already present on
 * ExpansionCandidate. Tiering is a visual grouping only — it does not
 * re-sort candidates within a tier and does not change the rank number
 * shown on the card. The backend ranker is the source of truth.
 *
 * Precedence (checked in order):
 *   1. Exploratory — gate failure / D grade / low score demotes the card
 *      even if another signal is strong.
 *   2. Premier     — grade A + overall_pass + score >= PREMIER_MIN_SCORE.
 *   3. Standard    — everything else. This is the baseline and renders
 *      with no visual change from pre-patch behavior.
 *
 * Thresholds live here as named constants so product can tune them in
 * one place without touching JSX or the wrapper list component.
 */

export const PREMIER_MIN_SCORE = 75;
export const EXPLORATORY_MAX_SCORE = 55;
export const PREMIER_CONFIDENCE_GRADE = "A";
export const EXPLORATORY_CONFIDENCE_GRADE = "D";

export type CandidateTier = "premier" | "standard" | "exploratory";

export function classifyCandidateTier(candidate: ExpansionCandidate): CandidateTier {
  const score = candidate.final_score;
  const grade = candidate.confidence_grade ?? null;
  const overallPass = candidate.gate_status_json?.overall_pass ?? null;

  // 1. Exploratory — any of these demotes the card.
  //    - explicit gate failure (pass === false, not null)
  //    - grade D
  //    - score strictly below the exploratory ceiling
  if (
    overallPass === false ||
    grade === EXPLORATORY_CONFIDENCE_GRADE ||
    (typeof score === "number" && score < EXPLORATORY_MAX_SCORE)
  ) {
    return "exploratory";
  }

  // 2. Premier — grade A + score >= threshold. Null overall_pass does not
  //    block Premier: Aqar Tier 1 candidates structurally have
  //    overall_pass = null because parking_pass is always null for them
  //    (no parking ground truth in Aqar listings). Explicit failure
  //    (overall_pass === false) is already routed to Exploratory by the
  //    precedence rule above, so the only values that reach this branch
  //    are true and null — no need to recheck the gate here.
  if (
    grade === PREMIER_CONFIDENCE_GRADE &&
    typeof score === "number" &&
    score >= PREMIER_MIN_SCORE
  ) {
    return "premier";
  }

  // 3. Standard — baseline, unchanged.
  return "standard";
}

export type TierGrouped = {
  premier: ExpansionCandidate[];
  standard: ExpansionCandidate[];
  exploratory: ExpansionCandidate[];
};

/**
 * Group candidates by tier while preserving their input order. The input
 * order is the backend ranker's order — we must not re-sort within a tier.
 */
export function groupCandidatesByTier(candidates: ExpansionCandidate[]): TierGrouped {
  const grouped: TierGrouped = { premier: [], standard: [], exploratory: [] };
  for (const c of candidates) {
    grouped[classifyCandidateTier(c)].push(c);
  }
  return grouped;
}
