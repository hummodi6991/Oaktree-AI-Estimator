import type {
  BriefExtractionResult,
  BriefProposalConfidence,
  BriefProposalEntry,
  ExpansionBrandProfile,
  ExpansionBrief,
} from "../../lib/api/expansionAdvisor";

/** Feature flag — mirrors the backend EXPANSION_BRIEF_EXTRACTION_ENABLED.
 * Off (default): the textarea is not rendered and the request payload is
 * byte-identical to today (locked decision L6). */
export function isBriefExtractionEnabled(): boolean {
  const raw = (import.meta.env.VITE_EXPANSION_BRIEF_EXTRACTION_ENABLED ?? "") as string;
  return raw === "1" || raw.toLowerCase() === "true";
}

/** Client-side caps (locked decisions L5 / design §4.5). */
export const BRIEF_TEXT_MAX_LENGTH = 600;
export const BRIEF_EXTRACTION_MAX_CALLS = 5;

/** Proposal fields that live in the Advanced section of the form — when
 * Apply writes any of these, the section auto-expands so nothing changes
 * invisibly (design §5.1). */
const ADVANCED_SECTION_FIELDS = new Set([
  "price_tier",
  "primary_channel",
  "cannibalization_tolerance_m",
  "parking_sensitivity",
  "frontage_sensitivity",
  "visibility_sensitivity",
  "preferred_districts",
  "excluded_districts",
]);

/** Fields the Apply button may write into brand_profile. Mirrors the
 * backend's closed surface — anything else in a proposal is ignored. */
export const APPLYABLE_PROFILE_FIELDS = [
  "brand_archetype",
  "price_tier",
  "primary_channel",
  "parking_sensitivity",
  "frontage_sensitivity",
  "visibility_sensitivity",
  "cannibalization_tolerance_m",
  "preferred_districts",
  "excluded_districts",
] as const;

export type ApplyableProfileField = (typeof APPLYABLE_PROFILE_FIELDS)[number];

/** Flatten a rich proposal into a brand-profile delta, skipping chips the
 * user discarded. */
export function proposalToProfileDelta(
  proposal: Record<string, BriefProposalEntry>,
  discarded: ReadonlySet<string> = new Set(),
): Partial<ExpansionBrandProfile> {
  const delta: Record<string, unknown> = {};
  for (const field of APPLYABLE_PROFILE_FIELDS) {
    if (discarded.has(field)) continue;
    const entry = proposal[field];
    if (entry && entry.value !== undefined && entry.value !== null) {
      delta[field] = entry.value;
    }
  }
  return delta as Partial<ExpansionBrandProfile>;
}

export function deltaTouchesAdvancedSection(
  delta: Partial<ExpansionBrandProfile>,
): boolean {
  return Object.keys(delta).some((k) => ADVANCED_SECTION_FIELDS.has(k));
}

function mergeDistricts(existing: string[] | null | undefined, incoming: string[]): string[] {
  const merged = [...(existing || [])];
  for (const key of incoming) {
    if (!merged.includes(key)) merged.push(key);
  }
  return merged;
}

/** Write an accepted extraction into the brief. Pure — returns a new
 * object; the form's controls then visibly reflect the applied values
 * (the edit affordance IS the existing form, design §5.1). */
export function applyExtractionToBrief(
  brief: ExpansionBrief,
  result: BriefExtractionResult,
  briefText: string,
  discarded: ReadonlySet<string> = new Set(),
): { next: ExpansionBrief; delta: Partial<ExpansionBrandProfile> } {
  const delta = proposalToProfileDelta(result.proposal, discarded);
  const profile: ExpansionBrandProfile = { ...(brief.brand_profile || {}) };

  for (const [field, value] of Object.entries(delta)) {
    if (field === "preferred_districts" || field === "excluded_districts") {
      profile[field] = mergeDistricts(profile[field], value as string[]);
    } else {
      (profile as Record<string, unknown>)[field] = value;
    }
  }

  profile.brief_text = briefText;
  profile.brief_extraction = {
    extraction_json: result as unknown as Record<string, unknown>,
    model: result.model ?? null,
    prompt_version: result.prompt_version ?? null,
    accepted: true,
    edited_fields: [],
  };

  return { next: { ...brief, brand_profile: profile }, delta };
}

/** Fields the user changed after pressing Apply (audit, design §6.1).
 * Compared against the applied delta at submit time. */
export function editedFieldsSinceApply(
  profile: ExpansionBrandProfile | null | undefined,
  appliedDelta: Partial<ExpansionBrandProfile>,
): string[] {
  if (!profile) return Object.keys(appliedDelta);
  const edited: string[] = [];
  for (const [field, applied] of Object.entries(appliedDelta)) {
    const current = (profile as Record<string, unknown>)[field];
    const same = Array.isArray(applied)
      ? Array.isArray(current) && applied.every((v) => (current as unknown[]).includes(v))
      : current === applied;
    if (!same) edited.push(field);
  }
  return edited;
}

// ── Panel view model (pure; unit-tested without DOM rendering) ──────

export type BriefChipViewModel = {
  field: ApplyableProfileField;
  labelKey: string;
  valueLabelKeys: string[];
  valueText: string | null;
  confidence: BriefProposalConfidence;
  evidence: string;
};

const FIELD_LABEL_KEYS: Record<ApplyableProfileField, string> = {
  brand_archetype: "expansionAdvisor.brandArchetype",
  price_tier: "expansionAdvisor.priceTier",
  primary_channel: "expansionAdvisor.primaryChannel",
  parking_sensitivity: "expansionAdvisor.parkingSensitivity",
  frontage_sensitivity: "expansionAdvisor.frontageSensitivity",
  visibility_sensitivity: "expansionAdvisor.visibilitySensitivity",
  cannibalization_tolerance_m: "expansionAdvisor.cannibalizationTolerance",
  preferred_districts: "expansionAdvisor.preferredDistricts",
  excluded_districts: "expansionAdvisor.excludedDistricts",
};

const VALUE_LABEL_KEYS: Record<string, Record<string, string>> = {
  brand_archetype: {
    balanced: "expansionAdvisor.archetypeBalanced",
    delivery_led: "expansionAdvisor.archetypeDeliveryLed",
    street_flagship: "expansionAdvisor.archetypeStreetFlagship",
    neighborhood_local: "expansionAdvisor.archetypeNeighborhoodLocal",
  },
  price_tier: {
    value: "expansionAdvisor.value",
    mid: "expansionAdvisor.mid",
    premium: "expansionAdvisor.premium",
  },
  primary_channel: {
    balanced: "expansionAdvisor.balanced",
    dine_in: "expansionAdvisor.dineIn",
    delivery: "expansionAdvisor.delivery",
  },
  parking_sensitivity: {
    low: "expansionAdvisor.low",
    medium: "expansionAdvisor.medium",
    high: "expansionAdvisor.high",
  },
  frontage_sensitivity: {
    low: "expansionAdvisor.low",
    medium: "expansionAdvisor.medium",
    high: "expansionAdvisor.high",
  },
  visibility_sensitivity: {
    low: "expansionAdvisor.low",
    medium: "expansionAdvisor.medium",
    high: "expansionAdvisor.high",
  },
};

export function confidenceBadgeColor(confidence: BriefProposalConfidence): string {
  // Same visual grammar as ConfidenceBadge: high = solid green,
  // medium = amber, low = neutral/gray (design §5.1).
  if (confidence === "high") return "green";
  if (confidence === "medium") return "amber";
  return "neutral";
}

export function buildBriefChips(
  proposal: Record<string, BriefProposalEntry>,
): BriefChipViewModel[] {
  const chips: BriefChipViewModel[] = [];
  for (const field of APPLYABLE_PROFILE_FIELDS) {
    const entry = proposal[field];
    if (!entry || entry.value === undefined || entry.value === null) continue;
    const confidence: BriefProposalConfidence =
      entry.confidence === "high" || entry.confidence === "medium" ? entry.confidence : "low";
    let valueLabelKeys: string[] = [];
    let valueText: string | null = null;
    if (field === "preferred_districts" || field === "excluded_districts") {
      valueText = (entry.value as string[]).join("، ");
    } else if (field === "cannibalization_tolerance_m") {
      valueText = `${entry.value} m`;
    } else {
      const key = VALUE_LABEL_KEYS[field]?.[String(entry.value)];
      if (key) valueLabelKeys = [key];
      else valueText = String(entry.value);
    }
    chips.push({
      field,
      labelKey: FIELD_LABEL_KEYS[field],
      valueLabelKeys,
      valueText,
      confidence,
      evidence: entry.evidence || "",
    });
  }
  return chips;
}
