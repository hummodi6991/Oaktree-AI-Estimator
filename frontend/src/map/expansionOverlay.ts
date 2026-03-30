import type { ExpansionCandidate } from "../lib/api/expansionAdvisor";

export function buildExpansionOverlayData(
  expansionCandidates: ExpansionCandidate[] = [],
  selectedExpansionCandidateId: string | null = null,
  shortlistExpansionCandidateIds: string[] = [],
  existingBranches: Array<{ lat: number; lon: number }> = [],
  compareExpansionCandidateIds: string[] = [],
  leadExpansionCandidateId: string | null = null,
) {
  const candidateFeatures = expansionCandidates
    .filter((item) => Number.isFinite(item.lon) && Number.isFinite(item.lat))
    .map((item) => ({
      type: "Feature" as const,
      geometry: { type: "Point" as const, coordinates: [item.lon, item.lat] },
      properties: {
        candidate_id: item.id,
        selected: item.id === selectedExpansionCandidateId,
        shortlisted: shortlistExpansionCandidateIds.includes(item.id),
        compared: compareExpansionCandidateIds.includes(item.id),
        is_lead: item.id === leadExpansionCandidateId,
        rank_position: item.rank_position ?? null,
        district: item.district ?? null,
        final_score: item.score_breakdown_json?.display_score ?? item.final_score ?? null,
        gate_pass: item.gate_status_json?.overall_pass ?? null,
        source_type: item.source_type ?? "parcel",
      },
    }));

  const branchFeatures = existingBranches
    .filter((branch) => Number.isFinite(branch.lon) && Number.isFinite(branch.lat))
    .map((branch) => ({
      type: "Feature" as const,
      geometry: { type: "Point" as const, coordinates: [branch.lon, branch.lat] },
      properties: {},
    }));

  return {
    candidateFc: { type: "FeatureCollection" as const, features: candidateFeatures },
    branchFc: { type: "FeatureCollection" as const, features: branchFeatures },
  };
}
