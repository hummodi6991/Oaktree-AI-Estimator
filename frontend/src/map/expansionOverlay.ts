import type { ExpansionCandidate } from "../lib/api/expansionAdvisor";

export function buildExpansionOverlayData(
  expansionCandidates: ExpansionCandidate[] = [],
  selectedExpansionCandidateId: string | null = null,
  shortlistExpansionCandidateIds: string[] = [],
  existingBranches: Array<{ lat: number; lon: number }> = [],
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
