import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  compareExpansionCandidates,
  createExpansionSearch,
  createSavedExpansionSearch,
  getExpansionCandidateMemo,
  getExpansionRecommendationReport,
  getSavedExpansionSearch,
  listSavedExpansionSearches,
  type ExpansionBrief,
  type ExpansionCandidate,
} from "../../lib/api/expansionAdvisor";
import ExpansionBriefForm, { defaultBrief } from "./ExpansionBriefForm";
import ExpansionResultsPanel from "./ExpansionResultsPanel";
import ExpansionComparePanel from "./ExpansionComparePanel";
import ExpansionMemoPanel from "./ExpansionMemoPanel";
import SavedSearchesPanel from "./SavedSearchesPanel";
import ExpansionReportPanel from "./ExpansionReportPanel";

type CompareResponseItem = {
  candidate_id: string;
  final_score?: number;
  economics_score?: number;
  estimated_payback_months?: number;
  payback_band?: string;
  brand_fit_score?: number;
  provider_density_score?: number;
  provider_whitespace_score?: number;
  confidence_grade?: string;
  gate_status_json?: Record<string, boolean>;
};

type CompareResponse = {
  items?: CompareResponseItem[];
};

export function restoreSavedUiState(saved: {
  search_id?: string;
  selected_candidate_ids?: string[] | null;
  ui_state_json?: Record<string, unknown> | null;
  candidates?: ExpansionCandidate[];
}) {
  const uiState = (saved.ui_state_json || {}) as Record<string, unknown>;
  const compareIds = Array.isArray(uiState.compare_ids) ? (uiState.compare_ids as string[]) : [];
  const selectedId = typeof uiState.selected_candidate_id === "string" ? uiState.selected_candidate_id : null;
  const selectedCandidate = selectedId
    ? (saved.candidates || []).find((item) => item.id === selectedId) || null
    : null;

  return {
    searchId: saved.search_id || "",
    shortlistIds: (saved.selected_candidate_ids as string[]) || [],
    compareIds,
    selectedCandidate,
  };
}

export function shouldLoadMemoFromMapSelection(externalCandidateId: string | null | undefined, selectedCandidateId: string | null) {
  return Boolean(externalCandidateId && externalCandidateId !== selectedCandidateId);
}


export function getCompareRows(compareResult: CompareResponse | null): CompareResponseItem[] {
  return compareResult?.items || [];
}

export default function ExpansionAdvisorPage({
  onCandidatesChange,
  onSelectedCandidateChange,
  externalSelectedCandidateId,
}: {
  onCandidatesChange: (candidates: ExpansionCandidate[], shortlistIds: string[], selectedId: string | null, branches: ExpansionBrief["existing_branches"]) => void;
  onSelectedCandidateChange: (candidate: ExpansionCandidate | null) => void;
  externalSelectedCandidateId?: string | null;
}) {
  const { t } = useTranslation();
  const [brief, setBrief] = useState<ExpansionBrief>(defaultBrief);
  const [loadingSearch, setLoadingSearch] = useState(false);
  const [candidates, setCandidates] = useState<ExpansionCandidate[]>([]);
  const [searchId, setSearchId] = useState<string>("");
  const [selectedCandidate, setSelectedCandidate] = useState<ExpansionCandidate | null>(null);
  const [shortlistIds, setShortlistIds] = useState<string[]>([]);
  const [compareIds, setCompareIds] = useState<string[]>([]);
  const [memo, setMemo] = useState<Record<string, unknown> | null>(null);
  const [loadingMemo, setLoadingMemo] = useState(false);
  const [savedItems, setSavedItems] = useState<any[]>([]);
  const [compareResult, setCompareResult] = useState<CompareResponse | null>(null);
  const [report, setReport] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    listSavedExpansionSearches().then((res) => setSavedItems(res.items || [])).catch(() => {});
  }, []);

  useEffect(() => {
    onCandidatesChange(candidates, shortlistIds, selectedCandidate?.id || null, brief.existing_branches);
  }, [candidates, shortlistIds, selectedCandidate, brief.existing_branches, onCandidatesChange]);

  const onSubmitBrief = async (nextBrief: ExpansionBrief) => {
    setBrief(nextBrief);
    setLoadingSearch(true);
    try {
      const result = await createExpansionSearch(nextBrief);
      setSearchId(result.search_id);
      setCandidates(result.items || []);
      setCompareResult(null);
    } finally {
      setLoadingSearch(false);
    }
  };

  const handleSelectCandidate = async (candidate: ExpansionCandidate) => {
    setSelectedCandidate(candidate);
    onSelectedCandidateChange(candidate);
    setLoadingMemo(true);
    try {
      setMemo(await getExpansionCandidateMemo(candidate.id));
    } finally {
      setLoadingMemo(false);
    }
  };

  useEffect(() => {
    if (!shouldLoadMemoFromMapSelection(externalSelectedCandidateId, selectedCandidate?.id || null)) return;
    const target = candidates.find((item) => item.id === externalSelectedCandidateId);
    if (!target) return;
    void handleSelectCandidate(target);
  }, [externalSelectedCandidateId, candidates, selectedCandidate?.id]);

  const compareEnabled = compareIds.length >= 2 && compareIds.length <= 6;
  const title = useMemo(() => `${brief.brand_name || t("expansionAdvisor.title")} Study`, [brief.brand_name, t]);

  return (
    <div style={{ display: "grid", gridTemplateColumns: "300px 1fr", gap: 16 }}>
      <div style={{ display: "grid", gap: 16 }}>
        <ExpansionBriefForm initialValue={brief} loading={loadingSearch} onSubmit={onSubmitBrief} />
        <h4>{t("expansionAdvisor.savedStudies")}</h4>
        <SavedSearchesPanel
          items={savedItems}
          onOpen={async (savedId) => {
            try {
              const saved = await getSavedExpansionSearch(savedId);
              if (saved.filters_json) setBrief(saved.filters_json as ExpansionBrief);
              if (saved.candidates) setCandidates(saved.candidates);
              const restored = restoreSavedUiState(saved);
              setSearchId(restored.searchId);
              setShortlistIds(restored.shortlistIds);
              setCompareIds(restored.compareIds);
              if (restored.selectedCandidate) {
                void handleSelectCandidate(restored.selectedCandidate);
              }
            } catch {
              // noop
            }
          }}
        />
      </div>
      <div style={{ display: "grid", gap: 12 }}>
        <div style={{ display: "flex", gap: 8 }}>
          <ExpansionComparePanel
            compareIds={compareIds}
            onCompare={async () => {
              if (!compareEnabled || !searchId) return;
              const result = await compareExpansionCandidates(searchId, compareIds);
              setCompareResult(result as CompareResponse);
            }}
          />
          <button
            onClick={async () => {
              if (!searchId) return;
              await createSavedExpansionSearch({
                search_id: searchId,
                title,
                description: "",
                status: "draft",
                selected_candidate_ids: shortlistIds,
                filters_json: brief as unknown as Record<string, unknown>,
                ui_state_json: { selected_candidate_id: selectedCandidate?.id || null, compare_ids: compareIds },
              });
              const latest = await listSavedExpansionSearches();
              setSavedItems(latest.items || []);
            }}
          >
            {t("expansionAdvisor.saveSearch")}
          </button>
          <button onClick={async () => { if (!searchId) return; setReport(await getExpansionRecommendationReport(searchId)); }}>{t("expansionAdvisor.loadReport")}</button>
        </div>

        {candidates.length ? (
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
            <span>{t("expansionAdvisor.totalCandidates")}: {candidates.length}</span>
            <span>{t("expansionAdvisor.passGates")}: {candidates.filter((c) => c.gate_status_json?.overall_pass).length}</span>
            <span>{t("expansionAdvisor.topDistrict")}: {(candidates.find((c) => c.district)?.district) || "-"}</span>
            <span>{t("expansionAdvisor.selectedStrategy")}: {(brief.brand_profile?.primary_channel || "-")}/{(brief.brand_profile?.expansion_goal || "-")}</span>
          </div>
        ) : null}

        {getCompareRows(compareResult).length ? (
          <div>
            <h4>{t("expansionAdvisor.compareCandidates")}</h4>
            <table>
              <thead>
                <tr>
                  <th>candidate_id</th>
                  <th>final_score</th>
                  <th>confidence_grade</th>
                  <th>gate_pass</th>
                  <th>economics_score</th>
                  <th>brand_fit_score</th>
                  <th>provider_density_score</th>
                  <th>provider_whitespace_score</th>
                  <th>estimated_payback_months</th>
                  <th>payback_band</th>
                </tr>
              </thead>
              <tbody>
                {getCompareRows(compareResult).map((item) => (
                  <tr key={item.candidate_id}>
                    <td>{item.candidate_id}</td>
                    <td>{item.final_score ?? "-"}</td>
                    <td>{item.confidence_grade ?? "-"}</td>
                    <td>{item.gate_status_json?.overall_pass ? "pass" : "fail"}</td>
                    <td>{item.economics_score ?? "-"}</td>
                    <td>{item.brand_fit_score ?? "-"}</td>
                    <td>{item.provider_density_score ?? "-"}</td>
                    <td>{item.provider_whitespace_score ?? "-"}</td>
                    <td>{item.estimated_payback_months ?? "-"}</td>
                    <td>{item.payback_band ?? "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}

        {candidates.length ? (
          <ExpansionResultsPanel
            items={candidates}
            selectedCandidateId={selectedCandidate?.id || null}
            shortlistIds={shortlistIds}
            compareIds={compareIds}
            onSelectCandidate={handleSelectCandidate}
            onToggleShortlist={(candidateId) =>
              setShortlistIds((current) => (current.includes(candidateId) ? current.filter((id) => id !== candidateId) : [...current, candidateId]))
            }
            onToggleCompare={(candidateId) =>
              setCompareIds((current) =>
                current.includes(candidateId) ? current.filter((id) => id !== candidateId) : current.length < 6 ? [...current, candidateId] : current,
              )
            }
          />
        ) : (
          <div>{loadingSearch ? t("expansionAdvisor.loadingSearch") : t("expansionAdvisor.noCandidates")}</div>
        )}
        <ExpansionMemoPanel memo={memo} loading={loadingMemo} />
        <ExpansionReportPanel report={report as any} />
      </div>
    </div>
  );
}
