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
  normalizeCandidates,
  type CandidateMemoResponse,
  type CompareCandidateItem,
  type CompareCandidatesResponse,
  type ExpansionBrief,
  type ExpansionCandidate,
  type RecommendationReportResponse,
  type SavedExpansionSearch,
} from "../../lib/api/expansionAdvisor";
import ExpansionBriefForm, { defaultBrief } from "./ExpansionBriefForm";
import ExpansionResultsPanel from "./ExpansionResultsPanel";
import ExpansionComparePanel from "./ExpansionComparePanel";
import ExpansionMemoPanel from "./ExpansionMemoPanel";
import SavedSearchesPanel from "./SavedSearchesPanel";
import ExpansionReportPanel from "./ExpansionReportPanel";

export function restoreSavedUiState(saved: SavedExpansionSearch) {
  const uiState = (saved.ui_state_json || {}) as Record<string, unknown>;
  const compareIds = Array.isArray(uiState.compare_ids) ? (uiState.compare_ids as string[]) : [];
  const selectedId = typeof uiState.selected_candidate_id === "string" ? uiState.selected_candidate_id : null;
  const selectedCandidate = selectedId ? (saved.candidates || []).find((item) => item.id === selectedId) || null : null;

  return {
    searchId: saved.search_id || "",
    shortlistIds: saved.selected_candidate_ids || [],
    compareIds,
    selectedCandidate,
  };
}

export function shouldLoadMemoFromMapSelection(externalCandidateId: string | null | undefined, selectedCandidateId: string | null) {
  return Boolean(externalCandidateId && externalCandidateId !== selectedCandidateId);
}

export function getCompareRows(compareResult: CompareCandidatesResponse | null): CompareCandidateItem[] {
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
  const [candidates, setCandidates] = useState<ExpansionCandidate[]>([]);
  const [searchId, setSearchId] = useState<string>("");
  const [selectedCandidate, setSelectedCandidate] = useState<ExpansionCandidate | null>(null);
  const [shortlistIds, setShortlistIds] = useState<string[]>([]);
  const [compareIds, setCompareIds] = useState<string[]>([]);
  const [memo, setMemo] = useState<CandidateMemoResponse | null>(null);
  const [savedItems, setSavedItems] = useState<SavedExpansionSearch[]>([]);
  const [compareResult, setCompareResult] = useState<CompareCandidatesResponse | null>(null);
  const [report, setReport] = useState<RecommendationReportResponse | null>(null);
  const [loadingSearch, setLoadingSearch] = useState(false);
  const [loadingMemo, setLoadingMemo] = useState(false);
  const [loadingCompare, setLoadingCompare] = useState(false);
  const [loadingReport, setLoadingReport] = useState(false);
  const [loadingSaved, setLoadingSaved] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [memoError, setMemoError] = useState<string | null>(null);
  const [compareError, setCompareError] = useState<string | null>(null);
  const [reportError, setReportError] = useState<string | null>(null);
  const [savedLoadError, setSavedLoadError] = useState<string | null>(null);

  useEffect(() => {
    listSavedExpansionSearches().then((res) => setSavedItems(res.items || [])).catch(() => {});
  }, []);

  useEffect(() => {
    onCandidatesChange(candidates, shortlistIds, selectedCandidate?.id || null, brief.existing_branches);
  }, [candidates, shortlistIds, selectedCandidate, brief.existing_branches, onCandidatesChange]);

  const onSubmitBrief = async (nextBrief: ExpansionBrief) => {
    setBrief(nextBrief);
    setLoadingSearch(true);
    setSearchError(null);
    try {
      const result = await createExpansionSearch(nextBrief);
      setSearchId(result.search_id);
      setCandidates(normalizeCandidates(result.items || []));
      setCompareResult(null);
      setMemo(null);
      setReport(null);
      setSelectedCandidate(null);
      setCompareIds([]);
    } catch {
      setSearchError(t("expansionAdvisor.errorSearch"));
    } finally {
      setLoadingSearch(false);
    }
  };

  const handleSelectCandidate = async (candidate: ExpansionCandidate) => {
    setSelectedCandidate(candidate);
    onSelectedCandidateChange(candidate);
    setLoadingMemo(true);
    setMemoError(null);
    try {
      setMemo(await getExpansionCandidateMemo(candidate.id));
    } catch {
      setMemoError(t("expansionAdvisor.errorMemo"));
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

  const title = useMemo(() => `${brief.brand_name || t("expansionAdvisor.title")} Study`, [brief.brand_name, t]);

  return (
    <div style={{ display: "grid", gridTemplateColumns: "300px 1fr", gap: 16 }}>
      <div style={{ display: "grid", gap: 16 }}>
        <ExpansionBriefForm initialValue={brief} loading={loadingSearch} onSubmit={onSubmitBrief} />
        {searchError ? <small>{searchError}</small> : null}
        <h4>{t("expansionAdvisor.savedStudies")}</h4>
        {savedLoadError ? <small>{savedLoadError}</small> : null}
        <SavedSearchesPanel
          items={savedItems}
          loading={loadingSaved}
          onOpen={async (savedId) => {
            setLoadingSaved(true);
            setSavedLoadError(null);
            try {
              const saved = await getSavedExpansionSearch(savedId);
              const restored = restoreSavedUiState(saved);
              setSearchId(restored.searchId);
              setShortlistIds(restored.shortlistIds);
              setCompareIds(restored.compareIds);
              setCandidates(normalizeCandidates(saved.candidates || []));
              if (saved.filters_json) setBrief(saved.filters_json as ExpansionBrief);
              setMemo(null);
              setReport(null);
              if (restored.selectedCandidate) {
                void handleSelectCandidate(restored.selectedCandidate);
              } else {
                setSelectedCandidate(null);
                onSelectedCandidateChange(null);
              }
            } catch {
              setSavedLoadError(t("expansionAdvisor.errorSavedLoad"));
            } finally {
              setLoadingSaved(false);
            }
          }}
        />
      </div>
      <div style={{ display: "grid", gap: 12 }}>
        <div style={{ display: "flex", gap: 8 }}>
          <ExpansionComparePanel
            compareIds={compareIds}
            result={compareResult}
            loading={loadingCompare}
            error={compareError}
            onCompare={async () => {
              if (!searchId) return;
              setLoadingCompare(true);
              setCompareError(null);
              try {
                setCompareResult(await compareExpansionCandidates(searchId, compareIds));
              } catch {
                setCompareError(t("expansionAdvisor.errorCompare"));
              } finally {
                setLoadingCompare(false);
              }
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
          <button onClick={async () => { if (!searchId) return; setLoadingReport(true); setReportError(null); try { setReport(await getExpansionRecommendationReport(searchId)); } catch { setReportError(t("expansionAdvisor.errorReport")); } finally { setLoadingReport(false); } }}>{t("expansionAdvisor.loadReport")}</button>
        </div>

        {candidates.length ? (
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
            <span>{t("expansionAdvisor.totalCandidates")}: {candidates.length}</span>
            <span>{t("expansionAdvisor.passGates")}: {candidates.filter((c) => c.gate_status_json?.overall_pass).length}</span>
            <span>{t("expansionAdvisor.topDistrict")}: {(candidates.find((c) => c.district)?.district) || "-"}</span>
            <span>{t("expansionAdvisor.selectedStrategy")}: {(brief.brand_profile?.primary_channel || "-")}/{(brief.brand_profile?.expansion_goal || "-")}</span>
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
        {memoError ? <small>{memoError}</small> : null}
        <ExpansionMemoPanel memo={memo} loading={loadingMemo} />
        {reportError ? <small>{reportError}</small> : null}
        <ExpansionReportPanel report={report} loading={loadingReport} />
      </div>
    </div>
  );
}
