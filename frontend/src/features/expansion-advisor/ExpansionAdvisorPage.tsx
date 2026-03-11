import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  compareExpansionCandidates,
  createExpansionSearch,
  createSavedExpansionSearch,
  getExpansionCandidates,
  getExpansionCandidateMemo,
  getExpansionSearch,
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
import type { ExpansionSearchDetailResponse } from "../../lib/api/expansionAdvisor";
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

export function getNewSearchResetState() {
  return {
    selectedCandidate: null,
    shortlistIds: [],
    compareIds: [],
    compareResult: null,
    memo: null,
    report: null,
  };
}

export function briefFromSavedSearch(saved: SavedExpansionSearch): ExpansionBrief {
  const filters = (saved.filters_json || {}) as Partial<ExpansionBrief>;
  if (filters.brand_name || filters.category || filters.service_model) {
    return {
      ...defaultBrief,
      ...filters,
      target_districts: filters.target_districts || [],
      existing_branches: filters.existing_branches || [],
      limit: filters.limit || defaultBrief.limit,
    };
  }

  const search = (saved.search || {}) as Partial<ExpansionSearchDetailResponse>;
  const requestJson = ((search.request_json || {}) as Partial<ExpansionBrief>) || {};

  return {
    ...defaultBrief,
    ...requestJson,
    brand_name: (requestJson.brand_name || search.brand_name || defaultBrief.brand_name) as string,
    category: (requestJson.category || search.category || defaultBrief.category) as string,
    service_model: (requestJson.service_model || search.service_model || defaultBrief.service_model) as ExpansionBrief["service_model"],
    min_area_m2: Number(requestJson.min_area_m2 || search.min_area_m2 || defaultBrief.min_area_m2),
    max_area_m2: Number(requestJson.max_area_m2 || search.max_area_m2 || defaultBrief.max_area_m2),
    target_area_m2: Number(requestJson.target_area_m2 || search.target_area_m2 || defaultBrief.target_area_m2 || 0) || null,
    target_districts: (requestJson.target_districts || search.target_districts || defaultBrief.target_districts) as string[],
    existing_branches: (requestJson.existing_branches || search.existing_branches || defaultBrief.existing_branches) as ExpansionBrief["existing_branches"],
    limit: Number(requestJson.limit || defaultBrief.limit),
    brand_profile: (requestJson.brand_profile || search.brand_profile || defaultBrief.brand_profile) as ExpansionBrief["brand_profile"],
  };
}

function sameCandidateId(a: ExpansionCandidate | null, b: ExpansionCandidate | null) {
  return (a?.id || null) === (b?.id || null);
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
  const [saveError, setSaveError] = useState<string | null>(null);

  useEffect(() => {
    setLoadingSaved(true);
    setSavedLoadError(null);
    listSavedExpansionSearches()
      .then((res) => setSavedItems(res.items || []))
      .catch(() => setSavedLoadError(t("expansionAdvisor.errorSavedLoad")))
      .finally(() => setLoadingSaved(false));
  }, [t]);

  useEffect(() => {
    onCandidatesChange(candidates, shortlistIds, selectedCandidate?.id || null, brief.existing_branches);
  }, [candidates, shortlistIds, selectedCandidate, brief.existing_branches, onCandidatesChange]);

  const onSubmitBrief = async (nextBrief: ExpansionBrief) => {
    setBrief(nextBrief);
    setLoadingSearch(true);
    setSaveError(null);
    setSearchError(null);
    const resetState = getNewSearchResetState();
    setSelectedCandidate(resetState.selectedCandidate);
    onSelectedCandidateChange(resetState.selectedCandidate);
    setShortlistIds(resetState.shortlistIds);
    setCompareIds(resetState.compareIds);
    setCompareResult(resetState.compareResult);
    setMemo(resetState.memo);
    setReport(resetState.report);
    try {
      const result = await createExpansionSearch(nextBrief);
      setSearchId(result.search_id);
      setCandidates(normalizeCandidates(result.items || []));
    } catch {
      setSearchError(t("expansionAdvisor.errorSearch"));
    } finally {
      setLoadingSearch(false);
    }
  };

  const hydrateSavedStudy = async (saved: SavedExpansionSearch) => {
    const restored = restoreSavedUiState(saved);
    const nextBrief = briefFromSavedSearch(saved);
    setMemo(null);
    setReport(null);
    setCompareResult(null);
    setMemoError(null);
    setReportError(null);
    setCompareError(null);
    setSaveError(null);

    setSearchId(restored.searchId);
    setShortlistIds(restored.shortlistIds);
    setCompareIds(restored.compareIds);
    setBrief(nextBrief);

    let hydratedCandidates = normalizeCandidates(saved.candidates || []);
    try {
      if (restored.searchId) {
        const [searchDetail, candidateList] = await Promise.all([
          getExpansionSearch(restored.searchId),
          getExpansionCandidates(restored.searchId),
        ]);
        hydratedCandidates = normalizeCandidates(candidateList.items || []);
        setBrief(
          briefFromSavedSearch({
            ...saved,
            search: searchDetail,
            candidates: hydratedCandidates,
          }),
        );
      }
    } catch {
      // fall back to embedded saved payload; handled by caller only if saved fetch itself failed
    }

    setCandidates(hydratedCandidates);

    const selectedFromHydrated =
      restored.selectedCandidate?.id
        ? hydratedCandidates.find((item) => item.id === restored.selectedCandidate?.id) || null
        : null;
    setSelectedCandidate(selectedFromHydrated);

    if (selectedFromHydrated) {
      await handleSelectCandidate(selectedFromHydrated);
    } else {
      onSelectedCandidateChange(null);
    }

    if (restored.searchId && restored.compareIds.length >= 2 && restored.compareIds.length <= 6) {
      setLoadingCompare(true);
      setCompareError(null);
      try {
        setCompareResult(await compareExpansionCandidates(restored.searchId, restored.compareIds));
      } catch {
        setCompareError(t("expansionAdvisor.errorCompare"));
      } finally {
        setLoadingCompare(false);
      }
    }

    if (restored.searchId) {
      setLoadingReport(true);
      setReportError(null);
      try {
        setReport(await getExpansionRecommendationReport(restored.searchId));
      } catch {
        setReportError(t("expansionAdvisor.errorReport"));
      } finally {
        setLoadingReport(false);
      }
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
    if (!target) {
      if (selectedCandidate !== null) {
        setSelectedCandidate(null);
        onSelectedCandidateChange(null);
      }
      return;
    }
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
              await hydrateSavedStudy(saved);
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
              setSaveError(null);
              try {
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
              } catch {
                setSaveError(t("expansionAdvisor.errorSavedLoad"));
              }
            }}
          >
            {t("expansionAdvisor.saveSearch")}
          </button>
          <button onClick={async () => { if (!searchId) return; setLoadingReport(true); setReportError(null); try { setReport(await getExpansionRecommendationReport(searchId)); } catch { setReportError(t("expansionAdvisor.errorReport")); } finally { setLoadingReport(false); } }}>{t("expansionAdvisor.loadReport")}</button>
        </div>
        {saveError ? <small>{saveError}</small> : null}

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
            onSelectCandidate={(candidate) => {
              if (sameCandidateId(candidate, selectedCandidate)) {
                return;
              }
              void handleSelectCandidate(candidate);
            }}
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
