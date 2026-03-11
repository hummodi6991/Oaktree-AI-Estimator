import { useEffect, useMemo, useRef, useState } from "react";
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

export function resolveCandidateById(candidates: ExpansionCandidate[], candidateId?: string | null): ExpansionCandidate | null {
  if (!candidateId) return null;
  return candidates.find((item) => item.id === candidateId) || null;
}

export function restoreSavedUiState(saved: SavedExpansionSearch, candidates: ExpansionCandidate[] = []) {
  const uiState = (saved.ui_state_json || {}) as Record<string, unknown>;
  const compareIds = Array.isArray(uiState.compare_ids) ? (uiState.compare_ids as string[]) : [];
  const selectedCandidateId = typeof uiState.selected_candidate_id === "string" ? uiState.selected_candidate_id : null;

  return {
    searchId: saved.search_id || "",
    shortlistIds: saved.selected_candidate_ids || [],
    compareIds,
    selectedCandidateId,
    selectedCandidate: resolveCandidateById(candidates, selectedCandidateId),
  };
}

export function shouldLoadMemoFromMapSelection(externalCandidateId: string | null | undefined, selectedCandidateId: string | null) {
  return Boolean(externalCandidateId && externalCandidateId !== selectedCandidateId);
}

export function getCompareRows(compareResult: CompareCandidatesResponse | null): CompareCandidateItem[] {
  return compareResult?.items || [];
}

export function shouldKeepCompareResult(compareIds: string[], compareResult: CompareCandidatesResponse | null): boolean {
  if (!compareResult || !compareResult.items.length) return false;
  const resultIds = compareResult.items.map((item) => item.candidate_id);
  if (resultIds.length !== compareIds.length) return false;
  return resultIds.every((id, idx) => id === compareIds[idx]);
}

export function getNextCompareIds(current: string[], candidateId: string): string[] {
  if (current.includes(candidateId)) return current.filter((id) => id !== candidateId);
  if (current.length >= 6) return current;
  return [...current, candidateId];
}

export function getNewSearchResetState() {
  return {
    selectedCandidate: null,
    shortlistIds: [],
    compareIds: [],
    compareResult: null,
    memo: null,
    report: null,
    memoError: null,
    reportError: null,
    compareError: null,
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

export function sameCandidateId(a: ExpansionCandidate | null, b: ExpansionCandidate | null): boolean {
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
  const [saving, setSaving] = useState(false);
  const memoAnchorRef = useRef<HTMLDivElement | null>(null);

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

  useEffect(() => {
    if (shouldKeepCompareResult(compareIds, compareResult)) return;
    if (compareResult !== null) {
      setCompareResult(null);
    }
  }, [compareIds, compareResult]);

  const loadReport = async (targetSearchId: string) => {
    if (!targetSearchId) return;
    setLoadingReport(true);
    setReportError(null);
    try {
      setReport(await getExpansionRecommendationReport(targetSearchId));
    } catch {
      setReportError(t("expansionAdvisor.errorReport"));
    } finally {
      setLoadingReport(false);
    }
  };

  const loadCompare = async (targetSearchId: string, targetCompareIds: string[]) => {
    if (!targetSearchId || targetCompareIds.length < 2 || targetCompareIds.length > 6) {
      setCompareResult(null);
      return;
    }
    setLoadingCompare(true);
    setCompareError(null);
    try {
      setCompareResult(await compareExpansionCandidates(targetSearchId, targetCompareIds));
    } catch {
      setCompareError(t("expansionAdvisor.errorCompare"));
    } finally {
      setLoadingCompare(false);
    }
  };

  const refreshSavedStudies = async () => {
    const latest = await listSavedExpansionSearches();
    setSavedItems(latest.items || []);
  };

  const handleSelectCandidate = async (candidate: ExpansionCandidate, forceReloadMemo = false) => {
    if (sameCandidateId(candidate, selectedCandidate) && !forceReloadMemo) {
      return;
    }
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
      requestAnimationFrame(() => memoAnchorRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }));
    }
  };

  const handleSelectCandidateById = async (candidateId?: string | null, forceReloadMemo = false) => {
    const target = resolveCandidateById(candidates, candidateId);
    if (!target) {
      return;
    }
    await handleSelectCandidate(target, forceReloadMemo);
  };

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
    setCandidates([]);
    setMemoError(resetState.memoError);
    setReportError(resetState.reportError);
    setCompareError(resetState.compareError);
    setSavedLoadError(null);
    setSearchId("");
    try {
      const result = await createExpansionSearch(nextBrief);
      setSearchId(result.search_id);
      setCandidates(normalizeCandidates(result.items || []));
      void loadReport(result.search_id);
    } catch {
      setSearchError(t("expansionAdvisor.errorSearch"));
    } finally {
      setLoadingSearch(false);
    }
  };

  const hydrateSavedStudy = async (saved: SavedExpansionSearch) => {
    setMemo(null);
    setReport(null);
    setCompareResult(null);
    setMemoError(null);
    setReportError(null);
    setCompareError(null);
    setSaveError(null);

    let hydratedCandidates = normalizeCandidates(saved.candidates || []);
    let hydratedSaved = saved;

    try {
      if (saved.search_id) {
        const [searchDetail, candidateList] = await Promise.all([getExpansionSearch(saved.search_id), getExpansionCandidates(saved.search_id)]);
        hydratedCandidates = normalizeCandidates(candidateList.items || []);
        hydratedSaved = {
          ...saved,
          search: searchDetail,
          candidates: hydratedCandidates,
        };
      }
    } catch {
      // fall back to embedded saved payload
    }

    const restored = restoreSavedUiState(hydratedSaved, hydratedCandidates);
    setSearchId(restored.searchId);
    setBrief(briefFromSavedSearch(hydratedSaved));
    setCandidates(hydratedCandidates);
    setShortlistIds(restored.shortlistIds.filter((id) => Boolean(resolveCandidateById(hydratedCandidates, id))));
    const restoredCompareIds = restored.compareIds.filter((id) => Boolean(resolveCandidateById(hydratedCandidates, id)));
    setCompareIds(restoredCompareIds);

    if (restored.selectedCandidateId) {
      const selected = resolveCandidateById(hydratedCandidates, restored.selectedCandidateId);
      if (selected) {
        await handleSelectCandidate(selected, true);
      } else {
        setSelectedCandidate(null);
        onSelectedCandidateChange(null);
      }
    } else {
      setSelectedCandidate(null);
      onSelectedCandidateChange(null);
    }

    if (restored.searchId && restoredCompareIds.length >= 2 && restoredCompareIds.length <= 6) {
      await loadCompare(restored.searchId, restoredCompareIds);
    }

    if (restored.searchId) {
      await loadReport(restored.searchId);
    }
  };

  useEffect(() => {
    if (!shouldLoadMemoFromMapSelection(externalSelectedCandidateId, selectedCandidate?.id || null)) return;
    const target = resolveCandidateById(candidates, externalSelectedCandidateId);
    if (!target) {
      if (selectedCandidate !== null) {
        setSelectedCandidate(null);
        setMemo(null);
        setMemoError(null);
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
              await loadCompare(searchId, compareIds);
            }}
            onSelectCandidateId={(candidateId) => {
              void handleSelectCandidateById(candidateId);
            }}
          />
          <button
            onClick={async () => {
              if (!searchId) return;
              setSaving(true);
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
                await refreshSavedStudies();
              } catch {
                setSaveError(t("expansionAdvisor.errorSavedLoad"));
              } finally {
                setSaving(false);
              }
            }}
            disabled={!searchId || saving}
          >
            {saving ? t("common.loading") : t("expansionAdvisor.saveSearch")}
          </button>
          <button
            onClick={async () => {
              if (!searchId) return;
              await loadReport(searchId);
            }}
          >
            {t("expansionAdvisor.loadReport")}
          </button>
        </div>
        {saveError ? <small>{saveError}</small> : null}

        {candidates.length ? (
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
            <span>
              {t("expansionAdvisor.totalCandidates")}: {candidates.length}
            </span>
            <span>
              {t("expansionAdvisor.passGates")}: {candidates.filter((c) => c.gate_status_json?.overall_pass).length}
            </span>
            <span>
              {t("expansionAdvisor.topDistrict")}: {candidates.find((c) => c.district)?.district || "-"}
            </span>
            <span>
              {t("expansionAdvisor.selectedStrategy")}: {(brief.brand_profile?.primary_channel || "-")}/{brief.brand_profile?.expansion_goal || "-"}
            </span>
          </div>
        ) : null}

        {candidates.length ? (
          <ExpansionResultsPanel
            items={candidates}
            selectedCandidateId={selectedCandidate?.id || null}
            shortlistIds={shortlistIds}
            compareIds={compareIds}
            onSelectCandidate={(candidate) => {
              void handleSelectCandidate(candidate);
            }}
            onToggleShortlist={(candidateId) =>
              setShortlistIds((current) => (current.includes(candidateId) ? current.filter((id) => id !== candidateId) : [...current, candidateId]))
            }
            onToggleCompare={(candidateId) => {
              setCompareIds((current) => getNextCompareIds(current, candidateId));
            }}
          />
        ) : (
          <div>{loadingSearch ? t("expansionAdvisor.loadingSearch") : t("expansionAdvisor.noCandidates")}</div>
        )}
        <div ref={memoAnchorRef} />
        {memoError ? <small>{memoError}</small> : null}
        <ExpansionMemoPanel memo={memo} loading={loadingMemo} />
        {reportError ? <small>{reportError}</small> : null}
        <ExpansionReportPanel
          report={report}
          loading={loadingReport}
          onSelectCandidateId={(candidateId) => {
            void handleSelectCandidateById(candidateId);
          }}
        />
      </div>
    </div>
  );
}
