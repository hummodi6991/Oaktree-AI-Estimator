import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  compareExpansionCandidates,
  createExpansionSearch,
  createSavedExpansionSearch,
  deleteSavedExpansionSearch,
  getExpansionCandidates,
  getExpansionCandidateMemo,
  getExpansionSearch,
  getExpansionRecommendationReport,
  getSavedExpansionSearch,
  listSavedExpansionSearches,
  normalizeCandidates,
  updateSavedExpansionSearch,
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
import ExpansionMemoPanel, { type MemoDrawerSection } from "./ExpansionMemoPanel";
import SavedSearchesPanel from "./SavedSearchesPanel";
import ExpansionReportPanel from "./ExpansionReportPanel";
import SaveStudyDialog from "./SaveStudyDialog";
import SortFilterBar from "./SortFilterBar";
import {
  normalizeBriefPayload,
  filterCandidates,
  sortCandidates,
  extractDistricts,
  generateStudyTitle,
  restoreLeadCandidateId,
  restoreSortFilter,
  buildUiStateJson,
  restoreMapViewState,
  restoreDrawerState,
  type FilterKey,
  type SortKey,
  type MemoCache,
  type ReportCache,
  type MapViewState,
  type DrawerKey,
  memoCacheKey,
  reportCacheKey,
  extractSavedStudyMeta,
} from "./studyAdapters";
import CompareOutcomeBanner from "./CompareOutcomeBanner";
import { CandidateListSkeleton } from "./SkeletonLoaders";
import { trackEvent } from "../../api";
import "./expansion-advisor.css";

/* ─── Pure helpers (exported for tests) ─── */

export function resolveCandidateById(candidates: ExpansionCandidate[], candidateId?: string | null): ExpansionCandidate | null {
  if (!candidateId) return null;
  return candidates.find((item) => item.id === candidateId) || null;
}

export function restoreSavedUiState(saved: SavedExpansionSearch, candidates: ExpansionCandidate[] = []) {
  const uiState = (saved.ui_state_json || {}) as Record<string, unknown>;
  const compareIds = Array.isArray(uiState.compare_ids) ? (uiState.compare_ids as string[]) : [];
  const selectedCandidateId = typeof uiState.selected_candidate_id === "string" ? uiState.selected_candidate_id : null;
  const leadCandidateId = restoreLeadCandidateId(saved.ui_state_json, candidates);
  const sortFilter = restoreSortFilter(saved.ui_state_json);
  const mapView = restoreMapViewState(saved.ui_state_json);
  const drawerState = restoreDrawerState(saved.ui_state_json);
  return {
    searchId: saved.search_id || "",
    shortlistIds: saved.selected_candidate_ids || [],
    compareIds,
    selectedCandidateId,
    selectedCandidate: resolveCandidateById(candidates, selectedCandidateId),
    leadCandidateId,
    mapView,
    drawerState,
    ...sortFilter,
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
  return { selectedCandidate: null, shortlistIds: [] as string[], compareIds: [] as string[], compareResult: null as CompareCandidatesResponse | null, memo: null as CandidateMemoResponse | null, report: null as RecommendationReportResponse | null, memoError: null as string | null, reportError: null as string | null, compareError: null as string | null };
}

export function briefFromSavedSearch(saved: SavedExpansionSearch): ExpansionBrief {
  const filters = (saved.filters_json || {}) as Partial<ExpansionBrief>;
  if (filters.brand_name || filters.category || filters.service_model) {
    return { ...defaultBrief, ...filters, target_districts: filters.target_districts || [], existing_branches: filters.existing_branches || [], limit: filters.limit || defaultBrief.limit };
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

/* ─── Component ─── */

export default function ExpansionAdvisorPage({
  onCandidatesChange,
  onSelectedCandidateChange,
  externalSelectedCandidateId,
  onMapViewRequest,
}: {
  onCandidatesChange: (candidates: ExpansionCandidate[], shortlistIds: string[], selectedId: string | null, branches: ExpansionBrief["existing_branches"], compareIds?: string[], leadCandidateId?: string | null) => void;
  onSelectedCandidateChange: (candidate: ExpansionCandidate | null) => void;
  externalSelectedCandidateId?: string | null;
  onMapViewRequest?: (view: MapViewState) => void;
}) {
  const { t, i18n } = useTranslation();
  const [brief, setBrief] = useState<ExpansionBrief>(defaultBrief);
  const [candidates, setCandidates] = useState<ExpansionCandidate[]>([]);
  const [searchId, setSearchId] = useState<string>("");
  const [selectedCandidate, setSelectedCandidate] = useState<ExpansionCandidate | null>(null);
  const [shortlistIds, setShortlistIds] = useState<string[]>([]);
  const [compareIds, setCompareIds] = useState<string[]>([]);
  const [leadCandidateId, setLeadCandidateId] = useState<string | null>(null);
  const [memo, setMemo] = useState<CandidateMemoResponse | null>(null);
  const [savedItems, setSavedItems] = useState<SavedExpansionSearch[]>([]);
  const [compareResult, setCompareResult] = useState<CompareCandidatesResponse | null>(null);
  const [report, setReport] = useState<RecommendationReportResponse | null>(null);
  const [loadingSearch, setLoadingSearch] = useState(false);
  const [searchStartTime, setSearchStartTime] = useState<number | null>(null);
  const [searchElapsedMs, setSearchElapsedMs] = useState<number | null>(null);
  const [liveElapsed, setLiveElapsed] = useState<number>(0);
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
  const [activeDrawer, setActiveDrawer] = useState<DrawerKey>("none");
  const [searchMeta, setSearchMeta] = useState<Record<string, unknown>>({});
  const [searchNotes, setSearchNotes] = useState<Record<string, unknown>>({});
  const [activeSavedId, setActiveSavedId] = useState<string | null>(null);
  const [activeSavedStatus, setActiveSavedStatus] = useState<"draft" | "final">("draft");
  const [mapViewState, setMapViewState] = useState<MapViewState>({});
  const [saveToast, setSaveToast] = useState<{ type: "success" | "error"; message: string } | null>(null);
  const [showSavedWorkspace, setShowSavedWorkspace] = useState(false);
  const [memoSection, setMemoSection] = useState<MemoDrawerSection | undefined>(undefined);
  const saveToastTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const showToast = useCallback((type: "success" | "error", message: string) => {
    if (saveToastTimerRef.current) clearTimeout(saveToastTimerRef.current);
    setSaveToast({ type, message });
    saveToastTimerRef.current = setTimeout(() => setSaveToast(null), 4000);
  }, []);

  // Brief edit/run state
  const [briefMode, setBriefMode] = useState<"edit" | "summary">("edit");

  // Sort & filter
  const [activeFilter, setActiveFilter] = useState<FilterKey>("all");
  const [activeSort, setActiveSort] = useState<SortKey>("rank");
  const [districtFilter, setDistrictFilter] = useState<string>("");

  // Caches
  const memoCacheRef = useRef<MemoCache>(new Map());
  const reportCacheRef = useRef<ReportCache>(new Map());

  // Load saved studies on mount
  useEffect(() => {
    setLoadingSaved(true);
    setSavedLoadError(null);
    listSavedExpansionSearches()
      .then((res) => {
        setSavedItems(res.items || []);
        setSavedLoadError(null);
      })
      .catch(() => {
        setSavedLoadError(t("expansionAdvisor.errorSavedLoad"));
      })
      .finally(() => setLoadingSaved(false));
  }, [t]);

  // Sync candidates to parent
  useEffect(() => {
    onCandidatesChange(candidates, shortlistIds, selectedCandidate?.id || null, brief.existing_branches, compareIds, leadCandidateId);
  }, [candidates, shortlistIds, selectedCandidate, brief.existing_branches, compareIds, leadCandidateId, onCandidatesChange]);

  // Clear stale compare result
  useEffect(() => {
    if (shouldKeepCompareResult(compareIds, compareResult)) return;
    if (compareResult !== null) setCompareResult(null);
  }, [compareIds, compareResult]);

  // Collapse brief to summary once search runs
  useEffect(() => {
    if (searchId && candidates.length > 0) setBriefMode("summary");
  }, [searchId, candidates.length]);

  // Live ticking timer while search is in progress
  useEffect(() => {
    if (!searchStartTime || searchElapsedMs !== null) return;
    const interval = setInterval(() => {
      setLiveElapsed(Date.now() - searchStartTime);
    }, 100);
    return () => clearInterval(interval);
  }, [searchStartTime, searchElapsedMs]);

  // When lead candidate changes, sync map focus and scroll finalist tile into view
  useEffect(() => {
    if (!leadCandidateId) return;
    const lead = resolveCandidateById(candidates, leadCandidateId);
    if (lead) {
      onSelectedCandidateChange(lead);
      requestAnimationFrame(() => {
        const el = document.querySelector(`[data-candidate-id="${lead.id}"]`);
        el?.scrollIntoView({ behavior: "smooth", block: "nearest" });
      });
    }
  }, [leadCandidateId]); // intentionally minimal deps — fires only when lead changes

  const loadReport = useCallback(async (targetSearchId: string) => {
    if (!targetSearchId) return;
    const cacheKey = reportCacheKey(targetSearchId);
    const cached = reportCacheRef.current.get(cacheKey);
    if (cached) { setReport(cached); setReportError(null); return; }
    setLoadingReport(true);
    setReportError(null);
    void trackEvent("ui_expansion_report_opened", { meta: { search_id: targetSearchId } });
    try {
      const result = await getExpansionRecommendationReport(targetSearchId);
      reportCacheRef.current.set(cacheKey, result);
      setReport(result);
      setReportError(null);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "";
      console.warn("[expansion-report] report fetch failed", { searchId: targetSearchId, error: msg, status: msg.slice(0, 3) });
      if (msg.startsWith("404") || msg.startsWith("422")) {
        // Search not found or validation error — soft-handle
        console.info("[expansion-report] report not available (soft)", targetSearchId, msg.slice(0, 3));
        setReport(null);
        setReportError(null);
      } else {
        // 500 or network failure — surface to user so real bugs are visible
        setReportError(t("expansionAdvisor.errorReport"));
      }
    } finally { setLoadingReport(false); }
  }, [t]);

  const loadCompare = async (targetSearchId: string, targetCompareIds: string[]) => {
    if (!targetSearchId || targetCompareIds.length < 2 || targetCompareIds.length > 6) { setCompareResult(null); return; }
    setLoadingCompare(true);
    setCompareError(null);
    void trackEvent("ui_expansion_compare_opened", { meta: { search_id: targetSearchId, count: targetCompareIds.length } });
    try { setCompareResult(await compareExpansionCandidates(targetSearchId, targetCompareIds)); } catch { setCompareError(t("expansionAdvisor.errorCompare")); } finally { setLoadingCompare(false); }
  };

  const refreshSavedStudies = async () => {
    const latest = await listSavedExpansionSearches();
    setSavedItems(latest.items || []);
  };

  const loadMemoForCandidate = useCallback(async (candidateId: string): Promise<CandidateMemoResponse | null> => {
    const cacheKey = memoCacheKey(candidateId);
    const cached = memoCacheRef.current.get(cacheKey);
    if (cached) return cached;
    const result = await getExpansionCandidateMemo(candidateId);
    memoCacheRef.current.set(cacheKey, result);
    return result;
  }, []);

  const handleSelectCandidate = async (candidate: ExpansionCandidate, forceReloadMemo = false) => {
    if (sameCandidateId(candidate, selectedCandidate) && !forceReloadMemo) return;
    setSelectedCandidate(candidate);
    onSelectedCandidateChange(candidate);
    setLoadingMemo(true);
    setMemoError(null);
    try {
      const memoResult = await loadMemoForCandidate(candidate.id);
      setMemo(memoResult);
    } catch { setMemoError(t("expansionAdvisor.errorMemo")); } finally {
      setLoadingMemo(false);
    }
  };

  const handleSelectCandidateById = async (candidateId?: string | null, forceReloadMemo = false) => {
    const target = resolveCandidateById(candidates, candidateId);
    if (!target) return;
    await handleSelectCandidate(target, forceReloadMemo);
  };

  const handleOpenMemoById = async (
    candidateId: string,
    options?: { section?: MemoDrawerSection },
  ) => {
    setMemoSection(options?.section);
    await handleSelectCandidateById(candidateId);
    setActiveDrawer("memo");
  };

  const onSubmitBrief = async (nextBrief: ExpansionBrief) => {
    const normalized = normalizeBriefPayload(nextBrief);
    const t0 = Date.now();
    setSearchStartTime(t0);
    setSearchElapsedMs(null);
    setLiveElapsed(0);
    setBrief(normalized);
    setLoadingSearch(true);
    setSaveError(null);
    setSearchError(null);
    const reset = getNewSearchResetState();
    setSelectedCandidate(reset.selectedCandidate);
    onSelectedCandidateChange(reset.selectedCandidate);
    setShortlistIds(reset.shortlistIds);
    setCompareIds(reset.compareIds);
    setCompareResult(reset.compareResult);
    setMemo(reset.memo);
    setReport(reset.report);
    setCandidates([]);
    setMemoError(reset.memoError);
    setReportError(reset.reportError);
    setCompareError(reset.compareError);
    setSavedLoadError(null);
    setSearchId("");
    setSearchMeta({});
    setSearchNotes({});
    setActiveDrawer("none");
    setActiveSavedId(null);
    setActiveSavedStatus("draft");
    setLeadCandidateId(null);
    setActiveFilter("all");
    setActiveSort("rank");
    setDistrictFilter("");
    memoCacheRef.current.clear();
    reportCacheRef.current.clear();
    void trackEvent("ui_expansion_search_started", { meta: { brand: normalized.brand_name, category: normalized.category } });
    try {
      const result = await createExpansionSearch(normalized);
      setSearchId(result.search_id);
      setCandidates(normalizeCandidates(result.items || []));
      setSearchMeta(result.meta || {});
      setSearchNotes(result.notes || {});
      void loadReport(result.search_id);
      void trackEvent("ui_expansion_search_completed", { meta: { search_id: result.search_id, count: (result.items || []).length } });
      setSearchElapsedMs(Date.now() - t0);
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      console.error("[ExpansionAdvisor] search failed:", detail);
      setSearchError(`${t("expansionAdvisor.errorSearch")} — ${detail}`);
      setSearchElapsedMs(Date.now() - t0);
    } finally { setLoadingSearch(false); }
  };

  const hydrateSavedStudy = async (saved: SavedExpansionSearch) => {
    void trackEvent("ui_expansion_saved_search_opened", { meta: { saved_id: saved.id, search_id: saved.search_id } });
    setMemo(null); setReport(null); setCompareResult(null);
    setMemoError(null); setReportError(null); setCompareError(null); setSaveError(null);
    setActiveDrawer("none");
    setActiveSavedId(saved.id);
    setActiveSavedStatus(saved.status || "draft");
    memoCacheRef.current.clear();
    reportCacheRef.current.clear();
    let hydratedCandidates = normalizeCandidates(saved.candidates || []);
    let hydratedSaved = saved;
    try {
      if (saved.search_id) {
        const [searchDetail, candidateList] = await Promise.all([getExpansionSearch(saved.search_id), getExpansionCandidates(saved.search_id)]);
        hydratedCandidates = normalizeCandidates(candidateList.items || []);
        hydratedSaved = { ...saved, search: searchDetail, candidates: hydratedCandidates };
      }
    } catch { /* fall back to embedded saved payload */ }
    const restored = restoreSavedUiState(hydratedSaved, hydratedCandidates);
    setSearchId(restored.searchId);
    setBrief(briefFromSavedSearch(hydratedSaved));
    setCandidates(hydratedCandidates);
    setShortlistIds(restored.shortlistIds.filter((id) => Boolean(resolveCandidateById(hydratedCandidates, id))));
    const restoredCompareIds = restored.compareIds.filter((id) => Boolean(resolveCandidateById(hydratedCandidates, id)));
    setCompareIds(restoredCompareIds);
    setLeadCandidateId(restored.leadCandidateId);
    setActiveFilter(restored.activeFilter);
    setActiveSort(restored.activeSort);
    setDistrictFilter(restored.districtFilter);
    setMapViewState(restored.mapView);
    if (restored.mapView.center && onMapViewRequest) {
      onMapViewRequest(restored.mapView);
    }
    if (restored.selectedCandidateId) {
      const selected = resolveCandidateById(hydratedCandidates, restored.selectedCandidateId);
      if (selected) await handleSelectCandidate(selected, true);
      else { setSelectedCandidate(null); onSelectedCandidateChange(null); }
    } else { setSelectedCandidate(null); onSelectedCandidateChange(null); }
    if (restored.searchId && restoredCompareIds.length >= 2 && restoredCompareIds.length <= 6) await loadCompare(restored.searchId, restoredCompareIds);
    if (restored.searchId) await loadReport(restored.searchId);
    if (hydratedCandidates.length > 0) setBriefMode("summary");
    // Restore drawer state only for content drawers, not save dialog
    if (restored.drawerState === "report" || restored.drawerState === "memo" || restored.drawerState === "compare") {
      setActiveDrawer(restored.drawerState);
    }
  };

  const handleDeleteSaved = async (savedId: string) => {
    try {
      await deleteSavedExpansionSearch(savedId);
      await refreshSavedStudies();
      if (activeSavedId === savedId) setActiveSavedId(null);
    } catch { setSavedLoadError(t("expansionAdvisor.errorDelete")); }
  };

  const handleRenameSaved = async (savedId: string, title: string) => {
    try {
      await updateSavedExpansionSearch(savedId, { title });
      await refreshSavedStudies();
      showToast("success", t("expansionAdvisor.studyRenamed"));
    } catch { showToast("error", t("expansionAdvisor.updateFailed")); }
  };

  const handleEditDescriptionSaved = async (savedId: string, description: string) => {
    try {
      await updateSavedExpansionSearch(savedId, { description });
      await refreshSavedStudies();
    } catch { showToast("error", t("expansionAdvisor.updateFailed")); }
  };

  const handleChangeStatusSaved = async (savedId: string, status: "draft" | "final") => {
    try {
      await updateSavedExpansionSearch(savedId, { status });
      await refreshSavedStudies();
      if (activeSavedId === savedId) setActiveSavedStatus(status);
      showToast("success", t("expansionAdvisor.studyStatusChanged"));
    } catch { showToast("error", t("expansionAdvisor.updateFailed")); }
  };

  const handleUpdateSaved = async (studyTitle: string, description: string, status: "draft" | "final") => {
    if (!activeSavedId) return;
    setSaving(true);
    setSaveError(null);
    try {
      await updateSavedExpansionSearch(activeSavedId, {
        title: studyTitle,
        description,
        status,
        selected_candidate_ids: shortlistIds,
        filters_json: brief as unknown as Record<string, unknown>,
        ui_state_json: buildUiStateJson(selectedCandidate?.id || null, compareIds, leadCandidateId, activeFilter, activeSort, districtFilter, mapViewState, activeDrawer),
      });
      await refreshSavedStudies();
      setActiveSavedStatus(status);
      setActiveDrawer("none");
      showToast("success", t("expansionAdvisor.studyUpdateSuccess"));
    } catch { setSaveError(t("expansionAdvisor.errorUpdate")); showToast("error", t("expansionAdvisor.updateFailed")); } finally { setSaving(false); }
  };

  // External map selection sync
  useEffect(() => {
    if (!shouldLoadMemoFromMapSelection(externalSelectedCandidateId, selectedCandidate?.id || null)) return;
    const target = resolveCandidateById(candidates, externalSelectedCandidateId);
    if (!target) {
      if (selectedCandidate !== null) { setSelectedCandidate(null); setMemo(null); setMemoError(null); onSelectedCandidateChange(null); }
      return;
    }
    void handleSelectCandidate(target);
    // Scroll candidate card into view
    requestAnimationFrame(() => {
      const el = document.querySelector(`[data-candidate-id="${target.id}"]`);
      el?.scrollIntoView({ behavior: "smooth", block: "nearest" });
    });
  }, [externalSelectedCandidateId, candidates, selectedCandidate?.id]);

  const title = useMemo(() => generateStudyTitle(brief), [brief]);
  const bestCandidate = candidates[0] || null;
  const hasResults = candidates.length > 0;
  const districts = useMemo(() => extractDistricts(candidates), [candidates]);

  // Compute filtered/sorted candidates
  const displayCandidates = useMemo(() => {
    let result = filterCandidates(candidates, activeFilter, districtFilter);
    if (activeSort !== "rank") {
      result = sortCandidates(result, activeSort);
    }
    return result;
  }, [candidates, activeFilter, activeSort, districtFilter]);

  const localSortActive = activeSort !== "rank" || activeFilter !== "all" || districtFilter !== "";
  const compareShortlistEnabled = shortlistIds.length >= 2 && shortlistIds.length <= 6;
  const showBriefForm = briefMode === "edit" || !hasResults;

  return (
    <div className="ea-page">
      {/* Page title */}
      {hasResults && (
        <h2 className="ea-page__title">{title}</h2>
      )}

      {/* Header toolbar — shown after search completes, in summary mode */}
      {hasResults && briefMode === "summary" && (
        <div className="ea-actions-bar">
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--tertiary"
            onClick={() => setBriefMode("edit")}
          >
            {t("expansionAdvisor.editBrief")}
          </button>
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--tertiary"
            onClick={() => void onSubmitBrief(brief)}
            disabled={loadingSearch}
          >
            {loadingSearch ? t("expansionAdvisor.searchingCta") : t("expansionAdvisor.runAgain")}
          </button>
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--tertiary"
            onClick={() => { void loadReport(searchId); setActiveDrawer("report"); }}
          >
            {t("expansionAdvisor.openExecutiveReport")}
          </button>
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--primary"
            onClick={() => setActiveDrawer("save")}
            disabled={!searchId}
          >
            {activeSavedId ? t("expansionAdvisor.updateStudy") : t("expansionAdvisor.saveStudy")}
          </button>
          {savedItems.length > 0 && (
            <button
              type="button"
              className="oak-btn oak-btn--sm oak-btn--tertiary"
              onClick={() => setShowSavedWorkspace(true)}
            >
              {t("expansionAdvisor.savedStudiesWorkspace")}
            </button>
          )}
          {compareIds.length >= 2 && compareIds.length <= 6 && (
            <button
              type="button"
              className="oak-btn oak-btn--sm oak-btn--primary"
              disabled={loadingCompare}
              onClick={async () => {
                await loadCompare(searchId, compareIds);
                setActiveDrawer("compare");
              }}
            >
              {t("expansionAdvisor.compareSelected", { count: compareIds.length })}
            </button>
          )}
        </div>
      )}

      {/* Brief form — shown in edit mode or before first search */}
      {showBriefForm && !loadingSearch && (
        <div className="ea-card">
          <div className="ea-card__header">
            <h3 className="ea-card__title">{t("expansionAdvisor.brandBrief")}</h3>
            <span className="ea-card__subtitle">{t("expansionAdvisor.heroSubtitle")}</span>
          </div>
          <div className="ea-card__body">
            <ExpansionBriefForm initialValue={brief} loading={loadingSearch} onSubmit={onSubmitBrief} />
            {searchError && <div className="ea-state ea-state--error" style={{ marginTop: 8 }}>{searchError}</div>}
          </div>
        </div>
      )}

      {/* Summary strip */}
      {hasResults && (
        <div className="ea-summary-strip">
          <span className="ea-summary-strip__value">
            {t("expansionAdvisor.summaryLocationsFound", { count: candidates.length })}
            {bestCandidate && bestCandidate.final_score != null
              ? ` · ${t("expansionAdvisor.summaryTopScore", { score: Math.round(bestCandidate.final_score) })}`
              : ""}
          </span>
        </div>
      )}

      {/* Search elapsed time — footnote style */}
      {hasResults && searchElapsedMs !== null && (
        <div style={{ textAlign: "end", fontSize: "0.7rem", color: "var(--oak-text-light, #9ca3af)", padding: "0.1rem 0" }}>
          {t("expansionAdvisor.searchCompletedIn", { seconds: (searchElapsedMs / 1000).toFixed(1) })}
        </div>
      )}

      {/* Sort & filter bar */}
      {hasResults && (
        <SortFilterBar
          activeFilter={activeFilter}
          activeSort={activeSort}
          districtFilter={districtFilter}
          districts={districts}
          totalCount={candidates.length}
          filteredCount={displayCandidates.length}
          onFilterChange={setActiveFilter}
          onSortChange={setActiveSort}
          onDistrictFilterChange={setDistrictFilter}
        />
      )}

      {/* Missing districts banner */}
      {hasResults && Array.isArray(searchNotes.districts_with_no_candidates) && (searchNotes.districts_with_no_candidates as string[]).length > 0 && (
        <div className="ea-missing-districts-banner">
          <span className="ea-missing-districts-banner__icon">ℹ️</span>
          <span>
            {t("expansionAdvisor.noMatchingParcelsInDistricts", {
              districts: (searchNotes.districts_with_no_candidates as string[]).join(", "),
            })}
            {searchNotes.districts_no_candidates_reason ? (
              <> — {String(searchNotes.districts_no_candidates_reason)}</>
            ) : null}
          </span>
        </div>
      )}

      {/* Candidate list */}
      {hasResults ? (
        <ExpansionResultsPanel
          items={displayCandidates}
          selectedCandidateId={selectedCandidate?.id || null}
          shortlistIds={shortlistIds}
          compareIds={compareIds}
          leadCandidateId={leadCandidateId}
          localSortActive={localSortActive}
          onSelectCandidate={(candidate) => { void handleSelectCandidate(candidate); void trackEvent("ui_expansion_candidate_opened", { meta: { candidate_id: candidate.id } }); }}
          onToggleCompare={(candidateId) => setCompareIds((cur) => getNextCompareIds(cur, candidateId))}
          onOpenMemo={(candidateId, options) => void handleOpenMemoById(candidateId, options)}
          onShowOnMap={(candidate) => {
            onSelectedCandidateChange(candidate);
            setSelectedCandidate(candidate);
          }}
        />
      ) : loadingSearch ? (
        <>
          {searchStartTime && searchElapsedMs === null && (
            <div className="ea-search-timer" style={{ textAlign: "center", padding: "0.5rem 0", fontSize: "0.85rem", color: "#6b7280", fontVariantNumeric: "tabular-nums" }}>
              {t("expansionAdvisor.searchingCta")} {(liveElapsed / 1000).toFixed(1)}s
            </div>
          )}
          <CandidateListSkeleton count={5} />
        </>
      ) : (
        <div className="ea-first-run">
          <div className="ea-first-run__hero">
            <h3 className="ea-first-run__title">{t("expansionAdvisor.heroTitle")}</h3>
            <p className="ea-first-run__subtitle">{t("expansionAdvisor.heroSubtitle")}</p>
          </div>
          {/* Resume previous study prompt */}
          {savedItems.length > 0 && (
            <div className="ea-first-run__resume">
              <p className="ea-first-run__resume-text">{t("expansionAdvisor.resumeStudyPrompt")}</p>
              <div className="ea-first-run__resume-list">
                {savedItems.slice(0, 3).map((item) => {
                  const meta = extractSavedStudyMeta(item);
                  return (
                    <button
                      key={item.id}
                      className="ea-first-run__resume-item"
                      onClick={async () => {
                        setLoadingSaved(true);
                        setSavedLoadError(null);
                        try { const saved = await getSavedExpansionSearch(item.id); await hydrateSavedStudy(saved); } catch { setSavedLoadError(t("expansionAdvisor.errorSavedLoad")); } finally { setLoadingSaved(false); }
                      }}
                    >
                      <span className="ea-first-run__resume-title">{item.title}</span>
                      <span className="ea-first-run__resume-meta">
                        <span className={`ea-badge ea-badge--${meta.isFinal ? "green" : "neutral"}`} style={{ fontSize: "var(--oak-fs-xs)" }}>
                          {meta.isFinal ? t("expansionAdvisor.savedStudyFinal") : t("expansionAdvisor.savedStudyDraft")}
                        </span>
                        {meta.shortlistCount > 0 && <span>{t("expansionAdvisor.shortlistCountBadge", { count: meta.shortlistCount })}</span>}
                      </span>
                    </button>
                  );
                })}
              </div>
              {savedItems.length > 3 && (
                <button className="oak-btn oak-btn--sm oak-btn--tertiary" style={{ marginTop: 8 }} onClick={() => setShowSavedWorkspace(true)}>
                  {t("expansionAdvisor.openSavedStudies")} ({savedItems.length})
                </button>
              )}
            </div>
          )}
          <div className="ea-first-run__divider">
            <span>{savedItems.length > 0 ? t("expansionAdvisor.startNewStudy") : ""}</span>
          </div>
          <ol className="ea-first-run__steps">
            <li className="ea-first-run__step">{t("expansionAdvisor.workflowStep1")}</li>
            <li className="ea-first-run__step">{t("expansionAdvisor.workflowStep2")}</li>
            <li className="ea-first-run__step">{t("expansionAdvisor.workflowStep3")}</li>
            <li className="ea-first-run__step">{t("expansionAdvisor.workflowStep4")}</li>
            <li className="ea-first-run__step">{t("expansionAdvisor.workflowStep5")}</li>
            <li className="ea-first-run__step">{t("expansionAdvisor.workflowStep6")}</li>
          </ol>
        </div>
      )}

      {/* Saved load error (surfaced when loading a saved study fails) */}
      {savedLoadError && (
        <div className="ea-state ea-state--error">{savedLoadError}</div>
      )}

      {/* ─── Drawers / dialogs ─── */}

      {activeDrawer === "memo" && (
        <ExpansionMemoPanel
          memo={memo}
          loading={loadingMemo}
          isLeadCandidate={selectedCandidate?.id === leadCandidateId}
          report={report}
          candidateRaw={selectedCandidate as unknown as Record<string, unknown>}
          briefRaw={brief as unknown as Record<string, unknown>}
          lang={i18n.language?.startsWith("ar") ? "ar" : "en"}
          searchId={searchId || null}
          initialSection={memoSection}
          onClose={() => { setActiveDrawer("none"); setMemoSection(undefined); }}
          onBackToDetail={() => { setActiveDrawer("none"); setMemoSection(undefined); }}
          onBackToCompare={compareResult ? () => { setActiveDrawer("compare"); setMemoSection(undefined); } : undefined}
          onOpenCompare={compareShortlistEnabled ? async () => {
            setCompareIds(shortlistIds.slice(0, 6));
            await loadCompare(searchId, shortlistIds.slice(0, 6));
            setActiveDrawer("compare");
            setMemoSection(undefined);
          } : undefined}
          hasShortlist={shortlistIds.length >= 2}
          hasCompare={Boolean(compareResult)}
        />
      )}

      {activeDrawer === "compare" && (
        <>
          {compareResult && (
            <CompareOutcomeBanner
              result={compareResult}
              candidates={candidates}
              leadCandidateId={leadCandidateId}
              onSelectCandidateId={(candidateId) => { setActiveDrawer("none"); void handleSelectCandidateById(candidateId); }}
            />
          )}
          <ExpansionComparePanel
            compareIds={compareIds}
            result={compareResult}
            loading={loadingCompare}
            error={compareError}
            leadCandidateId={leadCandidateId}
            onCompare={async () => { if (searchId) await loadCompare(searchId, compareIds); }}
            onSelectCandidateId={(candidateId) => { setActiveDrawer("none"); void handleSelectCandidateById(candidateId); }}
            onClose={() => setActiveDrawer("none")}
          />
        </>
      )}

      {activeDrawer === "report" && (
        <ExpansionReportPanel
          report={report}
          loading={loadingReport}
          error={reportError}
          leadCandidateId={leadCandidateId}
          leadCandidate={resolveCandidateById(candidates, leadCandidateId)}
          memo={leadCandidateId && selectedCandidate?.id === leadCandidateId ? memo : null}
          onSelectCandidateId={(candidateId) => { setActiveDrawer("none"); void handleSelectCandidateById(candidateId); }}
          onClose={() => setActiveDrawer("none")}
        />
      )}

      {activeDrawer === "save" && (() => {
        const activeSaved = activeSavedId ? savedItems.find((s) => s.id === activeSavedId) : null;
        return <SaveStudyDialog
          defaultTitle={activeSaved?.title || title}
          defaultDescription={activeSaved?.description || undefined}
          defaultStatus={activeSaved?.status || undefined}
          saving={saving}
          error={saveError}
          isUpdate={Boolean(activeSavedId)}
          onClose={() => setActiveDrawer("none")}
          onSave={async (studyTitle, description, status) => {
            if (activeSavedId) {
              await handleUpdateSaved(studyTitle, description, status);
              return;
            }
            setSaving(true);
            setSaveError(null);
            try {
              const created = await createSavedExpansionSearch({
                search_id: searchId,
                title: studyTitle,
                description,
                status,
                selected_candidate_ids: shortlistIds,
                filters_json: brief as unknown as Record<string, unknown>,
                ui_state_json: buildUiStateJson(selectedCandidate?.id || null, compareIds, leadCandidateId, activeFilter, activeSort, districtFilter, mapViewState, activeDrawer),
              });
              setActiveSavedId(created.id);
              setActiveSavedStatus(status);
              await refreshSavedStudies();
              setActiveDrawer("none");
              showToast("success", t("expansionAdvisor.studySaveSuccess"));
              void trackEvent("ui_expansion_saved_search_created", { meta: { saved_id: created.id, search_id: searchId } });
            } catch { setSaveError(t("expansionAdvisor.saveFailed")); showToast("error", t("expansionAdvisor.saveFailed")); } finally { setSaving(false); }
          }}
        />;
      })()}

      {reportError && activeDrawer !== "report" && <div className="ea-state ea-state--error">{reportError}</div>}

      {/* Save toast */}
      {saveToast && (
        <div className={`ea-toast ea-toast--${saveToast.type}`} onClick={() => setSaveToast(null)}>
          {saveToast.message}
        </div>
      )}

      {/* Saved studies workspace overlay */}
      {showSavedWorkspace && (
        <div className="ea-dialog-backdrop" onClick={() => setShowSavedWorkspace(false)}>
          <div className="ea-dialog ea-dialog--wide" onClick={(e) => e.stopPropagation()}>
            <div className="ea-dialog__header">
              <h3 className="ea-dialog__title">{t("expansionAdvisor.savedStudiesWorkspace")}</h3>
              <button className="ea-drawer__close" onClick={() => setShowSavedWorkspace(false)}>{t("expansionAdvisor.close")}</button>
            </div>
            <div className="ea-dialog__body">
              <SavedSearchesPanel
                items={savedItems}
                loading={loadingSaved}
                activeSavedId={activeSavedId}
                onOpen={async (savedId) => {
                  setLoadingSaved(true);
                  setSavedLoadError(null);
                  setShowSavedWorkspace(false);
                  try { const saved = await getSavedExpansionSearch(savedId); await hydrateSavedStudy(saved); } catch { setSavedLoadError(t("expansionAdvisor.errorSavedLoad")); } finally { setLoadingSaved(false); }
                }}
                onDelete={handleDeleteSaved}
                onRename={handleRenameSaved}
                onEditDescription={handleEditDescriptionSaved}
                onChangeStatus={handleChangeStatusSaved}
              />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
