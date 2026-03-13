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
import ExpansionMemoPanel from "./ExpansionMemoPanel";
import SavedSearchesPanel from "./SavedSearchesPanel";
import ExpansionReportPanel from "./ExpansionReportPanel";
import CandidateDetailPanel from "./CandidateDetailPanel";
import SaveStudyDialog from "./SaveStudyDialog";
import BriefSummaryRail from "./BriefSummaryRail";
import StudyHeader from "./StudyHeader";
import ShortlistTray from "./ShortlistTray";
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
import FinalistsWorkspace from "./FinalistsWorkspace";
import DecisionChecklist from "./DecisionChecklist";
import NextStepsStrip from "./NextStepsStrip";
import CopySummaryBlock from "./CopySummaryBlock";
import ValidationPlanPanel from "./ValidationPlanPanel";
import AssumptionsCard from "./AssumptionsCard";
import DecisionSnapshotCard from "./DecisionSnapshotCard";
import CompareOutcomeBanner from "./CompareOutcomeBanner";
import { CandidateListSkeleton, DetailSkeleton } from "./SkeletonLoaders";
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
  const { t } = useTranslation();
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
  const [activeSavedId, setActiveSavedId] = useState<string | null>(null);
  const [activeSavedStatus, setActiveSavedStatus] = useState<"draft" | "final">("draft");
  const [mapViewState, setMapViewState] = useState<MapViewState>({});
  const [saveToast, setSaveToast] = useState<{ type: "success" | "error"; message: string } | null>(null);
  const [showSavedWorkspace, setShowSavedWorkspace] = useState(false);
  const detailRef = useRef<HTMLDivElement | null>(null);
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
      .catch((err) => {
        // Treat "not found" (404) and "table missing" (500) as empty —
        // these indicate the saved-search feature isn't set up yet, not
        // a genuine load failure that users need to act on.
        const msg = err instanceof Error ? err.message : "";
        if (/^(404|500)\b/.test(msg)) {
          setSavedItems([]);
        } else {
          setSavedLoadError(t("expansionAdvisor.errorSavedLoad"));
        }
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
    if (cached) { setReport(cached); return; }
    setLoadingReport(true);
    setReportError(null);
    void trackEvent("ui_expansion_report_opened", { meta: { search_id: targetSearchId } });
    try {
      const result = await getExpansionRecommendationReport(targetSearchId);
      reportCacheRef.current.set(cacheKey, result);
      setReport(result);
    } catch { setReportError(t("expansionAdvisor.errorReport")); } finally { setLoadingReport(false); }
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
      requestAnimationFrame(() => detailRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }));
    }
  };

  const handleSelectCandidateById = async (candidateId?: string | null, forceReloadMemo = false) => {
    const target = resolveCandidateById(candidates, candidateId);
    if (!target) return;
    await handleSelectCandidate(target, forceReloadMemo);
  };

  const handleOpenMemoById = async (candidateId: string) => {
    await handleSelectCandidateById(candidateId);
    setActiveDrawer("memo");
  };

  const onSubmitBrief = async (nextBrief: ExpansionBrief) => {
    const normalized = normalizeBriefPayload(nextBrief);
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
      void loadReport(result.search_id);
      void trackEvent("ui_expansion_search_completed", { meta: { search_id: result.search_id, count: (result.items || []).length } });
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      console.error("[ExpansionAdvisor] search failed:", detail);
      setSearchError(`${t("expansionAdvisor.errorSearch")} — ${detail}`);
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
  const passCount = candidates.filter((c) => c.gate_status_json?.overall_pass).length;
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
  const isFinalStudy = activeSavedId !== null && activeSavedStatus === "final";
  const leadCandidate = useMemo(() => resolveCandidateById(candidates, leadCandidateId), [candidates, leadCandidateId]);

  return (
    <div className="ea-page">
      {/* Story steps header */}
      <div className="ea-story-steps">
        <span className={`ea-story-step ${!hasResults ? "ea-story-step--active" : "ea-story-step--done"}`}>{t("expansionAdvisor.storyStep1")}</span>
        <span className={`ea-story-step ${hasResults && !selectedCandidate ? "ea-story-step--active" : hasResults ? "ea-story-step--done" : ""}`}>{t("expansionAdvisor.storyStep2")}</span>
        <span className={`ea-story-step ${shortlistIds.length > 0 ? "ea-story-step--active" : selectedCandidate ? "ea-story-step--done" : ""}`}>{t("expansionAdvisor.storyStep3")}</span>
        <span className={`ea-story-step ${leadCandidateId ? "ea-story-step--active" : compareResult ? "ea-story-step--done" : ""}`}>{t("expansionAdvisor.storyStep4")}</span>
        <span className={`ea-story-step ${activeSavedId ? "ea-story-step--active" : ""}`}>{t("expansionAdvisor.storyStep5")}</span>
      </div>

      {/* Study header — shown after search completes */}
      {searchId && hasResults && (
        <StudyHeader
          title={title}
          candidateCount={candidates.length}
          shortlistCount={shortlistIds.length}
          bestCandidate={bestCandidate}
          leadCandidate={resolveCandidateById(candidates, leadCandidateId)}
          report={report}
          activeSavedId={activeSavedId}
          searchId={searchId}
          onSaveStudy={() => setActiveDrawer("save")}
          onOpenReport={() => { void loadReport(searchId); setActiveDrawer("report"); }}
          onCompareShortlist={async () => {
            if (shortlistIds.length >= 2) {
              setCompareIds(shortlistIds.slice(0, 6));
              await loadCompare(searchId, shortlistIds.slice(0, 6));
              setActiveDrawer("compare");
            }
          }}
          compareEnabled={compareShortlistEnabled}
          onOpenSavedStudies={() => setShowSavedWorkspace(true)}
        />
      )}

      {/* Decision snapshot — shown when lead candidate exists */}
      {searchId && hasResults && leadCandidate && (
        <DecisionSnapshotCard
          candidate={leadCandidate}
          report={report}
          memo={leadCandidateId && selectedCandidate?.id === leadCandidateId ? memo : null}
          prominent={isFinalStudy}
        />
      )}

      {/* Two-column layout: form + results */}
      <div className={`ea-layout${isFinalStudy ? " ea-layout--final" : ""}`}>
        {/* Left column: brief form/summary + shortlist tray + saved studies */}
        <div style={{ display: "grid", gap: 16, alignContent: "start" }}>
          {/* Brief: edit or summary mode */}
          {briefMode === "edit" || !hasResults ? (
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
          ) : (
            <BriefSummaryRail
              brief={brief}
              onEditBrief={() => setBriefMode("edit")}
              onRunAgain={() => void onSubmitBrief(brief)}
              loading={loadingSearch}
            />
          )}

          {/* Finalists workspace (replaces shortlist tray when shortlisted) */}
          {hasResults && shortlistIds.length > 0 ? (
            <FinalistsWorkspace
              candidates={candidates}
              shortlistIds={shortlistIds}
              leadCandidateId={leadCandidateId}
              selectedCandidateId={selectedCandidate?.id || null}
              onSetLead={(id) => setLeadCandidateId(id)}
              onClearLead={() => setLeadCandidateId(null)}
              onOpenMemo={(id) => void handleOpenMemoById(id)}
              onCompare={async () => {
                if (shortlistIds.length >= 2) {
                  setCompareIds(shortlistIds.slice(0, 6));
                  await loadCompare(searchId, shortlistIds.slice(0, 6));
                  setActiveDrawer("compare");
                }
              }}
              onRemoveShortlist={(id) => {
                setShortlistIds((cur) => cur.filter((sid) => sid !== id));
                if (leadCandidateId === id) setLeadCandidateId(null);
              }}
              onSelectCandidate={(id) => void handleSelectCandidateById(id)}
              compareEnabled={compareShortlistEnabled}
            />
          ) : hasResults ? (
            <ShortlistTray
              candidates={candidates}
              shortlistIds={shortlistIds}
              compareIds={compareIds}
              selectedCandidateId={selectedCandidate?.id || null}
              onSelectCandidate={(id) => void handleSelectCandidateById(id)}
              onRemoveShortlist={(id) => setShortlistIds((cur) => cur.filter((sid) => sid !== id))}
              onOpenMemo={(id) => void handleOpenMemoById(id)}
              onCompare={async () => {
                if (shortlistIds.length >= 2) {
                  setCompareIds(shortlistIds.slice(0, 6));
                  await loadCompare(searchId, shortlistIds.slice(0, 6));
                  setActiveDrawer("compare");
                }
              }}
              compareEnabled={compareShortlistEnabled}
            />
          ) : null}

          <div className="ea-card">
            <div className="ea-card__header">
              <h3 className="ea-card__title">{t("expansionAdvisor.expansionStudies")}</h3>
              {savedItems.length > 0 && (
                <button className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={() => setShowSavedWorkspace(true)}>
                  {t("expansionAdvisor.openSavedStudies")}
                </button>
              )}
            </div>
            <div className="ea-card__body">
              {savedLoadError && (
                <div className="ea-state ea-state--error">
                  {savedLoadError}
                  <button
                    className="oak-btn oak-btn--xs oak-btn--tertiary"
                    style={{ marginTop: 8 }}
                    onClick={() => {
                      setLoadingSaved(true);
                      setSavedLoadError(null);
                      listSavedExpansionSearches()
                        .then((res) => {
                          setSavedItems(res.items || []);
                          setSavedLoadError(null);
                        })
                        .catch((err) => {
                          const msg = err instanceof Error ? err.message : "";
                          if (/^(404|500)\b/.test(msg)) {
                            setSavedItems([]);
                          } else {
                            setSavedLoadError(t("expansionAdvisor.errorSavedLoad"));
                          }
                        })
                        .finally(() => setLoadingSaved(false));
                    }}
                  >
                    {t("expansionAdvisor.retry")}
                  </button>
                </div>
              )}
              {!savedLoadError && (
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
              )}
            </div>
          </div>
        </div>

        {/* Right column: results + detail */}
        <div style={{ display: "grid", gap: 16, alignContent: "start" }}>
          {/* Summary strip */}
          {hasResults && (
            <div className="ea-summary-strip">
              <div className="ea-summary-strip__item">
                <span className="ea-summary-strip__label">{t("expansionAdvisor.branchCandidates")}:</span>
                <span className="ea-summary-strip__value">{candidates.length}</span>
              </div>
              <div className="ea-summary-strip__item">
                <span className="ea-summary-strip__label">{t("expansionAdvisor.passGates")}:</span>
                <span className="ea-summary-strip__value">{passCount}</span>
              </div>
              {bestCandidate && (
                <div className="ea-summary-strip__item">
                  <span className="ea-summary-strip__label">{t("expansionAdvisor.bestScore")}:</span>
                  <span className="ea-summary-strip__value">{bestCandidate.final_score ?? "—"}</span>
                </div>
              )}
              <div className="ea-summary-strip__item">
                <span className="ea-summary-strip__label">{t("expansionAdvisor.topDistrict")}:</span>
                <span className="ea-summary-strip__value">{candidates.find((c) => c.district)?.district || "—"}</span>
              </div>
              {searchMeta.parcel_source ? (
                <div className="ea-summary-strip__item">
                  <span className="ea-summary-strip__label">{t("expansionAdvisor.parcelSource")}:</span>
                  <span className="ea-summary-strip__value">{String(searchMeta.parcel_source)}</span>
                </div>
              ) : null}
            </div>
          )}

          {/* Next steps strip - shown when lead candidate is set */}
          {hasResults && leadCandidateId && (
            <NextStepsStrip
              candidates={candidates}
              shortlistIds={shortlistIds}
              leadCandidateId={leadCandidateId}
              report={report}
              onOpenMemo={(id) => void handleOpenMemoById(id)}
              onOpenReport={() => { void loadReport(searchId); setActiveDrawer("report"); }}
              onCompare={async () => {
                if (shortlistIds.length >= 2) {
                  setCompareIds(shortlistIds.slice(0, 6));
                  await loadCompare(searchId, shortlistIds.slice(0, 6));
                  setActiveDrawer("compare");
                }
              }}
            />
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

          {/* Actions bar */}
          {searchId && (
            <div className="ea-actions-bar">
              <button
                className={`oak-btn oak-btn--sm ${compareIds.length >= 2 && compareIds.length <= 6 ? "oak-btn--primary" : "oak-btn--tertiary"}`}
                disabled={compareIds.length < 2 || compareIds.length > 6 || loadingCompare}
                onClick={async () => {
                  await loadCompare(searchId, compareIds);
                  setActiveDrawer("compare");
                }}
              >
                {compareIds.length >= 2 && compareIds.length <= 6
                  ? t("expansionAdvisor.compareSelected", { count: compareIds.length })
                  : t("expansionAdvisor.compareNeedTwo")}
              </button>
              <button className="oak-btn oak-btn--sm oak-btn--tertiary" onClick={() => { void loadReport(searchId); setActiveDrawer("report"); }}>
                {t("expansionAdvisor.openExecutiveReport")}
              </button>
              <button className="oak-btn oak-btn--sm oak-btn--tertiary" onClick={() => setActiveDrawer("save")} disabled={!searchId}>
                {activeSavedId ? t("expansionAdvisor.updateStudy") : t("expansionAdvisor.saveStudy")}
              </button>
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
              onToggleShortlist={(candidateId) => setShortlistIds((cur) => cur.includes(candidateId) ? cur.filter((id) => id !== candidateId) : [...cur, candidateId])}
              onToggleCompare={(candidateId) => setCompareIds((cur) => getNextCompareIds(cur, candidateId))}
              onOpenMemo={(candidateId) => void handleOpenMemoById(candidateId)}
              onShowOnMap={(candidate) => {
                onSelectedCandidateChange(candidate);
                setSelectedCandidate(candidate);
              }}
            />
          ) : loadingSearch ? (
            <CandidateListSkeleton count={5} />
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

          {/* Selected candidate detail */}
          <div ref={detailRef} />
          {memoError && <div className="ea-state ea-state--error">{memoError}</div>}
          {loadingMemo && <DetailSkeleton />}
          {selectedCandidate && !loadingMemo && (
            <div className="ea-card">
              <div className="ea-card__header">
                <h3 className="ea-card__title">
                  {selectedCandidate.id === leadCandidateId && <span className="ea-lead-tag">{t("expansionAdvisor.leadSite")}</span>}
                  #{selectedCandidate.rank_position} {selectedCandidate.district || selectedCandidate.parcel_id}
                </h3>
                <div style={{ display: "flex", gap: 6 }}>
                  {selectedCandidate.id !== leadCandidateId && shortlistIds.includes(selectedCandidate.id) && (
                    <button className="oak-btn oak-btn--sm oak-btn--tertiary" onClick={() => setLeadCandidateId(selectedCandidate.id)}>
                      {t("expansionAdvisor.setAsLead")}
                    </button>
                  )}
                  <button className="oak-btn oak-btn--sm oak-btn--primary" onClick={() => setActiveDrawer("memo")}>
                    {t("expansionAdvisor.viewDecisionMemo")}
                  </button>
                </div>
              </div>
              <div className="ea-card__body">
                <CandidateDetailPanel candidate={selectedCandidate} />
                <DecisionChecklist candidate={selectedCandidate} memo={memo} />
                {selectedCandidate.id === leadCandidateId && (
                  <>
                    <ValidationPlanPanel candidate={selectedCandidate} memo={memo} report={report} />
                    <AssumptionsCard candidate={selectedCandidate} report={report} />
                  </>
                )}
                {selectedCandidate.id !== leadCandidateId && (
                  <AssumptionsCard candidate={selectedCandidate} report={report} compact />
                )}
              </div>
            </div>
          )}

          {/* Copy summary block when lead candidate has a memo */}
          {leadCandidateId && selectedCandidate?.id === leadCandidateId && (memo || report) && (
            <CopySummaryBlock
              candidate={selectedCandidate}
              report={report}
              memo={memo}
            />
          )}
        </div>
      </div>

      {/* ─── Drawers / dialogs ─── */}

      {activeDrawer === "memo" && (
        <ExpansionMemoPanel
          memo={memo}
          loading={loadingMemo}
          isLeadCandidate={selectedCandidate?.id === leadCandidateId}
          report={report}
          onClose={() => setActiveDrawer("none")}
          onBackToDetail={() => setActiveDrawer("none")}
          onBackToCompare={compareResult ? () => setActiveDrawer("compare") : undefined}
          onOpenCompare={compareShortlistEnabled ? async () => {
            setCompareIds(shortlistIds.slice(0, 6));
            await loadCompare(searchId, shortlistIds.slice(0, 6));
            setActiveDrawer("compare");
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
