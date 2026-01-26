import { useEffect, useMemo, useRef, useState } from "react";
import type { Geometry } from "geojson";
import { useTranslation } from "react-i18next";

import { downloadMemoPdf, landPrice, makeEstimate, runScenario, trackEvent } from "../api";
import {
  cloneTemplate,
  ExcelInputs,
  LandUseCode,
  templateForLandUse,
} from "../lib/excelTemplates";
import ParkingSummary from "./ParkingSummary";
import type { EstimateNotes, EstimateTotals } from "../lib/types";
import { formatAreaM2, formatCurrencySAR, formatNumber, formatPercent } from "../i18n/format";
import { scaleAboveGroundAreaRatio } from "../utils/areaRatio";
import { applyPatch } from "../utils/applyPatch";
import MicroFeedbackPrompt from "./MicroFeedbackPrompt";
import ScenarioModal from "./ScenarioModal";

const PROVIDERS = [
  {
    value: "blended_v1" as const,
    labelKey: "excel.providers.blended_v1",
  },
  {
    value: "suhail" as const,
    labelKey: "excel.providers.suhail",
  },
  {
    value: "kaggle_hedonic_v0" as const,
    labelKey: "excel.providers.kaggle_hedonic_v0",
  },
];

type Centroid = [number, number];

type ExcelResult = {
  roi: number;
  costs: {
    land_cost: number;
    construction_direct_cost: number;
    fitout_cost: number;
    contingency_cost: number;
    consultants_cost: number;
    feasibility_fee: number;
    transaction_cost: number;
    grand_total_capex: number;
    y1_income: number;
    y1_income_effective?: number;
    y1_income_effective_factor?: number;
    opex_pct?: number;
    opex_cost?: number;
    y1_noi?: number;
  };
  breakdown: Record<string, any>;
  inputs: any;
  siteArea?: number;
  landPrice?: { ppm2?: number; source_type?: string };
  summary: string;
  excelRent?: {
    rent_sar_m2_yr?: Record<string, number>;
    rent_source_metadata?: Record<string, any>;
  };
  totals?: EstimateTotals;
  notes?: EstimateNotes;
};

function polygonCentroidAndArea(coords: number[][][]): { area: number; centroid: Centroid } | null {
  if (!coords?.length) return null;
  const ring = coords[0];
  if (!ring || ring.length < 3) return null;
  let crossSum = 0;
  let cxSum = 0;
  let cySum = 0;
  const len = ring.length;
  for (let i = 0; i < len; i += 1) {
    const [x0, y0] = ring[i];
    const [x1, y1] = ring[(i + 1) % len];
    const cross = x0 * y1 - x1 * y0;
    crossSum += cross;
    cxSum += (x0 + x1) * cross;
    cySum += (y0 + y1) * cross;
  }
  if (!crossSum) return null;
  const centroid: Centroid = [cxSum / (3 * crossSum), cySum / (3 * crossSum)];
  return { area: Math.abs(crossSum) / 2, centroid };
}

function centroidFromGeometry(geometry?: Geometry | null): Centroid | null {
  if (!geometry) return null;
  if (geometry.type === "Point") {
    return geometry.coordinates as Centroid;
  }
  if (geometry.type === "Polygon") {
    return polygonCentroidAndArea(geometry.coordinates as number[][][])?.centroid || null;
  }
  if (geometry.type === "MultiPolygon") {
    const coords = geometry.coordinates as number[][][][];
    let totalArea = 0;
    let cx = 0;
    let cy = 0;
    for (const poly of coords) {
      const details = polygonCentroidAndArea(poly);
      if (!details) continue;
      totalArea += details.area;
      cx += details.centroid[0] * details.area;
      cy += details.centroid[1] * details.area;
    }
    if (!totalArea) return null;
    return [cx / totalArea, cy / totalArea];
  }
  return null;
}

type ExcelFormProps = {
  parcel: any;
  landUseOverride?: string;
};

const normalizeLandUse = (value?: string | null): LandUseCode | null => {
  const v = (value || "").trim().toLowerCase();
  return v === "m" ? "m" : v === "s" ? "s" : null;
};

const normalizeEffectivePct = (value?: number | null) => {
  if (value == null || Number.isNaN(value)) return 90;
  return Math.max(0, Math.min(value, 100));
};

export default function ExcelForm({ parcel, landUseOverride }: ExcelFormProps) {
  const { t, i18n } = useTranslation();
  const [provider, setProvider] = useState<(typeof PROVIDERS)[number]["value"]>("blended_v1");
  const [price, setPrice] = useState<number | null>(null);
  const [suggestedPrice, setSuggestedPrice] = useState<number | null>(null);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [includeFitout, setIncludeFitout] = useState(true);
  const [includeContingency, setIncludeContingency] = useState(true);
  const [includeFeasibility, setIncludeFeasibility] = useState(true);
  const [includeOpex, setIncludeOpex] = useState(true);
  const normalizedParcelLandUse = normalizeLandUse(parcel?.landuse_code);
  const normalizedPropLandUse = normalizeLandUse(landUseOverride);
  const initialLandUse = normalizedPropLandUse ?? normalizedParcelLandUse ?? "s";
  const notAvailable = t("common.notAvailable");
  const providerLabel = t(`excel.providers.${provider}`);
  const isArabic = i18n.language?.toLowerCase().startsWith("ar");
  const scenarioProviders = PROVIDERS.map((item) => ({
    value: item.value,
    label: t(item.labelKey),
  }));

  const formatNumberValue = (value: number | string | null | undefined, digits = 0) =>
    formatNumber(value, { maximumFractionDigits: digits, minimumFractionDigits: digits }, notAvailable);

  const formatPercentValue = (value?: number | null, digits = 1) =>
    formatPercent(value ?? null, { maximumFractionDigits: digits, minimumFractionDigits: digits }, notAvailable);

  const applyInputPatch = (patch: Partial<ExcelInputs>, shouldRunEstimate = false) => {
    setOverrides((prev) => {
      const nextOverrides = applyPatch(prev, patch);
      if (shouldRunEstimate) {
        const nextInputs = applyPatch(baseInputsRef.current, nextOverrides);
        runEstimate(nextInputs);
      }
      return nextOverrides;
    });
  };

  const updateUnitCost = (key: string, value: string) => {
    const nextValue = value === "" ? 0 : Number(value);
    applyInputPatch({
      unit_cost: {
        [key]: nextValue,
      },
    });
  };

  // User override from dropdown; null means "use inferred"
  const [overrideLandUse, setOverrideLandUse] = useState<LandUseCode | null>(normalizedPropLandUse);
  const effectiveLandUse: LandUseCode = overrideLandUse ?? normalizedParcelLandUse ?? "s";
  const parcelIdentityRef = useRef<string | null>(null);
  const [estimateId, setEstimateId] = useState<string | null>(null);
  const [isFeedbackOpen, setIsFeedbackOpen] = useState(false);
  const [feedbackContext, setFeedbackContext] = useState<"estimate" | "pdf">("estimate");
  const feedbackSentinelRef = useRef<HTMLDivElement | null>(null);
  const feedbackEligibleRef = useRef(false);
  const [hasUserScrolled, setHasUserScrolled] = useState(false);
  const [estimateCompletedAt, setEstimateCompletedAt] = useState<number | null>(null);
  const [isFeedbackTimeReady, setIsFeedbackTimeReady] = useState(false);
  const feedbackDismissCooldownMs = 24 * 60 * 60 * 1000;

  // Excel inputs state (drives payload). Seed from template.
  const [baseInputs, setBaseInputs] = useState<ExcelInputs>(() =>
    cloneTemplate(templateForLandUse(initialLandUse)),
  );
  const [overrides, setOverrides] = useState<Partial<ExcelInputs>>({});
  const baseInputsRef = useRef(baseInputs);
  useEffect(() => {
    baseInputsRef.current = baseInputs;
  }, [baseInputs]);
  const inputs = useMemo(() => applyPatch(baseInputs, overrides), [baseInputs, overrides]);
  const inputsRef = useRef(inputs);
  useEffect(() => {
    inputsRef.current = inputs;
  }, [inputs]);
  const [error, setError] = useState<string | null>(null);
  const [excelResult, setExcelResult] = useState<ExcelResult | null>(null);
  const [effectiveIncomePctDraft, setEffectiveIncomePctDraft] = useState<string>(() =>
    String(normalizeEffectivePct(cloneTemplate(templateForLandUse(initialLandUse)).y1_income_effective_pct)),
  );
  const [isEditingFar, setIsEditingFar] = useState(false);
  const [farDraft, setFarDraft] = useState<string>("");
  const [farEditError, setFarEditError] = useState<string | null>(null);
  const [isScenarioOpen, setIsScenarioOpen] = useState(false);
  const [isScenarioSubmitting, setIsScenarioSubmitting] = useState(false);
  const [scenarioBaseResult, setScenarioBaseResult] = useState<ExcelResult | null>(null);
  const [isScenarioActive, setIsScenarioActive] = useState(false);
  const excelResultRef = useRef<ExcelResult | null>(null);
  const unitCostInputs = inputs.unit_cost || {};

  useEffect(() => {
    const geometrySignature = parcel?.geometry ? JSON.stringify(parcel.geometry) : "";
    const parcelKey = `${parcel?.parcel_id || ""}::${geometrySignature}`;
    if (parcelIdentityRef.current !== parcelKey) {
      parcelIdentityRef.current = parcelKey;
      setOverrideLandUse(null);
      setEstimateId(null);
      setScenarioBaseResult(null);
      setIsScenarioActive(false);
    }
  }, [parcel]);

  const getFeedbackKey = (id: string) => `feedback_given_${id}`;
  const getFeedbackDismissedKey = (id: string) => `feedback_dismissed_${id}`;
  const hasFeedbackKey = (id: string) => {
    if (typeof window === "undefined") return false;
    return window.localStorage.getItem(getFeedbackKey(id)) === "true";
  };
  const hasRecentFeedbackDismissal = (id: string) => {
    if (typeof window === "undefined") return false;
    const dismissedAt = Number(window.localStorage.getItem(getFeedbackDismissedKey(id)));
    if (!Number.isFinite(dismissedAt)) return false;
    return Date.now() - dismissedAt < feedbackDismissCooldownMs;
  };
  const markFeedbackKey = (id: string) => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(getFeedbackKey(id), "true");
  };
  const markFeedbackDismissed = (id: string) => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(getFeedbackDismissedKey(id), String(Date.now()));
  };
  const openFeedback = (context: "estimate" | "pdf", id?: string | null) => {
    if (!id || hasFeedbackKey(id) || hasRecentFeedbackDismissal(id) || isFeedbackOpen) return;
    setFeedbackContext(context);
    setIsFeedbackOpen(true);
    void trackEvent("ui_feedback_shown", {
      estimateId: id ?? undefined,
      meta: { context },
    });
  };
  const handleFeedbackDismiss = () => {
    if (estimateId) {
      markFeedbackDismissed(estimateId);
    }
    setIsFeedbackOpen(false);
  };
  const handleFeedbackSubmit = () => {
    if (estimateId) {
      markFeedbackKey(estimateId);
    }
    setIsFeedbackOpen(false);
  };

  useEffect(() => {
    setOverrideLandUse(normalizedPropLandUse);
  }, [normalizedPropLandUse]);

  useEffect(() => {
    setBaseInputs(cloneTemplate(templateForLandUse(effectiveLandUse)));
  }, [effectiveLandUse]);

  useEffect(() => {
    const template = templateForLandUse(effectiveLandUse);
    setOverrides((prev) => {
      const nextPatch: Partial<ExcelInputs> = {};
      const currentLandPrice = Number(inputsRef.current?.land_price_sar_m2 ?? 0);
      if (currentLandPrice > 0) {
        nextPatch.land_price_sar_m2 = currentLandPrice;
      }
      nextPatch.fitout_rate = includeFitout ? template.fitout_rate : 0;
      nextPatch.contingency_pct = includeContingency ? template.contingency_pct : 0;
      nextPatch.feasibility_fee_pct = includeFeasibility ? template.feasibility_fee_pct : 0;
      nextPatch.opex_pct = includeOpex ? template.opex_pct : 0;
      return applyPatch(prev, nextPatch);
    });
  }, [effectiveLandUse, includeFitout, includeContingency, includeFeasibility, includeOpex]);

  useEffect(() => {
    const normalized = normalizeEffectivePct(inputs?.y1_income_effective_pct as number | undefined);
    setEffectiveIncomePctDraft(String(normalized));
  }, [inputs?.y1_income_effective_pct]);

  useEffect(() => {
    if (!estimateId) return;
    feedbackEligibleRef.current = false;
  }, [estimateId]);

  useEffect(() => {
    if (!estimateCompletedAt) return;
    setIsFeedbackTimeReady(false);
    const timeoutId = window.setTimeout(() => setIsFeedbackTimeReady(true), 8000);
    return () => window.clearTimeout(timeoutId);
  }, [estimateCompletedAt]);

  useEffect(() => {
    excelResultRef.current = excelResult;
    if (!excelResult) return;
    const handleScroll = () => setHasUserScrolled(true);
    const windowScrollOptions: AddEventListenerOptions = { passive: true, once: true };
    const documentScrollOptions: AddEventListenerOptions = { passive: true, once: true, capture: true };
    window.addEventListener("scroll", handleScroll, windowScrollOptions);
    document.addEventListener("scroll", handleScroll, documentScrollOptions);
    return () => {
      window.removeEventListener("scroll", handleScroll);
      document.removeEventListener("scroll", handleScroll, true);
    };
  }, [excelResult]);

  useEffect(() => {
    if (
      !excelResult ||
      !estimateId ||
      isFeedbackOpen ||
      hasFeedbackKey(estimateId) ||
      hasRecentFeedbackDismissal(estimateId)
    ) {
      return;
    }
    const sentinel = feedbackSentinelRef.current;
    if (!sentinel) return;
    const canShowFeedback = hasUserScrolled || isFeedbackTimeReady;
    let dwellTimeout: number | null = null;

    const observer = new IntersectionObserver(
      (entries) => {
        const entry = entries[0];
        const isVisible = !!entry && entry.isIntersecting && entry.intersectionRatio >= 0.6;
        if (isVisible && canShowFeedback) {
          if (!feedbackEligibleRef.current) {
            feedbackEligibleRef.current = true;
            void trackEvent("ui_feedback_eligible", {
              estimateId,
              meta: { context: "estimate" },
            });
          }
          if (dwellTimeout != null) return;
          dwellTimeout = window.setTimeout(() => {
            openFeedback("estimate", estimateId);
            dwellTimeout = null;
          }, 1000);
          return;
        }

        if (dwellTimeout != null) {
          window.clearTimeout(dwellTimeout);
          dwellTimeout = null;
        }
      },
      { threshold: [0, 0.6, 1] },
    );

    observer.observe(sentinel);

    return () => {
      if (dwellTimeout != null) {
        window.clearTimeout(dwellTimeout);
      }
      observer.disconnect();
    };
  }, [excelResult, estimateId, hasUserScrolled, isFeedbackOpen, isFeedbackTimeReady]);

  const handleFitoutToggle = (checked: boolean) => {
    setIncludeFitout(checked);
    applyInputPatch(
      {
        fitout_rate: checked ? templateForLandUse(effectiveLandUse).fitout_rate : 0,
      },
      Boolean(excelResult),
    );
  };

  const handleContingencyToggle = (checked: boolean) => {
    setIncludeContingency(checked);
    applyInputPatch(
      {
        contingency_pct: checked ? templateForLandUse(effectiveLandUse).contingency_pct : 0,
      },
      Boolean(excelResult),
    );
  };

  const handleFeasibilityToggle = (checked: boolean) => {
    setIncludeFeasibility(checked);
    applyInputPatch(
      {
        feasibility_fee_pct: checked ? templateForLandUse(effectiveLandUse).feasibility_fee_pct : 0,
      },
      Boolean(excelResult),
    );
  };

  const handleOpexToggle = (checked: boolean) => {
    setIncludeOpex(checked);
    applyInputPatch(
      {
        opex_pct: checked ? templateForLandUse(effectiveLandUse).opex_pct : 0,
      },
      Boolean(excelResult),
    );
  };

  const resolveEffectivePctFromDraft = (draft: string) => {
    const parsed = Number(draft);
    if (!Number.isFinite(parsed)) return 90;
    return normalizeEffectivePct(parsed);
  };

  const commitEffectiveIncomePct = (draftOverride?: string) => {
    const pct = resolveEffectivePctFromDraft(draftOverride ?? effectiveIncomePctDraft);
    const currentPct = normalizeEffectivePct(inputsRef.current?.y1_income_effective_pct as number | undefined);

    if (pct === currentPct) {
      setEffectiveIncomePctDraft(String(pct));
      return;
    }

    applyInputPatch({ y1_income_effective_pct: pct }, Boolean(excelResult));
    setEffectiveIncomePctDraft(String(pct));
  };

  const assetProgram =
    effectiveLandUse === "m" ? "mixed_use_midrise" : "residential_midrise";

  async function fetchPrice() {
    setError(null);
    setFetchError(null);
    setPrice(null);
    const centroid = centroidFromGeometry(parcel?.geometry as Geometry | null);
    try {
      const res = await landPrice(
        "Riyadh",
        parcel?.district || undefined,
        provider,
        parcel?.parcel_id || undefined,
        centroid?.[0],
        centroid?.[1],
      );
      const ppm2 = res.value_sar_m2 ?? res.sar_per_m2 ?? res.value;
      if (ppm2 == null) {
        throw new Error(t("excel.noPriceError"));
      }
      setPrice(ppm2);
      setSuggestedPrice(ppm2);
      applyInputPatch({ land_price_sar_m2: ppm2 });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setFetchError(message);
    }
  }

  async function runEstimate(currentInputs: ExcelInputs = inputs) {
    if (!parcel) return;
    setError(null);
    setExcelResult(null);
    setEstimateId(null);
    setScenarioBaseResult(null);
    setIsScenarioActive(false);
    try {
      const excelInputs = { ...currentInputs, land_use_code: effectiveLandUse };
      const result = await makeEstimate({
        geometry: parcel.geometry,
        excelInputs,
        assetProgram,
        strategy: "build_to_sell",
        city: "Riyadh",
        far: 2.0,
        efficiency: 0.82,
        landUseOverride: overrideLandUse ?? undefined,
      });
      const notes = result?.notes || {};
      const costs = notes.cost_breakdown || {};
      const excelBreakdown = notes.excel_breakdown || {};
      const effectivePctInput = normalizeEffectivePct(
        (currentInputs?.y1_income_effective_pct ?? inputs?.y1_income_effective_pct) as number | undefined,
      );
      const effectiveFactorFromInput = effectivePctInput / 100;
      const y1IncomeEffective =
        costs.y1_income_effective ??
        excelBreakdown.y1_income_effective ??
        (costs.y1_income ?? excelBreakdown.y1_income ?? 0) * effectiveFactorFromInput;
      const y1IncomeEffectiveFactor =
        costs.y1_income_effective_factor ?? excelBreakdown.y1_income_effective_factor ?? effectiveFactorFromInput;

      setExcelResult({
        roi: notes.excel_roi ?? result?.totals?.excel_roi ?? 0,
        costs: {
          land_cost: costs.land_cost ?? 0,
          construction_direct_cost: costs.construction_direct_cost ?? 0,
          fitout_cost: costs.fitout_cost ?? 0,
          contingency_cost: costs.contingency_cost ?? 0,
          consultants_cost: costs.consultants_cost ?? 0,
          feasibility_fee: costs.feasibility_fee ?? 0,
          transaction_cost: costs.transaction_cost ?? 0,
          grand_total_capex: costs.grand_total_capex ?? 0,
          y1_income: costs.y1_income ?? 0,
          y1_income_effective: y1IncomeEffective,
          y1_income_effective_factor: y1IncomeEffectiveFactor,
        },
        breakdown: excelBreakdown,
        inputs: excelInputs,
        siteArea: notes.site_area_m2,
        landPrice: notes.excel_land_price,
        summary: notes.summary ?? "",
        excelRent: notes.excel_rent,
        totals: result?.totals,
        notes: result?.notes,
      });
      const nextEstimateId = result?.id ?? null;
      setEstimateId(nextEstimateId);
      setHasUserScrolled(false);
      setEstimateCompletedAt(Date.now());
      void trackEvent("ui_estimate_completed", {
        estimateId: nextEstimateId ?? undefined,
        meta: {
          roi: notes.excel_roi ?? result?.totals?.excel_roi ?? null,
          far_effective: excelBreakdown.far_above_ground ?? null,
          land_price_sar_m2: excelInputs.land_price_sar_m2 ?? null,
        },
      });
      // Preserve override selection on calculate; reset only when parcel identity changes.
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      const statusMatch = typeof message === "string" ? message.match(/^(\d{3})\s/) : null;
      const status = statusMatch ? Number(statusMatch[1]) : undefined;
      void trackEvent("ui_estimate_failed", {
        meta: { message, status },
      });
    }
  }

  const handleExportPdf = async () => {
    if (!estimateId) return;
    try {
      const blob = await downloadMemoPdf(estimateId);
      const blobUrl = URL.createObjectURL(blob);
      window.open(blobUrl, "_blank", "noopener,noreferrer");
      setTimeout(() => URL.revokeObjectURL(blobUrl), 60000);
      void trackEvent("ui_pdf_opened", {
        estimateId,
      });
      openFeedback("pdf", estimateId);
    } catch (err) {
      setError("Unable to export PDF. Please try again.");
    }
  };

  const handleEstimateClick = () => {
    void trackEvent("ui_estimate_started", {
      meta: {
        parcel_id: parcel?.parcel_id ?? null,
        landuse_code: parcel?.landuse_code ?? null,
        provider,
      },
    });
    void runEstimate();
  };

  const handleScenarioSubmit = async (patch: Record<string, unknown>, meta: Record<string, unknown>) => {
    if (!estimateId) return;
    setError(null);
    setIsScenarioSubmitting(true);
    try {
      const res = await runScenario(estimateId, patch);
      const scenarioTotals = res?.totals ?? res?.scenario;
      const rawNotes = res?.notes;
      const notes = rawNotes && typeof rawNotes === "object" ? rawNotes : null;
      const costs = notes?.cost_breakdown || null;
      const excelBreakdown = notes?.excel_breakdown || null;
      const fallbackRoi = res?.totals ? null : notes?.cost_breakdown?.roi ?? null;
      const effectivePctInput = normalizeEffectivePct(inputsRef.current?.y1_income_effective_pct);
      const effectiveFactorFromInput = effectivePctInput / 100;
      const y1IncomeEffective = costs
        ? costs.y1_income_effective ??
          excelBreakdown?.y1_income_effective ??
          (costs.y1_income ?? excelBreakdown?.y1_income ?? 0) * effectiveFactorFromInput
        : null;
      const y1IncomeEffectiveFactor = costs
        ? costs.y1_income_effective_factor ??
          excelBreakdown?.y1_income_effective_factor ??
          effectiveFactorFromInput
        : null;
      if (!scenarioBaseResult && excelResultRef.current) {
        setScenarioBaseResult(excelResultRef.current);
      }
      setExcelResult((prev) => {
        if (!prev) return prev;
        return {
          ...prev,
          totals: scenarioTotals ?? prev.totals,
          notes: notes ?? prev.notes,
          roi: notes?.excel_roi ?? scenarioTotals?.excel_roi ?? fallbackRoi ?? prev.roi,
          costs: costs
            ? {
              ...prev.costs,
              land_cost: costs.land_cost ?? prev.costs.land_cost,
              construction_direct_cost: costs.construction_direct_cost ?? prev.costs.construction_direct_cost,
              fitout_cost: costs.fitout_cost ?? prev.costs.fitout_cost,
              contingency_cost: costs.contingency_cost ?? prev.costs.contingency_cost,
              consultants_cost: costs.consultants_cost ?? prev.costs.consultants_cost,
              feasibility_fee: costs.feasibility_fee ?? prev.costs.feasibility_fee,
              transaction_cost: costs.transaction_cost ?? prev.costs.transaction_cost,
              grand_total_capex: costs.grand_total_capex ?? prev.costs.grand_total_capex,
              y1_income: costs.y1_income ?? prev.costs.y1_income,
              y1_income_effective: y1IncomeEffective ?? prev.costs.y1_income_effective,
              y1_income_effective_factor: y1IncomeEffectiveFactor ?? prev.costs.y1_income_effective_factor,
            }
            : prev.costs,
          breakdown: excelBreakdown ?? prev.breakdown,
        };
      });
      setIsScenarioActive(true);
      setIsScenarioOpen(false);
      void trackEvent("ui_scenario_run", {
        estimateId,
        meta,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setIsScenarioSubmitting(false);
    }
  };

  const copyEstimateId = async () => {
    if (!estimateId || !navigator?.clipboard?.writeText) return;
    try {
      await navigator.clipboard.writeText(estimateId);
    } catch (err) {
      // no-op; clipboard may be blocked
    }
  };

  const breakdown = excelResult?.breakdown || {};
  const notes = excelResult?.notes || {};
  const scenario =
    notes?.scenario_overrides ?? notes?.notes?.scenario_overrides ?? null;
  const builtArea = breakdown.built_area || {};
  const farAboveGround = breakdown.far_above_ground;
  const scenarioAreaRatio =
    scenario && typeof scenario.area_ratio === "number" ? scenario.area_ratio : null;
  const scenarioFar = scenario && typeof scenario.far === "number" ? scenario.far : null;
  const scenarioLandPrice =
    scenario && typeof scenario.land_price_sar_m2 === "number" ? scenario.land_price_sar_m2 : null;
  const displayedFar =
    scenarioAreaRatio != null
      ? scenarioFar ?? (farAboveGround != null ? farAboveGround * scenarioAreaRatio : null)
      : farAboveGround;
  const displayedBuiltArea =
    scenarioAreaRatio != null
      ? {
        ...builtArea,
        residential: (builtArea.residential ?? 0) * scenarioAreaRatio,
        retail: (builtArea.retail ?? 0) * scenarioAreaRatio,
        office: (builtArea.office ?? 0) * scenarioAreaRatio,
        basement: builtArea.basement,
      }
      : builtArea;
  const nla = breakdown.nla || {};
  const directCost = breakdown.direct_cost || {};
  const incomeComponents = breakdown.y1_income_components || {};
  const explanations =
    (isArabic
      ? breakdown.explanations_ar ?? breakdown.explanations_en ?? breakdown.explanations
      : breakdown.explanations_en ?? breakdown.explanations) || {};
  const farNoteBase = (() => {
    const note = explanations.effective_far_above_ground;
    if (typeof note !== "string") return note;
    const disallowedFragments = ["Above-ground FAR adjusted"];
    const filtered = note
      .split("|")
      .map((part) => part.trim())
      .filter((part) => !disallowedFragments.some((fragment) => part.startsWith(fragment)));
    return filtered.join(" | ");
  })();
  const usedInputs = excelResult?.inputs || {};
  const unitCost = usedInputs.unit_cost || {};
  const rentRates = usedInputs.rent_sar_m2_yr || {};
  const efficiency = usedInputs.efficiency || {};
  const areaRatio = usedInputs.area_ratio || {};
  const excelRent = excelResult?.excelRent;
  const rentMeta = excelRent?.rent_source_metadata as any;
  const rentRatesFromNotes = excelRent?.rent_sar_m2_yr as Record<string, number> | undefined;
  const appliedRentRates =
    rentRatesFromNotes && typeof rentRatesFromNotes === "object" ? rentRatesFromNotes : rentRates;

  let residentialRentYr: number | null = null;
  if (rentRatesFromNotes && typeof rentRatesFromNotes === "object") {
    residentialRentYr =
      (rentRatesFromNotes as any).residential ??
      ((Object.values(rentRatesFromNotes)[0] as number | undefined) ?? null);
  }

  const residentialRentMo =
    residentialRentYr != null ? residentialRentYr / 12 : null;
  const contingencyPct = usedInputs.contingency_pct ?? null;
  const consultantsPct = usedInputs.consultants_pct ?? null;
  const breakdownFeasibilityPct =
    typeof breakdown?.feasibility_fee_pct === "number" ? breakdown.feasibility_fee_pct : null;
  const feasibilityPct = breakdownFeasibilityPct ?? 0.02;
  const transactionPct = usedInputs.transaction_pct ?? null;
  const fitoutRate = usedInputs.fitout_rate ?? null;
  const siteArea = excelResult?.siteArea ?? null;
  const landCost = excelResult?.costs?.land_cost ?? null;
  const landCostValue = excelResult?.costs?.land_cost ?? 0;
  const landPricePpm2 =
    excelResult?.landPrice?.ppm2 ??
    (siteArea && siteArea > 0 && landCost != null && landCost > 0
      ? landCost / siteArea
      : null);
  const fitoutEntries = Object.entries(builtArea ?? {}) as Array<[string, number | string]>;
  const fitoutArea = fitoutEntries.reduce(
    (acc: number, [key, value]) => {
      const numericValue = typeof value === "number" ? value : Number(value) || 0;
      return key.toLowerCase().startsWith("basement") ? acc : acc + numericValue;
    },
    0,
  );
  const constructionSubtotal = typeof breakdown.sub_total === "number" ? breakdown.sub_total : 0;
  const contingencyAmount = typeof breakdown.contingency_cost === "number" ? breakdown.contingency_cost : 0;
  const consultantsBase = constructionSubtotal + contingencyAmount;
  const constructionDirectValues = Object.values(directCost ?? {}) as Array<number | string>;
  const constructionDirectTotal = constructionDirectValues.reduce((acc: number, value) => {
    const numericValue = typeof value === "number" ? value : Number(value) || 0;
    return acc + numericValue;
  }, 0);
  const fitoutTotalFromBreakdown = typeof breakdown.fitout_cost === "number" ? breakdown.fitout_cost : null;
  const fitoutTotal =
    fitoutTotalFromBreakdown ?? (typeof excelResult?.costs?.fitout_cost === "number" ? excelResult.costs.fitout_cost : 0);
  const fitoutExcluded = !includeFitout;
  const contingencyExcluded = !includeContingency;
  const feasibilityExcluded = !includeFeasibility;

  const formatArea = (value: number | null | undefined) =>
    formatAreaM2(Number(value), { maximumFractionDigits: 0 }, "");

  useEffect(() => {
    if (!isEditingFar && displayedFar != null) {
      setFarDraft(String(displayedFar));
    }
  }, [displayedFar, isEditingFar]);

  const buaNote = (key: string) => {
    const noteKey = `${key}_bua`;
    const showScenarioScale = scenarioAreaRatio != null && key !== "basement";
    if (explanations[noteKey]) {
      return (
        <>
          <div>{explanations[noteKey]}</div>
          {showScenarioScale && (
            <div style={{ marginTop: 4 }}>
              Scenario scale: ×{formatNumberValue(scenarioAreaRatio, 2)}
            </div>
          )}
        </>
      );
    }
    const ratio = areaRatio?.[key];
    if (siteArea != null && ratio != null) {
      return (
        <>
          <div>
            {t("excelNotes.siteAreaRatio", {
              area: formatNumberValue(siteArea, 0),
              ratio: formatNumberValue(ratio, 2),
            })}
          </div>
          {showScenarioScale && (
            <div style={{ marginTop: 4 }}>
              Scenario scale: ×{formatNumberValue(scenarioAreaRatio, 2)}
            </div>
          )}
        </>
      );
    }
    return (
      <>
        <div>{t("excelNotes.buaFallback")}</div>
        {showScenarioScale && (
          <div style={{ marginTop: 4 }}>
            Scenario scale: ×{formatNumberValue(scenarioAreaRatio, 2)}
          </div>
        )}
      </>
    );
  };

  const landNoteBase =
    explanations.land_cost ||
    (siteArea && landPricePpm2
      ? t("excelNotes.landCost", {
        area: formatNumberValue(siteArea, 0),
        price: formatNumberValue(landPricePpm2, 0),
        source: excelResult?.landPrice?.source_type || t("excel.sourceInput"),
      })
      : t("excelNotes.landCostFallback"));
  const landNote =
    scenarioLandPrice != null
      ? (
        <>
          <div>{landNoteBase}</div>
          <div style={{ marginTop: 4 }}>
            Scenario override: land price = {formatNumberValue(scenarioLandPrice, 0)} SAR/m²
          </div>
        </>
      )
      : landNoteBase;

  const farNote = (
    <>
      <div>{farNoteBase || t("excel.effectiveFarDefault")}</div>
      {scenarioAreaRatio != null && (
        <>
          {scenarioFar != null && (
            <div style={{ marginTop: 4 }}>
              Scenario override: FAR = {formatNumberValue(scenarioFar, 3)}
            </div>
          )}
          <div style={{ marginTop: 4 }}>
            Scaled by ×{formatNumberValue(scenarioAreaRatio, 2)}
          </div>
        </>
      )}
    </>
  );

  const fitoutNote =
    explanations.fitout ||
    (fitoutExcluded
      ? t("excelNotes.fitoutExcluded")
      : fitoutRate != null
      ? t("excelNotes.fitoutApplied", {
        area: formatNumberValue(fitoutArea, 0),
        rate: formatNumberValue(fitoutRate, 0),
      })
      : t("excelNotes.fitoutFallback"));

  const contingencyNote = contingencyExcluded
    ? t("excelNotes.contingencyExcluded")
    : explanations.contingency ||
      t("excelNotes.contingency", {
        pct: formatPercentValue(contingencyPct),
        direct: formatNumberValue(constructionDirectTotal, 0),
        fitout: formatNumberValue(fitoutTotal, 0),
      });

  const consultantsNote =
    explanations.consultants ||
    t("excelNotes.consultants", {
      base: formatNumberValue(consultantsBase, 0),
      pct: formatPercentValue(consultantsPct),
    });

  const transactionNote =
    explanations.transaction_cost ||
    t("excelNotes.transaction", {
      land: formatNumberValue(landCostValue, 0),
      pct: formatPercentValue(transactionPct),
    });
  const inclusionToggleStyle = (included: boolean) => ({
    border: "1px solid rgba(255,255,255,0.2)",
    background: included ? "rgba(16,185,129,0.15)" : "rgba(248,113,113,0.1)",
    color: "white",
    padding: "4px 8px",
    borderRadius: 999,
    cursor: "pointer",
    fontSize: "0.8rem",
  });

  const directNote =
    explanations.construction_direct ||
    Object.keys(directCost)
      .map((key) => {
        const area = builtArea[key] ?? 0;
        const costPerUnit = unitCost[key] ?? 0;
        return t("excelNotes.directItem", {
          key,
          area: formatNumberValue(area, 0),
          cost: formatNumberValue(costPerUnit, 0),
        });
      })
      .filter(Boolean)
      .join("; ");

  const incomeNote = t("excel.year1IncomeNote");

  const parkingIncomeExplanation =
    typeof explanations?.parking_income === "string" ? explanations.parking_income : null;
  const effectiveIncomePctRaw =
    usedInputs?.y1_income_effective_pct ??
    inputs?.y1_income_effective_pct ??
    null;
  const effectiveIncomePct = normalizeEffectivePct(effectiveIncomePctRaw as number | null | undefined);
  const committedEffectiveIncomePct = normalizeEffectivePct(
    inputsRef.current?.y1_income_effective_pct as number | undefined,
  );
  const effectiveIncomeApplyDisabled =
    !excelResult || resolveEffectivePctFromDraft(effectiveIncomePctDraft) === committedEffectiveIncomePct;
  const effectiveIncomeFactor = effectiveIncomePct / 100;
  // Some estimate fields may exist in excelResult.breakdown (raw backend excel output)
  // rather than excelResult.costs (API "cost_breakdown"). Prefer costs when present, fallback to breakdown.
  const excelBreakdown = (excelResult?.breakdown || {}) as Record<string, any>;
  const y1IncomeEffective =
    (excelResult?.costs?.y1_income_effective ?? excelBreakdown?.y1_income_effective ?? 0) as number;
  const opexPctResolved =
    (excelResult?.costs?.opex_pct ?? excelBreakdown?.opex_pct ?? inputs.opex_pct ?? 0.05) as number;
  const opexCostResolved =
    (excelResult?.costs?.opex_cost ??
      excelBreakdown?.opex_cost ??
      (y1IncomeEffective || 0) * (opexPctResolved || 0)) as number;
  const y1NoiResolved =
    (excelResult?.costs?.y1_noi ??
      excelBreakdown?.y1_noi ??
      (y1IncomeEffective || 0) - (opexCostResolved || 0)) as number;
  const y1IncomeEffectiveNote =
    explanations?.y1_income_effective ||
    t("excelNotes.effectiveIncome", {
      pct: formatPercentValue(effectiveIncomeFactor, 0),
    });
  const opexPct = opexPctResolved;
  const opexNote = t("excelNotes.opexEffectiveIncome", {
    pct: formatPercentValue(opexPct, 0),
  });

  const resolveRevenueNote = (key: string, baseNote: string, amount: number) => {
    if (key !== "parking_income") return baseNote;
    const trimmedExplanation = parkingIncomeExplanation?.trim() || "";
    if (Number(amount) > 0) {
      return trimmedExplanation || baseNote;
    }
    return t("excelNotes.revenueNoteDash");
  };

  const noteStyle = { fontSize: "0.8rem", color: "#cbd5f5" } as const;
  const baseCellStyle = { padding: "0.65rem 0.75rem", verticalAlign: "top" } as const;
  const itemColumnStyle = { ...baseCellStyle, paddingLeft: 0 } as const;
  const amountColumnStyle = {
    ...baseCellStyle,
    textAlign: "right",
    paddingRight: "1.5rem",
    direction: "ltr",
    unicodeBidi: "plaintext",
  } as const;
  const calcColumnStyle = {
    ...baseCellStyle,
    ...noteStyle,
    paddingLeft: "1rem",
    lineHeight: 1.5,
    wordBreak: "break-word",
  } as const;
  const itemHeaderStyle = { ...itemColumnStyle, fontWeight: 600 } as const;
  const amountHeaderStyle = { ...baseCellStyle, textAlign: "right", paddingRight: "1.5rem", fontWeight: 600 } as const;
  const calcHeaderStyle = { ...baseCellStyle, textAlign: "left", fontWeight: 600, paddingLeft: "1rem" } as const;
  const farEditInputStyle = {
    width: "160px",
    padding: "6px 10px",
    borderRadius: 6,
    border: "1px solid rgba(255,255,255,0.3)",
    background: "rgba(0,0,0,0.25)",
    color: "white",
    fontSize: "15px",
    fontWeight: 500,
    textAlign: "right" as const,
  };
  const farEditButtonStyle = {
    border: "none",
    padding: "4px 8px",
    minHeight: 32,
    borderRadius: 999,
    color: "#f8fafc",
    textDecoration: "underline",
    background: "rgba(148,163,184,0.2)",
    font: "inherit",
    cursor: "pointer",
    display: "inline-flex",
    alignItems: "center",
    gap: 6,
  } as const;
  const farEditActionStyle = {
    padding: "4px 10px",
    minHeight: 32,
    borderRadius: 6,
    border: "1px solid rgba(255,255,255,0.2)",
    fontSize: "0.8rem",
    fontWeight: 600,
    cursor: "pointer",
  } as const;
  const farErrorStyle = { color: "#fca5a5", fontSize: "0.75rem", marginTop: 4 } as const;

  const resetFarDraft = () => {
    setFarDraft(farAboveGround != null ? String(farAboveGround) : "");
  };

  const cancelFarEdit = () => {
    setIsEditingFar(false);
    resetFarDraft();
    setFarEditError(null);
  };

  const applyFarEdit = () => {
    const targetFar = Number(farDraft);
    if (!Number.isFinite(targetFar) || targetFar <= 0) {
      setFarEditError(t("excel.farEditErrorInvalid"));
      return;
    }

    const currentAreaRatio = inputsRef.current?.area_ratio || {};
    const scaled = scaleAboveGroundAreaRatio(currentAreaRatio, targetFar);
    if (!scaled) {
      setFarEditError(t("excel.farEditErrorMissing"));
      return;
    }

    applyInputPatch({ area_ratio: scaled.nextAreaRatio }, true);
    setIsEditingFar(false);
    setFarEditError(null);
    setFarDraft(String(targetFar));
    void trackEvent("ui_override_far", {
      meta: {
        from: farAboveGround ?? null,
        to: targetFar,
      },
    });
  };

  const startFarEdit = () => {
    if (farAboveGround == null) return;
    setFarEditError(null);
    setFarDraft(String(farAboveGround));
    setIsEditingFar(true);
  };
  const farApplyDisabled =
    farDraft.trim() === "" ||
    !Number.isFinite(Number(farDraft)) ||
    Number(farDraft) <= 0 ||
    (farAboveGround != null && Number(farDraft) === Number(farAboveGround));
  const revenueItems = Object.keys(incomeComponents || {}).map((key) => {
    const nlaVal = nla[key] ?? 0;
    const efficiencyVal = efficiency[key] ?? null;
    const baseArea = builtArea[key] ?? null;
    const efficiencyText =
      efficiencyVal != null && baseArea != null
        ? t("excelNotes.incomeEfficiency", {
          nla: formatNumberValue(nlaVal, 0),
          built: formatNumberValue(baseArea, 0),
          efficiency: formatNumberValue((efficiencyVal as number) * 100, 0),
        })
        : t("excelNotes.incomeBase", { nla: formatNumberValue(nlaVal, 0) });
    const rent = appliedRentRates[key] ?? 0;
    const baseNote = t("excelNotes.incomeItem", {
      key,
      efficiencyText,
      rent: formatNumberValue(rent, 0),
    });
    return {
      key,
      amount: incomeComponents[key] ?? 0,
      note: resolveRevenueNote(key, baseNote, incomeComponents[key] ?? 0),
    };
  });
  const summaryText =
    (isArabic
      ? notes.summary_ar ?? notes.summary_en ?? notes.summary
      : notes.summary_en ?? notes.summary ?? excelResult?.summary
    )?.trim() || (excelResult ? t("excel.summaryRoi", { value: formatPercentValue(excelResult.roi) }) : "");
  const unitCostFields = [
    { key: "residential", label: t("excel.unitCostResidential") },
    { key: "retail", label: t("excel.unitCostRetail") },
    { key: "office", label: t("excel.unitCostOffice") },
    { key: "basement", label: t("excel.unitCostBasement") },
  ];
  const activeUnitCostFields =
    effectiveLandUse === "m"
      ? unitCostFields
      : unitCostFields.filter((field) => field.key === "residential" || field.key === "basement");
  return (
    <div>
      <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <span>{t("excel.providerLabel")}</span>
          <select
            value={provider}
            onChange={(event) => {
              const nextProvider = event.target.value as any;
              setProvider(nextProvider);
              void trackEvent("ui_change_provider", { meta: { provider: nextProvider } });
            }}
          >
            {PROVIDERS.map((item) => (
              <option key={item.value} value={item.value}>
                {t(item.labelKey)}
              </option>
            ))}
          </select>
          <button onClick={fetchPrice}>{t("excel.fetchPrice")}</button>
          {price != null && (
            <strong>
              {t("excel.suggestedPrice", {
                price: formatNumberValue(price, 0),
                provider: providerLabel,
              })}
            </strong>
          )}
          {fetchError && <span style={{ color: "#fca5a5" }}>{t("common.errorPrefix")} {fetchError}</span>}
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <label style={{ opacity: 0.85 }}>{t("excel.overrideLandUse")}</label>
          <select
            value={overrideLandUse ?? ""}
            onChange={(event) => {
              const value = (event.target.value || "").trim().toLowerCase();
              if (!value) {
                setOverrideLandUse(null);
                return;
              }
              if (value === "s" || value === "m") {
                setOverrideLandUse(value as LandUseCode);
              }
            }}
            title={t("excel.overrideLandUseHint")}
          >
            <option value="">{t("excel.autoUseParcel")}</option>
            <option value="s">{t("excel.landUseOption", { code: "s", label: t("app.landUse.residential") })}</option>
            <option value="m">{t("excel.landUseOption", { code: "m", label: t("app.landUse.mixed") })}</option>
          </select>
          <span style={{ opacity: 0.75, fontSize: "0.8rem" }}>
            {t("excel.activeTemplate")} <strong>{effectiveLandUse}</strong>
          </span>
        </div>
        <label style={{ display: "flex", flexDirection: "column", gap: 4, color: "white" }}>
          <span>{t("excel.overrideLandPrice")}</span>
          <input
            type="number"
            value={inputs.land_price_sar_m2 ?? ""}
            onChange={(event) => {
              const prevValue = inputsRef.current?.land_price_sar_m2 ?? null;
              const nextValue = event.target.value === "" ? 0 : Number(event.target.value);
              applyInputPatch({ land_price_sar_m2: nextValue });
              if (prevValue !== nextValue) {
                void trackEvent("ui_override_land_price", {
                  meta: {
                    from: prevValue,
                    to: nextValue,
                  },
                });
              }
            }}
            style={{ padding: "4px 6px", borderRadius: 4, border: "1px solid rgba(255,255,255,0.2)" }}
          />
          <span style={{ fontSize: "0.8rem", color: "#cbd5f5" }}>
            {suggestedPrice != null
              ? t("excel.suggestedFromFetch", {
                price: formatNumberValue(suggestedPrice, 0),
                provider: providerLabel,
              })
              : t("excel.notFetched")}
          </span>
        </label>
        <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
          <span style={{ color: "white", fontWeight: 600 }}>{t("excel.unitCostTitle")}</span>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 8,
            }}
          >
            {activeUnitCostFields.map((field) => (
              <label
                key={field.key}
                style={{ display: "flex", flexDirection: "column", gap: 4, color: "white" }}
              >
                <span style={{ fontSize: "0.85rem", color: "#cbd5f5" }}>{field.label}</span>
                <input
                  type="number"
                  step="1"
                  min="0"
                  value={unitCostInputs[field.key] ?? ""}
                  onChange={(event) => updateUnitCost(field.key, event.target.value)}
                  style={{ padding: "4px 6px", borderRadius: 4, border: "1px solid rgba(255,255,255,0.2)" }}
                />
              </label>
            ))}
          </div>
        </div>
      </div>

      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginTop: 12, alignItems: "center" }}>
        <button onClick={handleEstimateClick}>{t("excel.calculateEstimate")}</button>
        <button
          type="button"
          onClick={() => setIsScenarioOpen(true)}
          disabled={!estimateId || isScenarioSubmitting}
        >
          Scenario
        </button>
        {isScenarioActive && (
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span
              style={{
                padding: "4px 8px",
                borderRadius: 999,
                fontSize: "0.75rem",
                border: "1px solid rgba(148,163,184,0.6)",
                color: "#e2e8f0",
                background: "rgba(148,163,184,0.2)",
              }}
            >
              Scenario active
            </span>
            <button
              type="button"
              onClick={() => {
                if (scenarioBaseResult) {
                  setExcelResult(scenarioBaseResult);
                  setScenarioBaseResult(null);
                  setIsScenarioActive(false);
                }
              }}
              style={{
                background: "transparent",
                border: "none",
                color: "#cbd5f5",
                textDecoration: "underline",
                cursor: "pointer",
                padding: 0,
                fontSize: "0.8rem",
              }}
            >
              Reset scenario
            </button>
          </div>
        )}
        {estimateId && (
          <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            <button type="button" onClick={handleExportPdf}>
              Export PDF
            </button>
            <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: "0.8rem", color: "#cbd5f5" }}>
              <span>Estimate ID: {estimateId}</span>
              <button
                type="button"
                onClick={copyEstimateId}
                aria-label="Copy estimate ID"
                style={{
                  border: "none",
                  background: "transparent",
                  color: "#cbd5f5",
                  cursor: "pointer",
                  padding: 0,
                }}
              >
                📋
              </button>
            </div>
          </div>
        )}
      </div>

      {error && (
        <div style={{ marginTop: 12, color: "#fca5a5" }}>
          {t("common.errorPrefix")} {error}
        </div>
      )}

      {excelResult && (
        <div
          style={{
            marginTop: "1rem",
            padding: "1rem",
            borderRadius: "0.5rem",
            background: "rgba(0,0,0,0.3)",
            color: "white",
            maxWidth: "100%",
            fontSize: "0.9rem",
          }}
        >
          <h3 style={{ marginTop: 0, marginBottom: "0.5rem" }}>
            {t("excel.financialBreakdown")}
          </h3>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
              gap: "1rem",
              alignItems: "start",
            }}
          >
            <div style={{ background: "rgba(255,255,255,0.03)", borderRadius: 8, padding: "0.75rem" }}>
              <h4 style={{ marginTop: 0, marginBottom: "0.5rem" }}>{t("excel.costBreakdown")}</h4>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={itemHeaderStyle}>{t("excel.item")}</th>
                    <th style={amountHeaderStyle}>{t("excel.amount")}</th>
                    <th style={calcHeaderStyle}>{t("excel.calculation")}</th>
                  </tr>
                </thead>
                <tbody>
                  {farAboveGround != null && (
                    <tr>
                      <td style={itemColumnStyle}>{t("excel.effectiveFar")}</td>
                      <td style={amountColumnStyle}>
                        {isEditingFar ? (
                          <div style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", gap: 6 }}>
                            <input
                              type="number"
                              step="0.01"
                              min="0"
                              data-field="effective_far"
                              value={farDraft}
                              onChange={(event) => setFarDraft(event.target.value)}
                              onKeyDown={(event) => {
                                if (event.key === "Enter") {
                                  event.preventDefault();
                                  applyFarEdit();
                                }
                                if (event.key === "Escape") {
                                  event.preventDefault();
                                  cancelFarEdit();
                                }
                              }}
                              style={farEditInputStyle}
                              autoFocus
                            />
                            <button
                              type="button"
                              onClick={applyFarEdit}
                              disabled={farApplyDisabled}
                              style={{
                                ...farEditActionStyle,
                                background: "rgba(59,130,246,0.9)",
                                color: "white",
                                opacity: farApplyDisabled ? 0.5 : 1,
                              }}
                            >
                              {t("excel.apply")}
                            </button>
                            <button
                              type="button"
                              onClick={cancelFarEdit}
                              style={{
                                ...farEditActionStyle,
                                background: "rgba(148,163,184,0.2)",
                                color: "white",
                              }}
                            >
                              {t("excel.cancel")}
                            </button>
                          </div>
                        ) : (
                          <button
                            type="button"
                            onClick={startFarEdit}
                            style={farEditButtonStyle}
                            title={t("excel.farEditHint")}
                          >
                            <span>{formatNumberValue(displayedFar, 3)}</span>
                            <span style={{ fontSize: "0.8rem", opacity: 0.85 }}>{t("excel.farEdit")}</span>
                          </button>
                        )}
                      </td>
                      <td style={calcColumnStyle}>
                        {farNote}
                        {!isEditingFar && (
                          <div style={{ marginTop: 6 }}>{t("excel.farEditHintInline")}</div>
                        )}
                        {farEditError && <div style={farErrorStyle}>{farEditError}</div>}
                      </td>
                    </tr>
                  )}
                  <tr>
                    <td style={itemColumnStyle}>{t("excel.residentialBua")}</td>
                    <td style={amountColumnStyle}>{formatArea(displayedBuiltArea.residential)}</td>
                    <td style={calcColumnStyle}>{buaNote("residential")}</td>
                  </tr>
                  {effectiveLandUse === "m" && builtArea.retail !== undefined && (
                    <tr>
                      <td style={itemColumnStyle}>{t("excel.retailBua")}</td>
                      <td style={amountColumnStyle}>{formatArea(displayedBuiltArea.retail)}</td>
                      <td style={calcColumnStyle}>{buaNote("retail")}</td>
                    </tr>
                  )}
                  {effectiveLandUse === "m" && builtArea.office !== undefined && (
                    <tr>
                      <td style={itemColumnStyle}>{t("excel.officeBua")}</td>
                      <td style={amountColumnStyle}>{formatArea(displayedBuiltArea.office)}</td>
                      <td style={calcColumnStyle}>{buaNote("office")}</td>
                    </tr>
                  )}
                  <tr>
                    <td style={itemColumnStyle}>{t("excel.basementBua")}</td>
                    <td style={amountColumnStyle}>{formatArea(displayedBuiltArea.basement)}</td>
                    <td style={calcColumnStyle}>{buaNote("basement")}</td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>{t("excel.landCost")}</td>
                    <td style={amountColumnStyle}>
                      {formatCurrencySAR(excelResult.costs.land_cost)}
                    </td>
                    <td style={calcColumnStyle}>
                      {landNote}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>{t("excel.constructionDirect")}</td>
                    <td style={amountColumnStyle}>
                      {formatCurrencySAR(excelResult.costs.construction_direct_cost)}
                    </td>
                    <td style={calcColumnStyle}>
                      {explanations.construction_direct
                        ? directNote
                        : directNote
                        ? `${directNote}; ${t("excel.constructionDirectDefault")}`
                        : t("excel.constructionDirectDefault")}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          gap: "0.5rem",
                        }}
                      >
                        <span>{t("excel.fitout")}</span>
                        <button
                          type="button"
                          onClick={() => handleFitoutToggle(!includeFitout)}
                          style={inclusionToggleStyle(includeFitout)}
                        >
                          {includeFitout ? t("excel.fitoutIncluded") : t("excel.fitoutExcluded")}
                        </button>
                      </div>
                    </td>
                    <td style={amountColumnStyle}>
                      {formatCurrencySAR(excelResult.costs.fitout_cost)}
                    </td>
                    <td style={calcColumnStyle}>
                      {fitoutNote}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          gap: "0.5rem",
                        }}
                      >
                        <span>{t("excel.contingency")}</span>
                        <button
                          type="button"
                          onClick={() => handleContingencyToggle(!includeContingency)}
                          style={inclusionToggleStyle(includeContingency)}
                        >
                          {includeContingency ? t("excel.fitoutIncluded") : t("excel.fitoutExcluded")}
                        </button>
                      </div>
                    </td>
                    <td style={amountColumnStyle}>
                      {formatCurrencySAR(excelResult.costs.contingency_cost)}
                    </td>
                    <td style={calcColumnStyle}>
                      {contingencyNote}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>{t("excel.consultants")}</td>
                    <td style={amountColumnStyle}>
                      {formatCurrencySAR(excelResult.costs.consultants_cost)}
                    </td>
                    <td style={calcColumnStyle}>
                      {consultantsNote}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          gap: "0.5rem",
                        }}
                      >
                        <span>{t("excel.feasibilityFee")}</span>
                        <button
                          type="button"
                          onClick={() => handleFeasibilityToggle(!includeFeasibility)}
                          style={inclusionToggleStyle(includeFeasibility)}
                        >
                          {includeFeasibility ? t("excel.fitoutIncluded") : t("excel.fitoutExcluded")}
                        </button>
                      </div>
                    </td>
                    <td style={amountColumnStyle}>
                      {formatCurrencySAR(excelResult.costs.feasibility_fee)}
                    </td>
                    <td style={calcColumnStyle}>
                      {feasibilityExcluded
                        ? t("excelNotes.feasibilityExcluded")
                        : t("excelNotes.feasibility", {
                          land: formatNumberValue(landCostValue, 0),
                          pct: formatPercentValue(feasibilityPct, 1),
                        })}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>{t("excel.transactionCosts")}</td>
                    <td style={amountColumnStyle}>
                      {formatCurrencySAR(excelResult.costs.transaction_cost)}
                    </td>
                    <td style={calcColumnStyle}>
                      {transactionNote}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>
                      <strong>{t("excel.totalCapex")}</strong>
                    </td>
                    <td style={amountColumnStyle}>
                      <strong>{formatCurrencySAR(excelResult.costs.grand_total_capex)}</strong>
                    </td>
                    <td style={calcColumnStyle}>
                      {t("excel.totalCapexNote")}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>

            <div style={{ background: "rgba(255,255,255,0.03)", borderRadius: 8, padding: "0.75rem" }}>
              <h4 style={{ marginTop: 0, marginBottom: "0.5rem" }}>{t("excel.revenueBreakdown")}</h4>
              {rentMeta?.provider === "REGA" && residentialRentMo != null && (
                <p style={{ marginTop: 0, marginBottom: "0.75rem", fontSize: "0.8rem", color: "#cbd5f5" }}>
                  {t("excel.regaNote", {
                    location: rentMeta.district || rentMeta.city || t("common.notAvailable"),
                    monthly: formatNumberValue(residentialRentMo, 0),
                    yearly: formatNumberValue(residentialRentYr, 0),
                  })}
                </p>
              )}
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={itemHeaderStyle}>{t("excel.item")}</th>
                    <th style={amountHeaderStyle}>{t("excel.amount")}</th>
                    <th style={calcHeaderStyle}>{t("excel.calculation")}</th>
                  </tr>
                </thead>
                <tbody>
                  {revenueItems.map((item) => (
                    <tr key={item.key}>
                      <td style={itemColumnStyle}>{item.key.replace(/_/g, " ")}</td>
                      <td style={amountColumnStyle}>{formatCurrencySAR(item.amount || 0)}</td>
                      <td style={calcColumnStyle}>{item.note}</td>
                    </tr>
                  ))}
                  <tr>
                    <td style={itemColumnStyle}>{t("excel.year1Income")}</td>
                    <td style={amountColumnStyle}>
                      {formatCurrencySAR(excelResult.costs.y1_income)}
                    </td>
                    <td style={calcColumnStyle}>
                      {incomeNote || t("excel.year1IncomeNote")}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                        <span>{t("excel.year1IncomeEffective")}</span>
                        <label style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: "0.9rem" }}>
                          <input
                            type="number"
                            min={0}
                            max={100}
                            step={1}
                            value={effectiveIncomePctDraft}
                            onChange={(event) => setEffectiveIncomePctDraft(event.target.value)}
                            onKeyDown={(event) => {
                              if (event.key === "Enter") {
                                event.preventDefault();
                                commitEffectiveIncomePct();
                              }
                            }}
                            style={{
                              width: 72,
                              padding: "4px 6px",
                              borderRadius: 4,
                              border: "1px solid rgba(255,255,255,0.2)",
                              background: "rgba(0,0,0,0.15)",
                              color: "white",
                            }}
                            aria-label={t("excel.effectiveIncomePct")}
                          />
                          <span style={{ opacity: 0.75 }}>%</span>
                        </label>
                        <button
                          type="button"
                          onClick={() => commitEffectiveIncomePct()}
                          disabled={effectiveIncomeApplyDisabled}
                          style={{
                            padding: "4px 8px",
                            borderRadius: 6,
                            border: "1px solid rgba(255,255,255,0.2)",
                            background: "rgba(255,255,255,0.08)",
                            color: "white",
                            cursor: effectiveIncomeApplyDisabled ? "not-allowed" : "pointer",
                            opacity: effectiveIncomeApplyDisabled ? 0.6 : 1,
                          }}
                        >
                          {t("common.apply")}
                        </button>
                      </div>
                    </td>
                    <td style={amountColumnStyle}>
                      {formatCurrencySAR(excelResult.costs.y1_income_effective ?? 0)}
                    </td>
                    <td style={calcColumnStyle}>{y1IncomeEffectiveNote}</td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          gap: "0.5rem",
                        }}
                      >
                        <span>{t("excel.opex")}</span>
                        <div style={{ display: "inline-flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                          <input
                            type="number"
                            step="0.01"
                            min={0}
                            max={1}
                            value={inputs.opex_pct ?? 0}
                            onChange={(event) => {
                              const rawValue = event.target.value;
                              const parsed = rawValue === "" ? 0 : Number(rawValue);
                              const clamped = Number.isFinite(parsed) ? Math.max(0, Math.min(parsed, 1)) : 0;
                              applyInputPatch({ opex_pct: clamped }, Boolean(excelResult));
                            }}
                            style={{
                              width: 72,
                              padding: "4px 6px",
                              borderRadius: 6,
                              border: "1px solid rgba(255,255,255,0.2)",
                              background: "rgba(0,0,0,0.15)",
                              color: "white",
                            }}
                            aria-label={t("excel.opex")}
                            disabled={!includeOpex}
                          />
                          <button
                            type="button"
                            onClick={() => handleOpexToggle(!includeOpex)}
                            style={inclusionToggleStyle(includeOpex)}
                          >
                            {includeOpex ? t("excel.included") : t("excel.excluded")}
                          </button>
                        </div>
                      </div>
                    </td>
                    <td style={amountColumnStyle}>{formatCurrencySAR(opexCostResolved)}</td>
                    <td style={calcColumnStyle}>{opexNote}</td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>{t("excel.noiYear1")}</td>
                    <td style={amountColumnStyle}>{formatCurrencySAR(y1NoiResolved)}</td>
                    <td style={calcColumnStyle}>{t("excelNotes.noiYear1")}</td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>
                      <strong>{t("excel.unleveredRoi")}</strong>
                    </td>
                    <td style={amountColumnStyle}>
                      <strong>{formatPercentValue(excelResult.roi)}</strong>
                    </td>
                    <td style={calcColumnStyle}>{t("excelNotes.roiNoiFormula")}</td>
                  </tr>
                </tbody>
              </table>

            </div>
          </div>
          <ParkingSummary totals={excelResult.totals} notes={excelResult.notes} />
          {summaryText && (
            <div
              style={{
                marginTop: "0.75rem",
                paddingTop: "0.75rem",
                borderTop: "1px solid rgba(255,255,255,0.08)",
              }}
            >
              <h5 style={{ margin: "0 0 0.35rem 0", fontSize: "0.95rem" }}>
                {t("excel.executiveSummary")}
              </h5>
              <p style={{ margin: 0, lineHeight: 1.4 }}>{summaryText}</p>
            </div>
          )}
          <div ref={feedbackSentinelRef} style={{ height: 1 }} />
        </div>
      )}
      <ScenarioModal
        isOpen={isScenarioOpen}
        providers={scenarioProviders}
        isSubmitting={isScenarioSubmitting}
        onClose={() => setIsScenarioOpen(false)}
        onSubmit={handleScenarioSubmit}
      />
      <MicroFeedbackPrompt
        isOpen={isFeedbackOpen}
        context={feedbackContext}
        estimateId={estimateId}
        onDismiss={handleFeedbackDismiss}
        onSubmit={handleFeedbackSubmit}
      />
    </div>
  );
}
