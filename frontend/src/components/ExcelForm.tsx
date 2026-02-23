import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import type { Geometry } from "geojson";
import { useTranslation } from "react-i18next";
import { ChevronDownIcon, ChevronRightIcon } from "@heroicons/react/24/outline";

import "../styles/excel-form.css";
import "../styles/calculations.css";
import "../styles/ui-v2.css";

import { landPrice, makeEstimate, runScenario, trackEvent } from "../api";
import {
  cloneTemplate,
  ExcelInputs,
  LandUseCode,
  ProgramComponents,
  templateForLandUse,
} from "../lib/excelTemplates";
import ParkingSummary from "./ParkingSummary";
import type { EstimateNotes, EstimateTotals } from "../lib/types";
import { formatAreaM2, formatCurrencySAR, formatNumber, formatPercent } from "../i18n/format";
import { resolveAreaRatioBase, scaleAboveGroundAreaRatio } from "../utils/areaRatio";
import { applyPatch } from "../utils/applyPatch";
import { formatPercentDraftFromFraction, resolveFractionFromDraftPercent } from "../utils/opex";
import MicroFeedbackPrompt from "./MicroFeedbackPrompt";
import ScenarioModal from "./ScenarioModal";
import EstimateCalculationsPanel from "./EstimateCalculationsPanel";
import Button from "./ui/Button";
import Card from "./ui/Card";
import Checkbox from "./ui/Checkbox";
import Field from "./ui/Field";
import Input from "./ui/Input";
import Select from "./ui/Select";
import Radio from "./ui/Radio";
import Table from "./ui/Table";
import Tabs from "./ui/Tabs";
import ToggleChip from "./ui/ToggleChip";

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
  assumptions?: Array<Record<string, any>>;
  used_inputs?: {
    area_ratio?: Record<string, number | string>;
  };
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
  mode?: "legacy" | "v2";
};

type MassingLock = "far" | "floors" | "coverage";
type ResultTab = "summary" | "financial" | "revenue" | "parking";

const normalizeLandUse = (value?: string | null): LandUseCode | null => {
  const v = (value || "").trim().toLowerCase();
  return v === "m" ? "m" : v === "s" ? "s" : null;
};

const normalizeEffectivePct = (value?: number | null) => {
  if (value == null || Number.isNaN(value)) return 90;
  return Math.max(0, Math.min(value, 100));
};

const normalizeCoverageRatio = (value?: number | null) => {
  if (value == null || Number.isNaN(value)) return null;
  if (value <= 0 || value > 1) return null;
  return value;
};

const resolveMassingLock = (value?: string | null): MassingLock => {
  if (value === "floors" || value === "coverage") return value;
  return "far";
};

const isBasementKey = (key: string) => {
  const k = (key || "").trim().toLowerCase();
  return k.includes("basement") || k.includes("underground") || k.includes("below");
};

const sumAboveGroundFar = (areaRatio: Record<string, any> | null | undefined) => {
  if (!areaRatio) return null;
  let sum = 0;
  for (const [key, value] of Object.entries(areaRatio)) {
    if (isBasementKey(key)) continue;
    const numeric = Number(value);
    if (Number.isFinite(numeric)) sum += numeric;
  }
  return sum > 0 ? sum : null;
};

const pinAreaRatioToFar = (
  areaRatio: Record<string, any> | null | undefined,
  targetFar: number | null | undefined,
) => {
  if (!areaRatio || targetFar == null || !Number.isFinite(targetFar) || targetFar <= 0) return null;
  const baseSum = sumAboveGroundFar(areaRatio);
  if (!baseSum) return null;
  const factor = targetFar / baseSum;
  if (!Number.isFinite(factor) || factor <= 0) return null;

  const out: Record<string, any> = {};
  for (const [key, value] of Object.entries(areaRatio)) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      out[key] = value;
      continue;
    }
    if (isBasementKey(key)) out[key] = numeric;
    else out[key] = numeric * factor;
  }
  return out;
};

const roundTo = (value: number, digits = 1) => {
  const factor = Math.pow(10, digits);
  return Math.round(value * factor) / factor;
};

export default function ExcelForm({ parcel, landUseOverride, mode = "legacy" }: ExcelFormProps) {
  const { t, i18n } = useTranslation();
  const [provider, setProvider] = useState<(typeof PROVIDERS)[number]["value"]>("blended_v1");
  const [price, setPrice] = useState<number | null>(null);
  const [suggestedPrice, setSuggestedPrice] = useState<number | null>(null);
  const [showLandPriceOverride, setShowLandPriceOverride] = useState(false);
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

  const [components, setComponents] = useState<ProgramComponents>({
    residential: true,
    retail: true,
    office: true,
  });
  const [componentsDraft, setComponentsDraft] = useState<ProgramComponents>(components);
  const componentsDirty =
    componentsDraft.residential !== components.residential ||
    componentsDraft.retail !== components.retail ||
    componentsDraft.office !== components.office;

  useEffect(() => {
    setComponentsDraft(components);
  }, [components]);

  const toggleComponent = (key: keyof ProgramComponents) => {
    setComponentsDraft((prev) => {
      const next = { ...prev, [key]: !prev[key] };
      if (!next.residential && !next.retail && !next.office) return prev;
      return next;
    });
  };

  const toggleComponentForMode = (key: keyof ProgramComponents) => {
    if (mode !== "v2") {
      toggleComponent(key);
      return;
    }

    setComponentsDraft((prev) => {
      const next = { ...prev, [key]: !prev[key] };
      if (!next.residential && !next.retail && !next.office) return prev;
      setComponents(next);
      applyInputPatch({ components: next }, Boolean(excelResultRef.current));
      return next;
    });
  };

  const applyComponents = () => {
    setComponents(componentsDraft);
    applyInputPatch({ components: componentsDraft }, Boolean(excelResult));
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
  const [showCalculations, setShowCalculations] = useState(false);
  const [activeCalcTab, setActiveCalcTab] = useState<ResultTab>("financial");
  const [activeV2Tab, setActiveV2Tab] = useState<ResultTab>("summary");
  const [v2FinancialOpen, setV2FinancialOpen] = useState<Record<string, boolean>>({
    costBreakdown: false,
    builtUpAreas: true,
    landAndConstruction: false,
    additionalCosts: true,
  });
  const [v2RevenueSections, setV2RevenueSections] = useState({
    rental: true,
    income: true,
    performance: false,
  });
  const [v2ParkingSections, setV2ParkingSections] = useState({
    requiredByComponent: true,
    notes: true,
  });
  const [effectiveIncomePctDraft, setEffectiveIncomePctDraft] = useState<string>(() =>
    String(normalizeEffectivePct(cloneTemplate(templateForLandUse(initialLandUse)).y1_income_effective_pct)),
  );
  const [opexPctDraft, setOpexPctDraft] = useState<string>(
    formatPercentDraftFromFraction(inputs.opex_pct),
  );
  const [coverageDraft, setCoverageDraft] = useState<string>(
    formatPercentDraftFromFraction(inputs.coverage_ratio, 0),
  );
  const [floorsDraft, setFloorsDraft] = useState<string>("");
  const [isEditingFar, setIsEditingFar] = useState(false);
  const [farDraft, setFarDraft] = useState<string>("");
  const [farEditError, setFarEditError] = useState<string | null>(null);
  const [coverageEditError, setCoverageEditError] = useState<string | null>(null);
  const [floorsEditError, setFloorsEditError] = useState<string | null>(null);
  const [isScenarioOpen, setIsScenarioOpen] = useState(false);
  const [isScenarioSubmitting, setIsScenarioSubmitting] = useState(false);
  const [scenarioBaseResult, setScenarioBaseResult] = useState<ExcelResult | null>(null);
  const excelResultRef = useRef<ExcelResult | null>(null);
  const unitCostInputs = inputs.unit_cost || {};
  const getBestAreaRatio = (): ExcelInputs["area_ratio"] | null => {
    const fromUsedInputs =
      (excelResultRef.current?.used_inputs?.area_ratio as ExcelInputs["area_ratio"] | undefined) ??
      (excelResultRef.current?.notes?.used_inputs?.area_ratio as ExcelInputs["area_ratio"] | undefined);

    if (fromUsedInputs && Object.keys(fromUsedInputs).length > 0) return fromUsedInputs;

    const fromInputs = inputsRef.current?.area_ratio as ExcelInputs["area_ratio"] | undefined;
    if (fromInputs && Object.keys(fromInputs).length > 0) return fromInputs;

    return null;
  };

  useEffect(() => {
    const geometrySignature = parcel?.geometry ? JSON.stringify(parcel.geometry) : "";
    const parcelKey = `${parcel?.parcel_id || ""}::${geometrySignature}`;
    if (parcelIdentityRef.current !== parcelKey) {
      parcelIdentityRef.current = parcelKey;
      setOverrideLandUse(null);
      setEstimateId(null);
      setScenarioBaseResult(null);
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
    if ((inputs.land_price_sar_m2 ?? 0) > 0) {
      setShowLandPriceOverride(true);
    }
  }, [inputs.land_price_sar_m2]);

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
      const defaultCoverage = effectiveLandUse === "m" ? 0.6 : 0.7;
      const currentCoverage = normalizeCoverageRatio(inputsRef.current?.coverage_ratio ?? null);
      if (prev.coverage_ratio == null && currentCoverage == null) {
        nextPatch.coverage_ratio = defaultCoverage;
      }
      if (prev.massing_lock == null && inputsRef.current?.massing_lock == null) {
        nextPatch.massing_lock = "far";
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
    setOpexPctDraft(formatPercentDraftFromFraction(inputs.opex_pct));
  }, [inputs.opex_pct]);

  useEffect(() => {
    const resolved = normalizeCoverageRatio(inputs.coverage_ratio ?? null);
    setCoverageDraft(
      formatPercentDraftFromFraction(resolved ?? (effectiveLandUse === "m" ? 0.6 : 0.7), 0),
    );
  }, [effectiveLandUse, inputs.coverage_ratio]);

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

  const resolveOpexPctFromDraft = (draft: string) => resolveFractionFromDraftPercent(draft);
  const resolveCoverageFromDraft = (draft: string) => {
    if (draft.trim() === "") return null;
    const parsed = Number(draft);
    if (!Number.isFinite(parsed)) return null;
    if (parsed <= 0 || parsed > 100) return null;
    return parsed / 100;
  };
  const resolveFloorsFromDraft = (draft: string) => {
    const trimmed = draft.trim();
    if (!trimmed) return null;
    const parsed = Number(trimmed);
    if (!Number.isFinite(parsed) || parsed <= 0) return null;
    return parsed;
  };
  const floorsAdjustmentValue = useMemo(() => {
    const notes = excelResult?.notes || {};
    const nestedNotes = notes?.notes ?? null;
    const floorsAdjustment =
      notes?.floors_adjustment ?? (nestedNotes && typeof nestedNotes === "object" ? nestedNotes.floors_adjustment : null);
    return floorsAdjustment && typeof floorsAdjustment.desired_floors_above_ground === "number"
      ? floorsAdjustment.desired_floors_above_ground
      : null;
  }, [excelResult]);
  const committedFloorsValue = useMemo(() => {
    const overrideValue = inputs?.desired_floors_above_ground;
    if (typeof overrideValue === "number" && Number.isFinite(overrideValue) && overrideValue > 0) {
      return overrideValue;
    }
    if (typeof floorsAdjustmentValue === "number" && floorsAdjustmentValue > 0) {
      return floorsAdjustmentValue;
    }
    return effectiveLandUse === "m" ? 3.5 : null;
  }, [effectiveLandUse, floorsAdjustmentValue, inputs?.desired_floors_above_ground]);
  const massingLock = resolveMassingLock(inputs.massing_lock ?? null);
  const defaultCoverageRatio = effectiveLandUse === "m" ? 0.6 : 0.7;
  const coverageRatio = normalizeCoverageRatio(inputs.coverage_ratio ?? null) ?? defaultCoverageRatio;

  const resolveScaledAreaRatio = (targetFar: number) => {
    const baseRatio = resolveAreaRatioBase([
      excelResultRef.current?.used_inputs?.area_ratio,
      inputsRef.current?.area_ratio,
      templateForLandUse(effectiveLandUse).area_ratio,
    ]);
    return scaleAboveGroundAreaRatio(baseRatio, targetFar);
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

  const commitOpexPct = (draftOverride?: string) => {
    const resolved = resolveOpexPctFromDraft(draftOverride ?? opexPctDraft);
    const currentPct = Math.max(0, Math.min(inputsRef.current?.opex_pct ?? 0, 1));

    if (resolved == null) return;

    if (Math.abs(resolved - currentPct) < 1e-6) {
      setOpexPctDraft(formatPercentDraftFromFraction(resolved));
      return;
    }

    applyInputPatch({ opex_pct: resolved }, Boolean(excelResult));
    setOpexPctDraft(formatPercentDraftFromFraction(resolved));
  };

  const commitCoverage = () => {
    const resolved = resolveCoverageFromDraft(coverageDraft);
    if (resolved == null) {
      setCoverageEditError("Enter a percent greater than 0 and up to 100.");
      return;
    }
    const committed = normalizeCoverageRatio(inputsRef.current?.coverage_ratio ?? null) ?? defaultCoverageRatio;
    if (Math.abs(resolved - committed) < 1e-6) {
      setCoverageDraft(formatPercentDraftFromFraction(resolved, 0));
      setCoverageEditError(null);
      return;
    }

    const lock = resolveMassingLock(inputsRef.current?.massing_lock ?? inputs.massing_lock ?? null);
    const farValue = displayedFar ?? farAboveGround ?? null;
    const floorsValue =
      committedFloorsValue ??
      (impliedFloors != null && Number.isFinite(impliedFloors) ? roundTo(impliedFloors, 1) : null);

    if (lock === "floors") {
      if (floorsValue == null || floorsValue <= 0) {
        setCoverageEditError("Set floors before updating coverage.");
        return;
      }
      const targetFar = floorsValue * resolved;
      const scaled = resolveScaledAreaRatio(targetFar);
      if (!scaled) {
        setCoverageEditError(t("excel.farEditErrorMissing"));
        return;
      }
      applyInputPatch(
        {
          coverage_ratio: resolved,
          desired_floors_above_ground: floorsValue,
          area_ratio: scaled.nextAreaRatio,
          disable_floors_scaling: true,
          disable_placeholder_area_ratio_scaling: true,
          massing_lock: "coverage",
        },
        true,
      );
      setFloorsDraft(String(roundTo(floorsValue, 1)));
      setCoverageDraft(formatPercentDraftFromFraction(resolved, 0));
      setCoverageEditError(null);
      return;
    }

    if (lock === "far" || lock === "coverage") {
      if (farValue == null || !Number.isFinite(farValue) || farValue <= 0) {
        setCoverageEditError("FAR must be available to update coverage.");
        return;
      }
      const baseAreaRatio = getBestAreaRatio();
      if (!baseAreaRatio) {
        setCoverageEditError("Missing current area ratios; please run estimate first.");
        return;
      }
      const bakedAreaRatio = pinAreaRatioToFar(baseAreaRatio, farValue);
      if (!bakedAreaRatio || Object.keys(bakedAreaRatio).length === 0) {
        setCoverageEditError("Missing current area ratios; please run estimate first.");
        return;
      }
      const nextFloors = roundTo(farValue / resolved, 1);
      if (!Number.isFinite(nextFloors) || nextFloors <= 0) {
        setCoverageEditError("Coverage results in an invalid floors value.");
        return;
      }
      applyInputPatch(
        {
          area_ratio: bakedAreaRatio,
          coverage_ratio: resolved,
          desired_floors_above_ground: nextFloors,
          disable_floors_scaling: true,
          disable_placeholder_area_ratio_scaling: true,
          massing_lock: "coverage",
        } as Partial<ExcelInputs>,
        true,
      );
      setFloorsDraft(String(nextFloors));
      setCoverageDraft(formatPercentDraftFromFraction(resolved, 0));
      setCoverageEditError(null);
      return;
    }

    applyInputPatch(
      {
        coverage_ratio: resolved,
        disable_placeholder_area_ratio_scaling: true,
        massing_lock: "coverage",
      },
      true,
    );
    setCoverageDraft(formatPercentDraftFromFraction(resolved, 0));
    setCoverageEditError(null);
  };

  const commitFloors = () => {
    const resolved = resolveFloorsFromDraft(floorsDraft);
    if (resolved == null) {
      setFloorsEditError("Enter a number greater than 0.");
      return;
    }
    const committedCoverage =
      normalizeCoverageRatio(inputsRef.current?.coverage_ratio ?? null) ?? (effectiveLandUse === "m" ? 0.6 : 0.7);
    const targetFar = resolved * committedCoverage;
    const scaled = resolveScaledAreaRatio(targetFar);
    if (!scaled) {
      setFloorsEditError(t("excel.farEditErrorMissing"));
      return;
    }
    applyInputPatch(
      {
        desired_floors_above_ground: resolved,
        area_ratio: scaled.nextAreaRatio,
        disable_floors_scaling: true,
        disable_placeholder_area_ratio_scaling: true,
        massing_lock: "floors",
      } as Partial<ExcelInputs>,
      true,
    );
    setFloorsDraft(String(resolved));
    setFloorsEditError(null);
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
      setShowLandPriceOverride(true);
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
    setShowCalculations(false);
    setEstimateId(null);
    setScenarioBaseResult(null);
    try {
      const resolvedComponents = currentInputs.components ?? components;
      const excelInputs = {
        ...currentInputs,
        land_use_code: effectiveLandUse,
        components: resolvedComponents,
      };
      const result = await makeEstimate({
        geometry: parcel.geometry,
        excelInputs,
        assetProgram,
        components: resolvedComponents,
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
      setShowCalculations(true);
      const usedAreaRatio = result?.used_inputs?.area_ratio;
      if (usedAreaRatio && Object.keys(usedAreaRatio).length > 0) {
        setOverrides((prev) => {
          const existing = prev.area_ratio as ExcelInputs["area_ratio"] | undefined;
          if (existing && Object.keys(existing).length > 0) return prev;
          if (inputsRef.current?.disable_floors_scaling === true) return prev;
          return applyPatch(prev, {
            area_ratio: usedAreaRatio,
            disable_placeholder_area_ratio_scaling: true,
          });
        });
      }
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
      const scenarioOverrides: Partial<ExcelInputs> = {};
      if (typeof patch.land_price_sar_m2 === "number" && Number.isFinite(patch.land_price_sar_m2)) {
        scenarioOverrides.land_price_sar_m2 = patch.land_price_sar_m2;
      }
      if (typeof patch.far === "number" && Number.isFinite(patch.far)) {
        const baseRatio = resolveAreaRatioBase([
          excelResultRef.current?.used_inputs?.area_ratio,
          inputsRef.current?.area_ratio,
          templateForLandUse(effectiveLandUse).area_ratio,
        ]);
        const scaled = scaleAboveGroundAreaRatio(baseRatio, patch.far);
        if (scaled) {
          scenarioOverrides.area_ratio = scaled.nextAreaRatio;
          (scenarioOverrides as Partial<ExcelInputs> & { disable_floors_scaling?: boolean })
            .disable_floors_scaling = true;
        } else {
          setError(t("excel.farEditErrorMissing"));
        }
      }
      if (Object.keys(scenarioOverrides).length > 0) {
        applyInputPatch(scenarioOverrides, false);
      }
      if (typeof patch.provider === "string" && patch.provider.trim()) {
        setProvider(patch.provider.trim() as (typeof PROVIDERS)[number]["value"]);
      }
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
  const impliedFloors =
    displayedFar != null && coverageRatio > 0 && Number.isFinite(coverageRatio)
      ? displayedFar / coverageRatio
      : null;
  const displayedBuiltArea =
    scenarioAreaRatio != null
      ? {
        ...builtArea,
        residential: (builtArea.residential ?? 0) * scenarioAreaRatio,
        retail: (builtArea.retail ?? 0) * scenarioAreaRatio,
        office: (builtArea.office ?? 0) * scenarioAreaRatio,
        basement: builtArea.basement,
        upper_annex_non_far: builtArea.upper_annex_non_far,
      }
      : builtArea;
  const nla = breakdown.nla || {};
  const directCost = breakdown.direct_cost || {};
  const incomeComponents = breakdown.y1_income_components || {};
  const upperAnnexFlow = breakdown?.revenue_meta?.upper_annex_flow;
  const upperAnnexSink = typeof upperAnnexFlow?.to_key === "string" ? upperAnnexFlow.to_key : "";
  const upperAnnexAreaM2 =
    typeof upperAnnexFlow?.area_m2 === "number" ? upperAnnexFlow.area_m2 : 0;
  const upperAnnexNlaAddedM2 =
    typeof upperAnnexFlow?.nla_added_m2 === "number" ? upperAnnexFlow.nla_added_m2 : 0;
  const showUpperAnnexHint =
    upperAnnexAreaM2 > 1e-6 &&
    (upperAnnexSink === "residential" || upperAnnexSink === "office");
  const fmtM2 = (value: number) =>
    new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(value);
  const upperAnnexHintText = showUpperAnnexHint
    ? `Includes upper annex: +${fmtM2(upperAnnexAreaM2)} m²${
      upperAnnexNlaAddedM2 > 1e-6 ? ` (≈ ${fmtM2(upperAnnexNlaAddedM2)} m² NLA)` : ""
    }`
    : "";
  const explanations =
    (isArabic
      ? breakdown.explanations_ar ?? breakdown.explanations_en ?? breakdown.explanations
      : breakdown.explanations_en ?? breakdown.explanations) || {};

  // Strip internal engine-plumbing notes from any explanation text shown in the UI.
  // These fragments are useful for debugging but reduce trust/clarity for users.
  const stripInternalExplain = (note: unknown): string | null => {
    if (typeof note !== "string") return null;
    const disallowedFragments = [
      "Above-ground FAR adjusted",
      "Floors scaling applied:",
      "Auto-added basement area to meet parking minimums:",
    ];
    const filtered = note
      .split("|")
      .map((part) => part.trim())
      .filter((part) => !disallowedFragments.some((fragment) => part.startsWith(fragment)));
    return filtered.join(" | ");
  };

  const explanationsClean = (() => {
    if (!explanations || typeof explanations !== "object") return {};
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(explanations as Record<string, unknown>)) {
      const cleaned = stripInternalExplain(v);
      if (cleaned) out[k] = cleaned;
    }
    return out;
  })();
  const explanationsDisplay: Record<string, string> = explanationsClean;
  const farNoteBase = (() => {
    const note = explanationsDisplay.effective_far_above_ground;
    if (typeof note !== "string" || !note) return null;
    const disallowedFragments = ["Above-ground FAR adjusted"];
    const filtered = note
      .split("|")
      .map((part) => part.trim())
      .filter((part) => !disallowedFragments.some((fragment) => part.startsWith(fragment)));
    return filtered.join(" | ");
  })();
  const usedInputs = excelResult?.inputs || {};
  const unitCost = usedInputs.unit_cost || {};
  const resolvedUnitCost =
    effectiveLandUse === "m"
      ? {
        ...unitCost,
        upper_annex_non_far: unitCost.upper_annex_non_far ?? 2200,
      }
      : unitCost;
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

  useEffect(() => {
    if (committedFloorsValue != null) {
      setFloorsDraft(String(committedFloorsValue));
      setFloorsEditError(null);
      return;
    }
    if (impliedFloors != null && Number.isFinite(impliedFloors)) {
      setFloorsDraft(String(roundTo(impliedFloors, 1)));
      setFloorsEditError(null);
      return;
    }
    setFloorsDraft("");
    setFloorsEditError(null);
  }, [committedFloorsValue, impliedFloors]);

  useEffect(() => {
    const lock = resolveMassingLock(inputsRef.current?.massing_lock ?? null);
    if (lock !== "coverage" && lock !== "far") return;
    if (isEditingFar) return;
    if (displayedFar == null || !coverageRatio) return;
    const derivedFloors = roundTo(displayedFar / coverageRatio, 1);
    setFloorsDraft(String(derivedFloors));
  }, [coverageRatio, displayedFar, isEditingFar]);

  const buaNote = (key: string) => {
    const noteKey = `${key}_bua`;
    const showScenarioScale = scenarioAreaRatio != null && key !== "basement";
    if (explanationsDisplay[noteKey]) {
      return (
        <>
          <div>{explanationsDisplay[noteKey]}</div>
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
    explanationsDisplay.land_cost ||
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
  const floorsNoteBase =
    "Used to scale above-ground area ratios when FAR is not manually overridden.";
  const floorsDisabledNote = "Skipped because FAR was manually overridden.";
  const farManuallyOverridden =
    inputsRef.current?.disable_floors_scaling === true &&
    resolveMassingLock(inputsRef.current?.massing_lock ?? null) === "far";
  const floorsNote = farManuallyOverridden ? floorsDisabledNote : floorsNoteBase;

  const fitoutNote =
    explanationsDisplay.fitout ||
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
    : explanationsDisplay.contingency ||
      t("excelNotes.contingency", {
        pct: formatPercentValue(contingencyPct),
        direct: formatNumberValue(constructionDirectTotal, 0),
        fitout: formatNumberValue(fitoutTotal, 0),
      });

  const consultantsNote =
    explanationsDisplay.consultants ||
    t("excelNotes.consultants", {
      base: formatNumberValue(consultantsBase, 0),
      pct: formatPercentValue(consultantsPct),
    });

  const transactionNote =
    explanationsDisplay.transaction_cost ||
    t("excelNotes.transaction", {
      land: formatNumberValue(landCostValue, 0),
      pct: formatPercentValue(transactionPct),
    });
  const directNote =
    explanationsDisplay.construction_direct ||
    Object.keys(directCost)
      .map((key) => {
        const area = builtArea[key] ?? 0;
        const costPerUnit = resolvedUnitCost[key] ?? 0;
        return t("excelNotes.directItem", {
          key,
          area: formatNumberValue(area, 0),
          cost: formatNumberValue(costPerUnit, 0),
        });
      })
      .filter(Boolean)
      .join("; ");
  const upperAnnexArea = displayedBuiltArea.upper_annex_non_far;
  const upperAnnexCost = directCost.upper_annex_non_far;
  const upperAnnexUnitCost = resolvedUnitCost.upper_annex_non_far ?? 0;
  const upperAnnexCostNote =
    explanationsDisplay.upper_annex_non_far_cost ||
    (upperAnnexArea != null
      ? `${formatNumberValue(upperAnnexArea, 0)} m² × ${formatNumberValue(upperAnnexUnitCost, 0)} SAR/m².`
      : null);

  const incomeNote = t("excel.year1IncomeNote");

  const parkingIncomeExplanation =
    explanationsDisplay.parking_income ?? null;
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
  const opexDraftValue = resolveOpexPctFromDraft(opexPctDraft);
  const committedOpexPct = Math.max(0, Math.min(inputsRef.current?.opex_pct ?? 0, 1));
  const opexApplyDisabled =
    !includeOpex ||
    opexDraftValue == null ||
    Math.abs(opexDraftValue - committedOpexPct) < 1e-6;
  const coverageDraftValue = resolveCoverageFromDraft(coverageDraft);
  const committedCoverageRatio =
    normalizeCoverageRatio(inputsRef.current?.coverage_ratio ?? null) ?? defaultCoverageRatio;
  const coverageApplyDisabled =
    coverageDraftValue == null || Math.abs(coverageDraftValue - committedCoverageRatio) < 1e-6;
  const floorsDraftValue = resolveFloorsFromDraft(floorsDraft);
  const committedFloorsOverride =
    typeof inputsRef.current?.desired_floors_above_ground === "number" &&
    Number.isFinite(inputsRef.current.desired_floors_above_ground) &&
    inputsRef.current.desired_floors_above_ground > 0
      ? inputsRef.current.desired_floors_above_ground
      : null;
  const floorsApplyDisabled =
    floorsDraftValue == null ||
    (committedFloorsOverride != null && Math.abs(floorsDraftValue - committedFloorsOverride) < 1e-6);
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
    explanationsDisplay.y1_income_effective ||
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

  const toNumericOrNull = (value: unknown) => {
    if (value == null || value === "") return null;
    const numeric = typeof value === "number" ? value : Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  };

  const boolOrNull = (value: unknown): boolean | null => {
    if (typeof value === "boolean") return value;
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      if (["true", "yes", "y", "1", "compliant"].includes(normalized)) return true;
      if (["false", "no", "n", "0", "noncompliant", "not_compliant"].includes(normalized)) return false;
    }
    return null;
  };

  const resolveParking = (result: ExcelResult) => {
    const totals = (result?.totals ?? {}) as Record<string, unknown>;
    const resultNotes = (result?.notes ?? {}) as Record<string, unknown>;
    const breakdown = (resultNotes?.excel_breakdown ?? {}) as Record<string, unknown>;
    const parkingMeta = (resultNotes?.parking ?? {}) as Record<string, unknown>;

    const firstString = (...vals: unknown[]): string | null => {
      for (const v of vals) {
        if (typeof v === "string" && v.trim().length) return v.trim();
      }
      return null;
    };

    const required =
      toNumericOrNull(totals.parking_required_spaces) ??
      toNumericOrNull(breakdown.parking_required_spaces) ??
      toNumericOrNull(parkingMeta.required_spaces_final) ??
      toNumericOrNull(parkingMeta.parking_required_spaces) ??
      null;

    const provided =
      toNumericOrNull(totals.parking_provided_spaces) ??
      toNumericOrNull(breakdown.parking_provided_spaces) ??
      toNumericOrNull(parkingMeta.provided_spaces_final) ??
      toNumericOrNull(parkingMeta.parking_provided_spaces) ??
      null;

    const deficit =
      toNumericOrNull(totals.parking_deficit_spaces) ??
      toNumericOrNull(breakdown.parking_deficit_spaces) ??
      toNumericOrNull(parkingMeta.deficit_spaces_final) ??
      toNumericOrNull(parkingMeta.parking_deficit_spaces) ??
      (required != null && provided != null ? Math.max(0, required - provided) : null);

    const compliant =
      boolOrNull(totals.parking_compliant) ??
      boolOrNull(breakdown.parking_compliant) ??
      boolOrNull(parkingMeta.compliant) ??
      (deficit != null ? deficit === 0 : null);

    const parkingAreaM2 =
      toNumericOrNull(breakdown.parking_area_m2) ??
      toNumericOrNull(parkingMeta.parking_area_m2_final) ??
      toNumericOrNull(parkingMeta.parking_area_m2) ??
      null;

    const policy =
      (typeof parkingMeta.parking_minimum_policy === "string" ? parkingMeta.parking_minimum_policy : null) ??
      (typeof parkingMeta.policy === "string" ? parkingMeta.policy : null);

    const autoAdjustment = firstString(
      parkingMeta.auto_adjustment_note,
      parkingMeta.auto_adjustment,
      parkingMeta.autoAdjustment,
      breakdown.parking_auto_adjustment_note,
      breakdown.auto_adjustment_note,
      resultNotes.parking_auto_adjustment_note,
    );

    const requiredByComponent =
      breakdown.parking_required_by_component && typeof breakdown.parking_required_by_component === "object"
        ? (breakdown.parking_required_by_component as Record<string, unknown>)
        : null;

    const warningCandidates = [
      parkingMeta.warnings,
      parkingMeta.warning,
      resultNotes.parking_warnings,
      breakdown.parking_warnings,
      breakdown.warnings,
    ];
    const warnings = warningCandidates
      .flatMap((candidate) => (Array.isArray(candidate) ? candidate : [candidate]))
      .filter((candidate): candidate is string => typeof candidate === "string" && candidate.trim().length > 0);

    return {
      required,
      provided,
      deficit,
      compliant,
      parkingAreaM2,
      policy,
      autoAdjustment,
      requiredByComponent,
      warnings,
    };
  };

  const resetFarDraft = () => {
    setFarDraft(displayedFar != null ? String(displayedFar) : "");
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
    const scaled = resolveScaledAreaRatio(targetFar);
    if (!scaled) {
      setFarEditError(t("excel.farEditErrorMissing"));
      return;
    }
    applyInputPatch(
      {
        area_ratio: scaled.nextAreaRatio,
        disable_floors_scaling: true,
        disable_placeholder_area_ratio_scaling: true,
        massing_lock: "far",
      } as Partial<ExcelInputs>,
      true,
    );
    setScenarioBaseResult(null);
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
    if (displayedFar == null) return;
    setFarEditError(null);
    setFarDraft(String(displayedFar));
    setIsEditingFar(true);
  };
  const farApplyDisabled =
    farDraft.trim() === "" ||
    !Number.isFinite(Number(farDraft)) ||
    Number(farDraft) <= 0 ||
    (displayedFar != null && Number(farDraft) === Number(displayedFar));
  const revenueItems = Object.keys(incomeComponents || {}).map((key) => {
    const isUpperAnnexSink = showUpperAnnexHint && key === upperAnnexSink;
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
      label: key.replace(/_/g, " "),
      amount: incomeComponents[key] ?? 0,
      note: resolveRevenueNote(key, baseNote, incomeComponents[key] ?? 0),
      upperAnnexHint: isUpperAnnexSink ? upperAnnexHintText : null,
    };
  });
  const includedBadge = <span className="ui-v2-pill">{isArabic ? "مُدرج" : "Included"}</span>;
  const prettifyRevenueKey = (key: string) => key.replace(/_/g, " ");
  const V2InfoTip = ({ label, body }: { label: string; body: string }) => (
    <span className="ui-v2-info">
      <button type="button" className="ui-v2-info__icon" aria-label={label}>i</button>
      <span className="ui-v2-info__tip" role="tooltip">
        <strong>How we calculated:</strong>
        <span>{body}</span>
      </span>
    </span>
  );
  const hasIncludedComponent = (key: string) => {
    const lowerKey = key.toLowerCase();
    if (lowerKey.includes("residential") && componentsDraft.residential) return true;
    if (lowerKey.includes("retail") && componentsDraft.retail) return true;
    if (lowerKey.includes("office") && componentsDraft.office) return true;
    return false;
  };
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
    { key: "upper_annex_non_far", label: t("excel.unitCostUpperAnnexNonFar") },
  ];
  const activeUnitCostFields =
    effectiveLandUse === "m"
      ? unitCostFields
      : unitCostFields.filter((field) => field.key === "residential" || field.key === "basement");
  const totalIncomeByClass =
    (incomeComponents?.residential ?? 0) + (incomeComponents?.retail ?? 0) + (incomeComponents?.office ?? 0);
  const revenueMixItems = [
    { key: "residential", label: t("excel.componentResidential") },
    { key: "retail", label: t("excel.componentRetail") },
    { key: "office", label: t("excel.componentOffice") },
  ].map((item) => {
    const income = Number(incomeComponents?.[item.key] ?? 0);
    const pct = totalIncomeByClass > 0 ? income / totalIncomeByClass : 0;
    return { ...item, pct };
  });
  const fixedAverageUnitSize: Record<"residential" | "retail" | "office", number> = {
    residential: 120,
    retail: 80,
    office: 120,
  };
  const readUnitCountFromSummary = (key: "residential" | "retail" | "office") => {
    const combinedSummary = [notes.summary_en, notes.summary_ar, notes.summary, excelResult?.summary]
      .filter((value): value is string => typeof value === "string")
      .join(" ");
    if (!combinedSummary) return null;
    const patterns: Record<typeof key, RegExp> = {
      residential: /(\d[\d,]*)\s+(?:residential\s+)?apartments?/i,
      retail: /(\d[\d,]*)\s+(?:retail\s+)?units?/i,
      office: /(\d[\d,]*)\s+(?:office\s+)?units?/i,
    };
    const match = combinedSummary.match(patterns[key]);
    if (!match) return null;
    const numeric = Number(match[1].replace(/,/g, ""));
    return Number.isFinite(numeric) ? numeric : null;
  };
  const resolveUnitCount = (key: "residential" | "retail" | "office") => {
    const fromSummary = readUnitCountFromSummary(key);
    if (fromSummary != null) return fromSummary;
    const area = Number(nla?.[key] ?? 0);
    if (!Number.isFinite(area) || area <= 0) return 0;
    return Math.floor(area / fixedAverageUnitSize[key]);
  };
  const unitMixItems = [
    { key: "residential" as const, label: t("excel.componentResidential"), suffix: isArabic ? "شقة" : "Apartments" },
    { key: "retail" as const, label: t("excel.componentRetail"), suffix: isArabic ? "وحدات" : "Units" },
    { key: "office" as const, label: t("excel.componentOffice"), suffix: isArabic ? "وحدات" : "Units" },
  ].map((item) => ({
    ...item,
    count: resolveUnitCount(item.key),
  }));
  const effectiveIncome = excelResult?.costs?.y1_income_effective ?? 0;
  const expenseRatio = effectiveIncome > 0 ? opexCostResolved / effectiveIncome : 0;
  const incomeMargin = effectiveIncome > 0 ? y1NoiResolved / effectiveIncome : 0;
  const totalCapex = excelResult?.costs?.grand_total_capex ?? 0;
  const yieldNoi = totalCapex > 0 ? y1NoiResolved / totalCapex : 0;
  const averageUnitSizeItems = [
    {
      key: "residential",
      label: t("excel.componentResidential"),
      value: fixedAverageUnitSize.residential,
    },
    {
      key: "retail",
      label: t("excel.componentRetail"),
      value: fixedAverageUnitSize.retail,
    },
    {
      key: "office",
      label: t("excel.componentOffice"),
      value: fixedAverageUnitSize.office,
    },
  ];
  const roiBandRaw = (
    notes?.roi_band ??
    notes?.excel_breakdown?.roi_band ??
    notes?.notes?.roi_band ??
    notes?.summary_en ??
    notes?.summary ??
    ""
  )
    .toString()
    .toLowerCase();
  const yieldBand = roiBandRaw.includes("double-digit")
    ? "double-digit"
    : roiBandRaw.includes("mid-single-digit")
      ? "mid-single-digit"
      : roiBandRaw.includes("low-single-digit")
        ? "low-single-digit"
        : roiBandRaw.includes("negative")
          ? "negative"
          : roiBandRaw.includes("uncertain")
            ? "uncertain"
            : "mid-single-digit";
  const yieldBandLabel: Record<string, string> = {
    negative: "Negative",
    "low-single-digit": "Low single digit",
    "mid-single-digit": "Mid single digit",
    "double-digit": "Double digit",
    uncertain: "Uncertain",
  };
  const selectedResultsTab: ResultTab = mode === "v2" ? activeV2Tab : activeCalcTab;

  function V2FinancialSummaryCard(props: { className?: string }) {
    if (!excelResult) return null;
    return (
      <div className={props.className ? `ui2-fin-card ${props.className}` : "ui2-fin-card"}>
        <div className="ui2-fin-card__title atlas-card__title fin-summary__title">{t("excel.financialSummaryTitle")}</div>

        <div className="ui-v2-kv">
          <div className="ui2-fin-row atlas-kv ui-v2-kv__row fin-summary__row">
            <span className="ui2-fin-row__label atlas-kv__label ui-v2-kv__key fin-summary__label">{t("excel.totalCapex")}</span>
            <span className="ui2-fin-row__value atlas-kv__value ui-v2-kv__val fin-summary__value">
              {formatCurrencySAR(excelResult.costs.grand_total_capex)}
            </span>
          </div>
          <div className="ui2-fin-row atlas-kv ui-v2-kv__row fin-summary__row">
            <span className="ui2-fin-row__label atlas-kv__label ui-v2-kv__key fin-summary__label">{t("excel.year1Income")}</span>
            <span className="ui2-fin-row__value atlas-kv__value ui-v2-kv__val fin-summary__value">
              {formatCurrencySAR(excelResult.costs.y1_income)}
            </span>
          </div>
          <div className="ui2-fin-row atlas-kv ui-v2-kv__row fin-summary__row">
            <span className="ui2-fin-row__label atlas-kv__label ui-v2-kv__key fin-summary__label">{t("excel.noiYear1")}</span>
            <span className="ui2-fin-row__value atlas-kv__value ui-v2-kv__val fin-summary__value">
              {formatCurrencySAR(y1NoiResolved)}
            </span>
          </div>

          {/* Match clean UI: ROI + Yield split row */}
          <div className="ui2-fin-split ui-v2-kv__row ui-v2-kv__row--split fin-summary__split">
            <div className="ui2-fin-split__col">
              <span className="ui2-fin-split__label ui-v2-kv__key fin-summary__label">{t("excel.unleveredRoi")}</span>
              <span className="ui2-fin-split__value ui-v2-kv__val fin-summary__value">{formatPercentValue(excelResult.roi)}</span>
            </div>
            <div className="ui2-fin-split__col">
              <span className="ui2-fin-split__label ui-v2-kv__key fin-summary__label">{t("excel.yield")}</span>
              <span className="ui2-fin-split__value ui-v2-kv__val fin-summary__value">{formatPercentValue(yieldNoi, 1)}</span>
            </div>
          </div>
        </div>

        {/* Key ratios (match clean UI right panel) */}
        <div className="calc-key-ratios fin-summary__key-ratios ui2-fin-keyratios">
          <h5 className="fin-summary__section-title">{t("excel.keyRatios")}</h5>
          <div className="calc-key-ratios__group">
            <span>{t("excel.revenueMix")}</span>
            {revenueMixItems.map((item) => (
              <p key={item.key}>
                {item.label} {formatPercentValue(item.pct, 0)}
              </p>
            ))}
          </div>
          <div className="calc-key-ratios__group">
            <span>{t("excel.expenseRatio")}</span>
            <p>{t("excel.expenseRatioValue", { pct: formatPercentValue(expenseRatio, 1), label: t("excel.opex") })}</p>
          </div>
          <div className="calc-key-ratios__group">
            <span>{t("excel.incomeMargin")}</span>
            <p>{t("excel.incomeMarginValue", { pct: formatPercentValue(incomeMargin, 1), label: t("excel.noiMargin") })}</p>
          </div>
          {notes?.income_stability ? (
            <div className="calc-key-ratios__group">
              <span>{t("excel.incomeStability")}</span>
              <p>{notes.income_stability}</p>
            </div>
          ) : null}
        </div>
      </div>
    );
  }

  /**
   * UI v2: summary layout wrapper (cleaner UI).
   * Wrapper-only; it does not change calculation logic.
   */
  function Ui2SummaryLayout(props: {
    header?: ReactNode;
    tabs?: ReactNode;
    leftCards?: ReactNode;
    rightFinancial?: ReactNode;
  }) {
    return (
      <div className="ui2-estimate-shell">
        {props.header ? <div className="estimated-calculations__header ui2-estimates-header">{props.header}</div> : null}
        {props.tabs ? <div className="estimated-calculations__tabs ui2-estimates-tabs">{props.tabs}</div> : null}
        {/* Deprecated for final layout: Summary tab now uses calc-grid right rail for financial summary */}
        <div className="ui2-kpi-grid">{props.leftCards}</div>
      </div>
    );
  }

  return (
    <div>
      <section className={mode === "v2" ? "excel-v2-controls oak-container" : undefined}>
        {mode === "v2" ? (
          <div className="excel-v2-controls__grid">
            <div className="excel-v2-controls__left">
              <div className="excel-v2-controls__row excel-controls-row">
                <div className="excel-v2-field">
                  <Field label={t("excel.providerLabel").replace(/:$/, "")}>
                    <Select
                      className="oak-select"
                      value={provider}
                      onChange={(event) => {
                        const nextProvider = event.target.value as any;
                        setProvider(nextProvider);
                        void trackEvent("ui_change_provider", { meta: { provider: nextProvider } });
                      }}
                      fullWidth
                    >
                      {PROVIDERS.map((item) => (
                        <option key={item.value} value={item.value}>
                          {t(item.labelKey)}
                        </option>
                      ))}
                    </Select>
                  </Field>
                </div>
                <div className="excel-v2-field">
                  <Field label={t("excel.overrideLandUse").replace(/:$/, "")}>
                    <Select
                      className="oak-select"
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
                      fullWidth
                    >
                      <option value="">{t("excel.autoUseParcel")}</option>
                      <option value="s">{t("excel.landUseOption", { code: "s", label: t("app.landUse.residential") })}</option>
                      <option value="m">{t("excel.landUseOption", { code: "m", label: t("app.landUse.mixed") })}</option>
                    </Select>
                  </Field>
                </div>
                {showLandPriceOverride ? (
                  <div className="excel-v2-field excel-v2-field--price">
                    <Field
                      label={t("excel.overrideLandPrice")}
                      hint={
                        suggestedPrice != null
                          ? t("excel.suggestedFromFetch", {
                            price: formatNumberValue(suggestedPrice, 0),
                            provider: providerLabel,
                          })
                          : t("excel.notFetched")
                      }
                    >
                      <Input
                        className="oak-input"
                        type="number"
                        fullWidth
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
                      />
                    </Field>
                  </div>
                ) : (
                  <div />
                )}
                <div className="excel-v2-controls__fetchAction">
                  <Button onClick={fetchPrice} variant="secondary" className="oak-btn oak-btn-secondary">{t("excel.fetchPrice")}</Button>
                </div>
              </div>
              <div className="excel-v2-controls__row excel-v2-controls__row--two">
                <div className="excel-components-row" aria-label="Components">
                  <label className="excel-components-row__item">
                    <input
                      className="excel-components-row__checkbox"
                      type="checkbox"
                      checked={componentsDraft.residential}
                      onChange={() => toggleComponentForMode("residential")}
                      aria-label={t("excel.componentResidential")}
                    />
                    <span className="excel-components-row__label">{t("excel.componentResidential")}</span>
                  </label>
                  <label className="excel-components-row__item">
                    <input
                      className="excel-components-row__checkbox"
                      type="checkbox"
                      checked={componentsDraft.retail}
                      onChange={() => toggleComponentForMode("retail")}
                      aria-label={t("excel.componentRetail")}
                    />
                    <span className="excel-components-row__label">{t("excel.componentRetail")}</span>
                  </label>
                  <label className="excel-components-row__item">
                    <input
                      className="excel-components-row__checkbox"
                      type="checkbox"
                      checked={componentsDraft.office}
                      onChange={() => toggleComponentForMode("office")}
                      aria-label={t("excel.componentOffice")}
                    />
                    <span className="excel-components-row__label">{t("excel.componentOffice")}</span>
                  </label>
                </div>
              </div>
              <div className="excel-v2-controls__actionsRow excel-controls-actions">
                <Button onClick={handleEstimateClick} className="oak-btn oak-btn-primary">{t("excel.calculateEstimate")}</Button>
              </div>
              {fetchError && <span style={{ color: "#b91c1c" }}>{t("common.errorPrefix")} {fetchError}</span>}
            </div>
            <aside className="excel-v2-controls__right">
              <div className="ot-card unit-cost-panel">
                <h3 className="unit-cost-panel__title">{t("excel.unitCostTitleV2")}</h3>
                <div className="unit-cost-panel__list">
                  {activeUnitCostFields.map((field) => (
                    <div key={field.key} className="unit-cost-panel__item">
                      <div className="unit-cost-panel__label">{field.label}</div>
                      <div className="unit-cost-panel__value">
                        {formatNumberValue(unitCostInputs[field.key] ?? 0, 0)} SAR
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </aside>
          </div>
        ) : (
          <>
            <div className="excel-controls-row">
              <div className="excel-controls-row__left">
                <div className="excel-controls-row__grid">
                  <div>
                    <Field label={t("excel.providerLabel").replace(/:$/, "")}>
                      <Select
                        className="oak-select"
                        value={provider}
                        onChange={(event) => {
                          const nextProvider = event.target.value as any;
                          setProvider(nextProvider);
                          void trackEvent("ui_change_provider", { meta: { provider: nextProvider } });
                        }}
                        fullWidth
                      >
                        {PROVIDERS.map((item) => (
                          <option key={item.value} value={item.value}>
                            {t(item.labelKey)}
                          </option>
                        ))}
                      </Select>
                    </Field>
                  </div>
                  <div>
                    <Field label={t("excel.overrideLandUse").replace(/:$/, "")}>
                      <Select
                        className="oak-select"
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
                        fullWidth
                      >
                        <option value="">{t("excel.autoUseParcel")}</option>
                        <option value="s">{t("excel.landUseOption", { code: "s", label: t("app.landUse.residential") })}</option>
                        <option value="m">{t("excel.landUseOption", { code: "m", label: t("app.landUse.mixed") })}</option>
                      </Select>
                    </Field>
                  </div>
                  {showLandPriceOverride ? (
                    <div>
                      <Field
                        label={t("excel.overrideLandPrice")}
                        hint={
                          suggestedPrice != null
                            ? t("excel.suggestedFromFetch", {
                              price: formatNumberValue(suggestedPrice, 0),
                              provider: providerLabel,
                            })
                            : t("excel.notFetched")
                        }
                      >
                        <Input
                          className="oak-input"
                          type="number"
                          fullWidth
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
                        />
                      </Field>
                    </div>
                  ) : null}
                </div>
                <div className="excel-controls-row__actions">
                  <Button onClick={fetchPrice} variant="secondary" className="oak-btn oak-btn-secondary">{t("excel.fetchPrice")}</Button>
                  <Checkbox
                    label={t("excel.componentResidential")}
                    checked={componentsDraft.residential}
                    onChange={() => toggleComponentForMode("residential")}
                  />
                  <Checkbox
                    label={t("excel.componentRetail")}
                    checked={componentsDraft.retail}
                    onChange={() => toggleComponentForMode("retail")}
                  />
                  <Checkbox
                    label={t("excel.componentOffice")}
                    checked={componentsDraft.office}
                    onChange={() => toggleComponentForMode("office")}
                  />
                  <Button type="button" onClick={applyComponents} disabled={!componentsDirty} variant="secondary">{t("common.apply")}</Button>
                  <Button onClick={handleEstimateClick} className="oak-btn oak-btn-primary">{t("excel.calculateEstimate")}</Button>
                  <span className="excel-controls-row__status">
                    {t("excel.activeTemplate")} <strong>{effectiveLandUse}</strong>
                  </span>
                  {price != null && (
                    <span className="excel-controls-row__status">
                      {t("excel.suggestedPrice", {
                        price: formatNumberValue(price, 0),
                        provider: providerLabel,
                      })}
                    </span>
                  )}
                  {fetchError && <span style={{ color: "#b91c1c" }}>{t("common.errorPrefix")} {fetchError}</span>}
                </div>
              </div>
            </div>
            <aside className="ot-card unit-cost-panel atlas-card">
              <h3 className="unit-cost-panel__title atlas-card__title">{t("excel.unitCostTitle")}</h3>
              <div className="unit-cost-panel__list">
                {activeUnitCostFields.map((field) => (
                  <div key={field.key} className="unit-cost-panel__item atlas-kv">
                    <div className="unit-cost-panel__label atlas-kv__label">{field.label}</div>
                    <div className="unit-cost-panel__value atlas-kv__value">
                      {formatNumberValue(unitCostInputs[field.key] ?? 0, 0)} SAR
                    </div>
                  </div>
                ))}
              </div>
              {showCalculations && excelResult && (
                <div style={{ marginTop: 12 }}>
                  <EstimateCalculationsPanel estimate={excelResult} />
                </div>
              )}
            </aside>
          </>
        )}
      </section>


      {error && (
        <div style={{ marginTop: 12, color: "#fca5a5" }}>
          {t("common.errorPrefix")} {error}
        </div>
      )}

      {showCalculations && excelResult && (
        <section className={mode === "v2" ? "calc-section ui-v2-results calc-shell" : "calc-section"}>
          {mode === "v2" ? (
            <>
              <div className="ui-v2-results__titlebar calc-shell__header">Estimated Calculations</div>
              <div className="ui-v2-results__tabs calc-shell__tabs calc-tabs" role="tablist" aria-label="Estimated calculations tabs">
                <button
                  type="button"
                  role="tab"
                  className={`ui-v2-results__tab calc-tab ${activeV2Tab === "summary" ? "calc-tab--active" : ""}`}
                  data-active={activeV2Tab === "summary"}
                  aria-selected={activeV2Tab === "summary"}
                  onClick={() => setActiveV2Tab("summary")}
                >
                  {isArabic ? "الملخص" : "Summary"}
                </button>
                <button
                  type="button"
                  role="tab"
                  className={`ui-v2-results__tab calc-tab ${activeV2Tab === "financial" ? "calc-tab--active" : ""}`}
                  data-active={activeV2Tab === "financial"}
                  aria-selected={activeV2Tab === "financial"}
                  onClick={() => setActiveV2Tab("financial")}
                >
                  {t("excel.financialBreakdown")}
                </button>
                <button
                  type="button"
                  role="tab"
                  className={`ui-v2-results__tab calc-tab ${activeV2Tab === "revenue" ? "calc-tab--active" : ""}`}
                  data-active={activeV2Tab === "revenue"}
                  aria-selected={activeV2Tab === "revenue"}
                  onClick={() => setActiveV2Tab("revenue")}
                >
                  {t("excel.revenueBreakdown")}
                </button>
                <button
                  type="button"
                  role="tab"
                  className={`ui-v2-results__tab calc-tab ${activeV2Tab === "parking" ? "calc-tab--active" : ""}`}
                  data-active={activeV2Tab === "parking"}
                  aria-selected={activeV2Tab === "parking"}
                  onClick={() => setActiveV2Tab("parking")}
                >
                  {t("parking.title")}
                </button>
              </div>
            </>
          ) : (
            <>
              <h3 className="calc-header oak-section-bar">{t("excel.calculationsTitle")}</h3>
              <Tabs
                items={[
                  { id: "summary", label: isArabic ? "الملخص" : "Summary" },
                  { id: "financial", label: t("excel.financialBreakdown") },
                  { id: "revenue", label: t("excel.revenueBreakdown") },
                  { id: "parking", label: t("parking.title") },
                ]}
                value={activeCalcTab}
                onChange={(id) => setActiveCalcTab(id as ResultTab)}
              />
            </>
          )}

          <div className={mode === "v2" ? "calc-grid ui-v2-results__panel" : "calc-grid"} role={mode === "v2" ? "tabpanel" : undefined}>
            {mode === "v2" && selectedResultsTab === "summary" ? (
              <>
                {/* LEFT column: KPI cards */}
                <div className="ui-v2-resultsCard calc-grid__left">
                  <div className="ui2-kpi-grid">
                    <div className="ui2-card atlas-summary-card atlas-card ui-v2-summary-card ui-v2-card ui-v2-card--elevated">
                      <div className="ui2-card__title atlas-card__title ui-v2-summary-card__title">{isArabic ? "توزيع الوحدات" : "Unit Mix"}</div>
                      <div className="ui-v2-metric-grid" role="list" aria-label={isArabic ? "توزيع الوحدات" : "Unit mix"}>
                        {unitMixItems.map((item) => (
                          <div key={item.key} className="ui-v2-metric-grid__item" role="listitem">
                            <div className="ui-v2-metric-grid__value">{formatNumberValue(item.count, 0)}</div>
                            <div className="ui-v2-metric-grid__label">{item.label}</div>
                            <div className="ui-v2-metric-grid__sub">{item.suffix}</div>
                          </div>
                        ))}
                      </div>
                    </div>

                    <div className="ui2-card atlas-summary-card atlas-card ui-v2-summary-card ui-v2-card ui-v2-card--elevated">
                      <div className="ui2-card__title atlas-card__title ui-v2-summary-card__title">{isArabic ? "متوسط مساحة الوحدة" : "Average Unit Size"}</div>
                      <div className="ui-v2-metric-grid" role="list" aria-label={isArabic ? "متوسط مساحة الوحدة" : "Average unit size"}>
                        {averageUnitSizeItems.map((item) => (
                          <div key={item.key} className="ui-v2-metric-grid__item" role="listitem">
                            <div className="ui-v2-metric-grid__value">{formatNumberValue(item.value, 0)} m²</div>
                            <div className="ui-v2-metric-grid__label">{item.label}</div>
                          </div>
                        ))}
                      </div>
                    </div>

                    <div className="ui2-card atlas-summary-card atlas-card ui-v2-summary-card ui-v2-card ui-v2-card--elevated">
                      <div className="ui2-card__title atlas-card__title ui-v2-summary-card__title">{isArabic ? "العائد" : "Yield"}</div>
                      <div className="atlas-yield-main ui2-card__value">{yieldBandLabel[yieldBand]}</div>
                      <div className="atlas-yield-helper ui2-card__sub">Unlevered Year 1 yield on cost</div>
                    </div>
                  </div>
                </div>

                {/* RIGHT column: Financial Summary (always on the right like clean UI) */}
                <div className="oak-right-panel calc-grid__right ui2-right-rail">
                  <V2FinancialSummaryCard className="ui2-right-rail__card" />
                </div>
              </>
            ) : (
              <Card className={mode === "v2" ? "ui-v2-resultsCard calc-grid__left" : undefined}>
                {selectedResultsTab === "summary" && (
                <div className="oak-card">
                  <h4 className="oak-card-title">{t("excel.financialSummaryTitle")}</h4>
                  <div className="oak-stats">
                    <div className="oak-stat">
                      <div className="oak-stat-value">{formatCurrencySAR(excelResult.costs.land_cost)}</div>
                      <div className="oak-stat-label">{t("excel.landCost")}</div>
                    </div>
                    <div className="oak-stat">
                      <div className="oak-stat-value">{formatCurrencySAR(excelResult.costs.grand_total_capex)}</div>
                      <div className="oak-stat-label">{t("excel.totalCapex")}</div>
                    </div>
                    <div className="oak-stat">
                      <div className="oak-stat-value">{formatPercentValue(excelResult.roi)}</div>
                      <div className="oak-stat-label">{t("excel.unleveredRoi")}</div>
                    </div>
                  </div>
                </div>
                )}
              {selectedResultsTab === "financial" && (
                mode === "v2" ? (
                  <div>
                    <h4 className="ui-v2-sectionTitle">{t("excel.costBreakdown")}</h4>
                    {(() => {
                      const directConstruction = excelResult.costs.construction_direct_cost ?? 0;
                      const fitoutCost = excelResult.costs.fitout_cost ?? 0;
                      const contingencyCost = excelResult.costs.contingency_cost ?? 0;
                      const consultantsCost = excelResult.costs.consultants_cost ?? 0;
                      const feasibilityFee = excelResult.costs.feasibility_fee ?? 0;
                      const landCostAmount = excelResult.costs.land_cost ?? 0;
                      const transactionCost = excelResult.costs.transaction_cost ?? 0;
                      const totalCapex = excelResult.costs.grand_total_capex ?? 0;
                      const v2Costs = excelResult.costs as Record<string, number | undefined>;
                      const hasDirectConstruction = typeof v2Costs.construction_direct_cost === "number";
                      const hasFitoutConstruction = typeof v2Costs.fitout_cost === "number";
                      const constructionCostAmount =
                        v2Costs.construction_cost ??
                        (hasDirectConstruction || hasFitoutConstruction
                          ? (v2Costs.construction_direct_cost ?? 0) + (v2Costs.fitout_cost ?? 0)
                          : undefined) ??
                        directConstruction;
                      const notesExcelBreakdown = (excelResult.notes?.excel_breakdown || {}) as Record<string, any>;
                      const breakdownBua = (notesExcelBreakdown?.bua || {}) as Record<string, any>;
                      const breakdownBuiltUpAreas = (notesExcelBreakdown?.built_up_areas || {}) as Record<string, any>;
                      const totalsSource = (excelResult.totals || {}) as Record<string, any>;
                      const constructionSubtotal =
                        v2Costs.sub_total ??
                        v2Costs.construction_subtotal ??
                        (directConstruction + fitoutCost);
                      const constructionTotal = constructionSubtotal;
                      const softTotal = contingencyCost + consultantsCost + feasibilityFee;
                      const landTotal = landCostAmount + transactionCost;
                      const costBreakdownTitle = isArabic ? "تفصيل التكاليف" : "Cost Breakdown";
                      const builtUpAreasTitle = isArabic ? "المساحات المبنية" : "Built Up Areas";
                      const landAndConstructionTitle = isArabic ? "تكلفة الأرض والبناء" : "Land and Construction Cost";
                      const additionalCostsTitle = isArabic ? "تكاليف إضافية" : "Additional Costs";
                      const subtotalLabel = isArabic ? "المجموع الفرعي" : "Subtotal";
                      const directConstructionLabel = i18n.exists("excel.directConstruction")
                        ? t("excel.directConstruction")
                        : isArabic
                          ? "التنفيذ المباشر"
                          : "Direct construction";
                      const fitoutLabel = i18n.exists("excel.fitout")
                        ? t("excel.fitout")
                        : isArabic
                          ? "التشطيب"
                          : "Fit-out";
                      const feasibilityFeeLabel = i18n.exists("excel.feasibilityFee")
                        ? t("excel.feasibilityFee")
                        : isArabic
                          ? "رسوم دراسة الجدوى"
                          : "Feasibility fee";
                      const toNumericOrNullValue = (value: unknown) => {
                        if (value == null || value === "") return null;
                        const numeric = typeof value === "number" ? value : Number(value);
                        return Number.isFinite(numeric) ? numeric : null;
                      };
                      const resolveBuiltUpArea = (...values: unknown[]) => {
                        for (const value of values) {
                          const numeric = toNumericOrNullValue(value);
                          if (numeric != null) return numeric;
                        }
                        return null;
                      };
                      const builtUpRows = [
                        {
                          key: "residential",
                          label: "Residential BUA",
                          value: resolveBuiltUpArea(
                            notesExcelBreakdown.bua_residential_m2,
                            breakdownBua.residential,
                            breakdownBuiltUpAreas.residential,
                            totalsSource.bua_residential_m2,
                          ),
                        },
                        {
                          key: "retail",
                          label: "Retail BUA",
                          value: resolveBuiltUpArea(
                            notesExcelBreakdown.bua_retail_m2,
                            breakdownBua.retail,
                            breakdownBuiltUpAreas.retail,
                            totalsSource.bua_retail_m2,
                          ),
                        },
                        {
                          key: "office",
                          label: "Office BUA",
                          value: resolveBuiltUpArea(
                            notesExcelBreakdown.bua_office_m2,
                            breakdownBua.office,
                            breakdownBuiltUpAreas.office,
                            totalsSource.bua_office_m2,
                          ),
                        },
                        {
                          key: "upperAnnex",
                          label: "Upper annex non FAR +0.5 floor",
                          value: resolveBuiltUpArea(
                            notesExcelBreakdown.bua_upper_annex_non_far_m2,
                            breakdownBua.upper_annex_non_far,
                            breakdownBuiltUpAreas.upper_annex_non_far,
                            totalsSource.bua_upper_annex_non_far_m2,
                          ),
                        },
                        {
                          key: "basement",
                          label: "Basement BUA",
                          value: resolveBuiltUpArea(
                            notesExcelBreakdown.bua_basement_m2,
                            breakdownBua.basement,
                            breakdownBuiltUpAreas.basement,
                            totalsSource.bua_basement_m2,
                          ),
                        },
                      ];
                      const farAboveGround = toNumericOrNullValue(notesExcelBreakdown?.far_above_ground);
                      const builtUpTooltipBody = farAboveGround != null
                        ? `Above-ground FAR = Σ(area ratios excluding basement) = ${formatNumberValue(farAboveGround, 2)}. Tap FAR to edit, then Apply.`
                        : "Above-ground FAR = Σ(area ratios excluding basement). Tap FAR to edit, then Apply.";

                      return (
                        <div className="ui-v2-accordionGroup">
                          <div>
                            <button
                              type="button"
                              className="ui-v2-accHead"
                              data-open={v2FinancialOpen.costBreakdown ? "true" : "false"}
                              onClick={() =>
                                setV2FinancialOpen((prev) => ({ ...prev, costBreakdown: !prev.costBreakdown }))
                              }
                            >
                              {v2FinancialOpen.costBreakdown ? (
                                <ChevronDownIcon className="ui-v2-accordion__chev" />
                              ) : (
                                <ChevronRightIcon className="ui-v2-accordion__chev" />
                              )}
                              <span className="ui-v2-accordion__title">{costBreakdownTitle}</span>
                              <span className="ui-v2-accordion__total">{formatCurrencySAR(totalCapex)}</span>
                            </button>
                            {v2FinancialOpen.costBreakdown && (
                              <div className="ui-v2-accordion__body">
                                <div className="ui-v2-rowList">
                                  <div className="ui-v2-row">
                                    <span className="ui-v2-row__label">Coverage</span>
                                    <span className="ui-v2-row__val ui-v2-costRow__controls">
                                      <Input
                                        type="number"
                                        size="sm"
                                        min={0}
                                        max={100}
                                        step="0.1"
                                        value={coverageDraft}
                                        onChange={(event) => {
                                          setCoverageDraft(event.target.value);
                                          if (coverageEditError) setCoverageEditError(null);
                                        }}
                                        onKeyDown={(event) => {
                                          if (event.key === "Enter") {
                                            event.preventDefault();
                                            commitCoverage();
                                          }
                                        }}
                                        aria-label="Coverage"
                                        className="ui-v2-costInput"
                                      />
                                      <span className="ui-v2-chip">%</span>
                                      <Button type="button" size="sm" variant="secondary" onClick={commitCoverage}>
                                        {t("excel.apply")}
                                      </Button>
                                    </span>
                                    <V2InfoTip label="Coverage info" body="Coverage affects built-up area allocation and parking." />
                                  </div>
                                  {coverageEditError && (
                                    <div className="ui-v2-costRow__error">
                                      {coverageEditError}
                                    </div>
                                  )}

                                  <div className="ui-v2-row">
                                    <span className="ui-v2-row__label">Effective FAR above ground</span>
                                    <span className="ui-v2-row__val ui-v2-costRow__controls">
                                      <Input
                                        type="number"
                                        size="sm"
                                        min={0.1}
                                        step="0.01"
                                        value={farDraft.trim() === "" && displayedFar != null ? String(displayedFar) : farDraft}
                                        onChange={(event) => {
                                          setFarDraft(event.target.value);
                                          if (farEditError) setFarEditError(null);
                                        }}
                                        onKeyDown={(event) => {
                                          if (event.key === "Enter") {
                                            event.preventDefault();
                                            applyFarEdit();
                                          }
                                          if (event.key === "Escape") {
                                            event.preventDefault();
                                            resetFarDraft();
                                          }
                                        }}
                                        aria-label="Effective FAR above ground"
                                        className="ui-v2-costInput"
                                      />
                                      <Button type="button" size="sm" variant="secondary" onClick={applyFarEdit} disabled={farApplyDisabled}>
                                        {t("excel.apply")}
                                      </Button>
                                    </span>
                                    <V2InfoTip label="Effective FAR info" body={builtUpTooltipBody} />
                                  </div>
                                  {farEditError && (
                                    <div className="ui-v2-costRow__error">
                                      {farEditError}
                                    </div>
                                  )}

                                  <div className="ui-v2-row">
                                    <span className="ui-v2-row__label">Implied floors</span>
                                    <span className="ui-v2-row__val">
                                      {(() => {
                                        const impliedFloorsV2 =
                                          toNumericOrNullValue(notesExcelBreakdown?.implied_floors) ??
                                          toNumericOrNullValue(totalsSource?.implied_floors);
                                        return impliedFloorsV2 != null ? formatNumberValue(impliedFloorsV2, 1) : "—";
                                      })()}
                                    </span>
                                    <V2InfoTip label="Implied floors info" body="Implied floors derived from FAR and coverage." />
                                  </div>

                                  <div className="ui-v2-row">
                                    <span className="ui-v2-row__label">Floors above ground</span>
                                    <span className="ui-v2-row__val ui-v2-costRow__controls">
                                      <Input
                                        type="number"
                                        size="sm"
                                        min={0.1}
                                        step="0.1"
                                        value={floorsDraft}
                                        onChange={(event) => {
                                          setFloorsDraft(event.target.value);
                                          if (floorsEditError) setFloorsEditError(null);
                                        }}
                                        onKeyDown={(event) => {
                                          if (event.key === "Enter") {
                                            event.preventDefault();
                                            commitFloors();
                                          }
                                        }}
                                        aria-label="Floors above ground"
                                        className="ui-v2-costInput"
                                      />
                                      <Button type="button" size="sm" variant="secondary" onClick={commitFloors} disabled={floorsApplyDisabled}>
                                        {t("excel.apply")}
                                      </Button>
                                    </span>
                                    <V2InfoTip label="Floors above ground info" body="Floors influence the effective FAR when scaling is enabled." />
                                  </div>
                                  {floorsEditError && (
                                    <div className="ui-v2-costRow__error">
                                      {floorsEditError}
                                    </div>
                                  )}

                                  <div className="ui-v2-row">
                                    <span className="ui-v2-row__label">Massing locks</span>
                                    <span className="ui-v2-row__val ui-v2-costRow__controls ui-v2-costRow__controls--radios">
                                      <label className="ui-v2-radio">
                                        <input
                                          type="radio"
                                          name="v2-massing-lock"
                                          checked={massingLock === "floors"}
                                          onChange={() => applyInputPatch({ massing_lock: "floors" }, true)}
                                        />
                                        <span>Lock Floors</span>
                                      </label>
                                      <label className="ui-v2-radio">
                                        <input
                                          type="radio"
                                          name="v2-massing-lock"
                                          checked={massingLock === "far"}
                                          onChange={() => applyInputPatch({ massing_lock: "far" }, true)}
                                        />
                                        <span>Lock FAR</span>
                                      </label>
                                      <label className="ui-v2-radio">
                                        <input
                                          type="radio"
                                          name="v2-massing-lock"
                                          checked={massingLock === "coverage"}
                                          onChange={() => applyInputPatch({ massing_lock: "coverage" }, true)}
                                        />
                                        <span>Lock Coverage</span>
                                      </label>
                                    </span>
                                    <V2InfoTip label="Massing locks info" body="Choose which parameter stays fixed when adjusting others." />
                                  </div>
                                </div>
                              </div>
                            )}
                          </div>

                          <div>
                            <button
                              type="button"
                              className="ui-v2-accHead"
                              data-open={v2FinancialOpen.builtUpAreas ? "true" : "false"}
                              onClick={() =>
                                setV2FinancialOpen((prev) => ({ ...prev, builtUpAreas: !prev.builtUpAreas }))
                              }
                            >
                              {v2FinancialOpen.builtUpAreas ? (
                                <ChevronDownIcon className="ui-v2-accordion__chev" />
                              ) : (
                                <ChevronRightIcon className="ui-v2-accordion__chev" />
                              )}
                              <span className="ui-v2-accordion__title">{builtUpAreasTitle}</span>
                            </button>
                            {v2FinancialOpen.builtUpAreas && (
                              <div className="ui-v2-accordion__body">
                                <div className="ui-v2-rowList">
                                  {builtUpRows.map((row) => (
                                    <div className="ui-v2-row" key={row.key}>
                                      <span className="ui-v2-row__label">{row.label}</span>
                                      <span className="ui-v2-row__val">
                                        {row.value != null ? formatAreaM2(row.value) : "—"}
                                      </span>
                                      <V2InfoTip label={`Info for ${row.label}`} body={builtUpTooltipBody} />
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}
                          </div>

                          <div>
                            <button
                              type="button"
                              className="ui-v2-accHead"
                              data-open={v2FinancialOpen.landAndConstruction ? "true" : "false"}
                              onClick={() =>
                                setV2FinancialOpen((prev) => ({ ...prev, landAndConstruction: !prev.landAndConstruction }))
                              }
                            >
                              {v2FinancialOpen.landAndConstruction ? (
                                <ChevronDownIcon className="ui-v2-accordion__chev" />
                              ) : (
                                <ChevronRightIcon className="ui-v2-accordion__chev" />
                              )}
                              <span className="ui-v2-accordion__title">{landAndConstructionTitle}</span>
                              <span className="ui-v2-accordion__total">{formatCurrencySAR(landTotal + constructionTotal)}</span>
                            </button>
                            {v2FinancialOpen.landAndConstruction && (
                              <div className="ui-v2-accordion__body">
                                <div className="ui-v2-kv2">
                                  <div className="ui-v2-kv2__row">
                                    <span className="ui-v2-kv2__key">{t("excel.landCost")}</span>
                                    <span className="ui-v2-kv2__val">{formatCurrencySAR(landCostAmount)}</span>
                                    <V2InfoTip
                                      label={`${t("excel.landCost")} info`}
                                      body="Land cost = site area × price per m² (+ adjustments)."
                                    />
                                  </div>
                                  <div className="ui-v2-kv2__row">
                                    <span className="ui-v2-kv2__key">Construction</span>
                                    <span className="ui-v2-kv2__val">{formatCurrencySAR(constructionCostAmount)}</span>
                                    <V2InfoTip
                                      label="Construction info"
                                      body="Construction cost derived from unit costs × built-up area."
                                    />
                                  </div>
                                  <div className="ui-v2-kv2__row">
                                    <span className="ui-v2-kv2__key">{directConstructionLabel}</span>
                                    <span className="ui-v2-kv2__val">{formatCurrencySAR(directConstruction)}</span>
                                    <V2InfoTip
                                      label={`${directConstructionLabel} info`}
                                      body="Construction cost derived from unit costs × built-up area."
                                    />
                                  </div>
                                  <div className="ui-v2-kv2__row">
                                    <span className="ui-v2-kv2__key">{fitoutLabel}</span>
                                    <span className="ui-v2-kv2__val">{formatCurrencySAR(fitoutCost)}</span>
                                    <V2InfoTip label={`${fitoutLabel} info`} body="Additional cost line item included in CapEx." />
                                  </div>
                                  <div className="ui-v2-kv2__row">
                                    <span className="ui-v2-kv2__key">{t("excel.transactionCosts")}</span>
                                    <span className="ui-v2-kv2__val">{formatCurrencySAR(transactionCost)}</span>
                                    <V2InfoTip
                                      label={`${t("excel.transactionCosts")} info`}
                                      body="Additional cost line item included in CapEx."
                                    />
                                  </div>
                                </div>
                              </div>
                            )}
                          </div>

                          <div>
                            <button
                              type="button"
                              className="ui-v2-accHead"
                              data-open={v2FinancialOpen.additionalCosts ? "true" : "false"}
                              onClick={() =>
                                setV2FinancialOpen((prev) => ({ ...prev, additionalCosts: !prev.additionalCosts }))
                              }
                            >
                              {v2FinancialOpen.additionalCosts ? (
                                <ChevronDownIcon className="ui-v2-accordion__chev" />
                              ) : (
                                <ChevronRightIcon className="ui-v2-accordion__chev" />
                              )}
                              <span className="ui-v2-accordion__title">{additionalCostsTitle}</span>
                              <span className="ui-v2-accordion__total">{formatCurrencySAR(softTotal)}</span>
                            </button>
                            {v2FinancialOpen.additionalCosts && (
                              <div className="ui-v2-accordion__body">
                                <div className="ui-v2-kv2">
                                  <div className="ui-v2-kv2__row">
                                    <span className="ui-v2-kv2__key">{t("excel.contingency")}</span>
                                    <span className="ui-v2-kv2__val">{formatCurrencySAR(contingencyCost)}</span>
                                    <V2InfoTip label={`${t("excel.contingency")} info`} body="Additional cost line item included in CapEx." />
                                  </div>
                                  <div className="ui-v2-kv2__row">
                                    <span className="ui-v2-kv2__key">{t("excel.consultants")}</span>
                                    <span className="ui-v2-kv2__val">{formatCurrencySAR(consultantsCost)}</span>
                                    <V2InfoTip label={`${t("excel.consultants")} info`} body="Additional cost line item included in CapEx." />
                                  </div>
                                  <div className="ui-v2-kv2__row">
                                    <span className="ui-v2-kv2__key">{feasibilityFeeLabel}</span>
                                    <span className="ui-v2-kv2__val">{formatCurrencySAR(feasibilityFee)}</span>
                                    <V2InfoTip label={`${feasibilityFeeLabel} info`} body="Additional cost line item included in CapEx." />
                                  </div>
                                </div>
                              </div>
                            )}
                          </div>

                          <div className="ui-v2-totalRow">
                            <span className="ui-v2-totalRow__key">{t("excel.totalCapex")}</span>
                            <span className="ui-v2-totalRow__val">{formatCurrencySAR(totalCapex)}</span>
                          </div>
                        </div>
                      );
                    })()}
                  </div>
                ) : (
            <div>
              <h4 style={{ marginTop: 0, marginBottom: "0.5rem" }}>{t("excel.costBreakdown")}</h4>
              <Table>
                <thead>
                  <tr>
                    <th className="col-item">{t("excel.item")}</th>
                    <th className="col-num">{t("excel.amount")}</th>
                    <th className="col-calc">{t("excel.calculation")}</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="col-item">Coverage</td>
                    <td className="col-num">
                      <div className="calc-inline-controls">
                        <Input
                          type="number"
                          size="sm"
                          min={0.1}
                          max={100}
                          step="0.1"
                          value={coverageDraft}
                          onChange={(event) => {
                            setCoverageDraft(event.target.value);
                            if (coverageEditError) {
                              setCoverageEditError(null);
                            }
                          }}
                          onKeyDown={(event) => {
                            if (event.key === "Enter") {
                              event.preventDefault();
                              commitCoverage();
                            }
                          }}
                          className="calc-input-coverage"
                          aria-label="Coverage ratio"
                        />
                        <span>%</span>
                        <Button type="button" size="sm" variant="secondary" onClick={commitCoverage} disabled={coverageApplyDisabled}>
                          {t("common.apply")}
                        </Button>
                      </div>
                    </td>
                    <td className="col-calc">
                      <div>Used with FAR to infer above-ground floors.</div>
                      {coverageEditError && <div className="calc-error">{coverageEditError}</div>}
                    </td>
                  </tr>
                  {farAboveGround != null && (
                    <tr>
                      <td className="col-item">{t("excel.effectiveFar")}</td>
                      <td className="col-num">
                        {isEditingFar ? (
                          <div className="calc-inline-controls">
                            <Input
                              type="number"
                              size="sm"
                              step="0.01"
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
                              className="calc-input-far"
                              autoFocus
                            />
                            <Button type="button" size="sm" variant="secondary" onClick={applyFarEdit} disabled={farApplyDisabled}>
                              {t("excel.apply")}
                            </Button>
                            <Button type="button" size="sm" variant="ghost" onClick={cancelFarEdit}>
                              {t("excel.cancel")}
                            </Button>
                          </div>
                        ) : (
                          <Button type="button" size="sm" variant="ghost" onClick={startFarEdit} title={t("excel.farEditHint")}>
                            <span>{formatNumberValue(displayedFar, 3)}</span>
                            <span style={{ fontSize: "0.8rem", opacity: 0.85 }}>{t("excel.farEdit")}</span>
                          </Button>
                        )}
                      </td>
                      <td className="col-calc">
                        {farNote}
                        {!isEditingFar && (
                          <div style={{ marginTop: 6 }}>{t("excel.farEditHintInline")}</div>
                        )}
                        {farEditError && <div className="calc-error">{farEditError}</div>}
                      </td>
                    </tr>
                  )}
                  <tr>
                    <td className="col-item">Implied floors</td>
                    <td className="col-num">
                      {impliedFloors != null && Number.isFinite(impliedFloors)
                        ? formatNumberValue(impliedFloors, 1)
                        : "—"}
                    </td>
                    <td className="col-calc">FAR ÷ coverage.</td>
                  </tr>
                  <tr>
                    <td className="col-item">Floors (above-ground)</td>
                    <td className="col-num">
                      <div className="calc-inline-controls">
                        <Input
                          type="number"
                          size="sm"
                          min={0.1}
                          step="0.1"
                          value={floorsDraft}
                          onChange={(event) => {
                            setFloorsDraft(event.target.value);
                            if (floorsEditError) {
                              setFloorsEditError(null);
                            }
                          }}
                          onKeyDown={(event) => {
                            if (event.key === "Enter") {
                              event.preventDefault();
                              commitFloors();
                            }
                          }}
                          className="calc-input-floors"
                          aria-label="Floors above ground"
                        />
                        <Button type="button" size="sm" variant="secondary" onClick={commitFloors} disabled={floorsApplyDisabled}>
                          {t("common.apply")}
                        </Button>
                      </div>
                    </td>
                    <td className="col-calc">
                      {floorsNote}
                      {floorsEditError && <div className="calc-error">{floorsEditError}</div>}
                    </td>
                  </tr>
                  <tr>
                    <td className="col-item">Massing locks</td>
                    <td className="col-num">
                      <div className="calc-inline-controls">
                        <Radio
                          name="massing-lock"
                          checked={massingLock === "floors"}
                          onChange={() => applyInputPatch({ massing_lock: "floors" })}
                          label="Lock floors"
                        />
                        <Radio
                          name="massing-lock"
                          checked={massingLock === "far"}
                          onChange={() => applyInputPatch({ massing_lock: "far" })}
                          label="Lock FAR"
                        />
                        <Radio
                          name="massing-lock"
                          checked={massingLock === "coverage"}
                          onChange={() => applyInputPatch({ massing_lock: "coverage" })}
                          label="Lock coverage"
                        />
                      </div>
                    </td>
                    <td className="col-calc">Choose a single driver for massing updates.</td>
                  </tr>
                  {components.residential && (
                    <tr>
                      <td className="col-item">{t("excel.residentialBua")}</td>
                      <td className="col-num">{formatArea(displayedBuiltArea.residential)}</td>
                      <td className="col-calc">{buaNote("residential")}</td>
                    </tr>
                  )}
                  {effectiveLandUse === "m" && builtArea.retail !== undefined && (
                    <tr>
                      <td className="col-item">{t("excel.retailBua")}</td>
                      <td className="col-num">{formatArea(displayedBuiltArea.retail)}</td>
                      <td className="col-calc">{buaNote("retail")}</td>
                    </tr>
                  )}
                  {effectiveLandUse === "m" && builtArea.office !== undefined && (
                    <tr>
                      <td className="col-item">{t("excel.officeBua")}</td>
                      <td className="col-num">{formatArea(displayedBuiltArea.office)}</td>
                      <td className="col-calc">{buaNote("office")}</td>
                    </tr>
                  )}
                  {effectiveLandUse === "m" && upperAnnexArea != null && upperAnnexArea > 0 && (
                    <tr>
                      <td className="col-item">{t("excel.upperAnnexNonFarBua")}</td>
                      <td className="col-num">{formatArea(upperAnnexArea)}</td>
                      <td className="col-calc">{explanationsDisplay.upper_annex_non_far_bua}</td>
                    </tr>
                  )}
                  <tr>
                    <td className="col-item">{t("excel.basementBua")}</td>
                    <td className="col-num">{formatArea(displayedBuiltArea.basement)}</td>
                    <td className="col-calc">{buaNote("basement")}</td>
                  </tr>
                  <tr>
                    <td className="col-item">{t("excel.landCost")}</td>
                    <td className="col-num">
                      {formatCurrencySAR(excelResult.costs.land_cost)}
                    </td>
                    <td className="col-calc">
                      {landNote}
                    </td>
                  </tr>
                  <tr>
                    <td className="col-item">{t("excel.constructionDirect")}</td>
                    <td className="col-num">
                      {formatCurrencySAR(excelResult.costs.construction_direct_cost)}
                    </td>
                    <td className="col-calc">
                      {explanationsDisplay?.construction_direct
                        ? directNote
                        : directNote
                        ? `${directNote}; ${t("excel.constructionDirectDefault")}`
                        : t("excel.constructionDirectDefault")}
                    </td>
                  </tr>
                  {effectiveLandUse === "m" && upperAnnexArea != null && upperAnnexArea > 0 && (
                    <tr>
                      <td className="col-item">{t("excel.upperAnnexNonFarCost")}</td>
                      <td className="col-num">
                        {formatCurrencySAR(upperAnnexCost ?? 0)}
                      </td>
                      <td className="col-calc">{upperAnnexCostNote}</td>
                    </tr>
                  )}
                  <tr>
                    <td className="col-item">
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          gap: "0.5rem",
                        }}
                      >
                        <span>{t("excel.fitout")}</span>
                        <ToggleChip
                          active={includeFitout}
                          onClick={() => handleFitoutToggle(!includeFitout)}
                          label={includeFitout ? t("excel.fitoutIncluded") : t("excel.fitoutExcluded")}
                        />
                      </div>
                    </td>
                    <td className="col-num">
                      {formatCurrencySAR(excelResult.costs.fitout_cost)}
                    </td>
                    <td className="col-calc">
                      {fitoutNote}
                    </td>
                  </tr>
                  <tr>
                    <td className="col-item">
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          gap: "0.5rem",
                        }}
                      >
                        <span>{t("excel.contingency")}</span>
                        <ToggleChip
                          active={includeContingency}
                          onClick={() => handleContingencyToggle(!includeContingency)}
                          label={includeContingency ? t("excel.fitoutIncluded") : t("excel.fitoutExcluded")}
                        />
                      </div>
                    </td>
                    <td className="col-num">
                      {formatCurrencySAR(excelResult.costs.contingency_cost)}
                    </td>
                    <td className="col-calc">
                      {contingencyNote}
                    </td>
                  </tr>
                  <tr>
                    <td className="col-item">{t("excel.consultants")}</td>
                    <td className="col-num">
                      {formatCurrencySAR(excelResult.costs.consultants_cost)}
                    </td>
                    <td className="col-calc">
                      {consultantsNote}
                    </td>
                  </tr>
                  <tr>
                    <td className="col-item">
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          gap: "0.5rem",
                        }}
                      >
                        <span>{t("excel.feasibilityFee")}</span>
                        <ToggleChip
                          active={includeFeasibility}
                          onClick={() => handleFeasibilityToggle(!includeFeasibility)}
                          label={includeFeasibility ? t("excel.fitoutIncluded") : t("excel.fitoutExcluded")}
                        />
                      </div>
                    </td>
                    <td className="col-num">
                      {formatCurrencySAR(excelResult.costs.feasibility_fee)}
                    </td>
                    <td className="col-calc">
                      {feasibilityExcluded
                        ? t("excelNotes.feasibilityExcluded")
                        : t("excelNotes.feasibility", {
                          land: formatNumberValue(landCostValue, 0),
                          pct: formatPercentValue(feasibilityPct, 1),
                        })}
                    </td>
                  </tr>
                  <tr>
                    <td className="col-item">{t("excel.transactionCosts")}</td>
                    <td className="col-num">
                      {formatCurrencySAR(excelResult.costs.transaction_cost)}
                    </td>
                    <td className="col-calc">
                      {transactionNote}
                    </td>
                  </tr>
                  <tr>
                    <td className="col-item">
                      <strong>{t("excel.totalCapex")}</strong>
                    </td>
                    <td className="col-num">
                      <strong>{formatCurrencySAR(excelResult.costs.grand_total_capex)}</strong>
                    </td>
                    <td className="col-calc">
                      {t("excel.totalCapexNote")}
                    </td>
                  </tr>
                </tbody>
              </Table>
            </div>
                )
              )}

              {selectedResultsTab === "revenue" && (
            <div>
              {mode === "v2" ? (
                <h4 className="ui-v2-sectionTitle">{t("excel.revenueBreakdown")}</h4>
              ) : (
                <h4 style={{ marginTop: 0, marginBottom: "0.5rem" }}>{t("excel.revenueBreakdown")}</h4>
              )}
              {rentMeta?.provider === "REGA" && residentialRentMo != null && (
                <p style={{ marginTop: 0, marginBottom: "0.75rem", fontSize: "0.8rem", color: "#cbd5f5" }}>
                  {t("excel.regaNote", {
                    location: rentMeta.district || rentMeta.city || t("common.notAvailable"),
                    monthly: formatNumberValue(residentialRentMo, 0),
                    yearly: formatNumberValue(residentialRentYr, 0),
                  })}
                </p>
              )}
              {mode === "v2" ? (
                <div className="ui-v2-accordionGroup">
                  <div className="ui-v2-revSection">
                    <button
                      type="button"
                      className="ui-v2-accHead"
                      data-open={v2RevenueSections.rental ? "true" : "false"}
                      onClick={() =>
                        setV2RevenueSections((prev) => ({
                          ...prev,
                          rental: !prev.rental,
                        }))
                      }
                    >
                      {v2RevenueSections.rental ? (
                        <ChevronDownIcon className="ui-v2-accordion__chev" />
                      ) : (
                        <ChevronRightIcon className="ui-v2-accordion__chev" />
                      )}
                      <span className="ui-v2-accordion__title">Rental Revenue by Asset Class</span>
                      <span className="ui-v2-accordion__total">{formatCurrencySAR(excelResult.costs.y1_income)}</span>
                    </button>
                    {v2RevenueSections.rental && (
                      <div className="ui-v2-accordion__body">
                        <div className="ui-v2-rowList">
                          {["residential", "retail", "office", "basement"].map((key) => {
                            const item = revenueItems.find((revenueItem) => revenueItem.key === key);
                            return (
                              <div key={key} className="ui-v2-row">
                                <span className="ui-v2-row__label">
                                  <span>{prettifyRevenueKey(item?.label || key)}</span>
                                  {item?.key && hasIncludedComponent(item.key) ? includedBadge : null}
                                </span>
                                <span className="ui-v2-row__val">{formatCurrencySAR(item?.amount || 0)}</span>
                                <V2InfoTip label={`Info ${key}`} body={(item?.note || "").trim() || "—"} />
                              </div>
                            );
                          })}
                        </div>
                        {revenueItems.find((item) => item.upperAnnexHint)?.upperAnnexHint && (
                          <div className="ui-v2-revNote">
                            Note: {revenueItems.find((item) => item.upperAnnexHint)?.upperAnnexHint}
                          </div>
                        )}
                      </div>
                    )}
                  </div>

                  <div className="ui-v2-revSection">
                    <button
                      type="button"
                      className="ui-v2-accHead"
                      data-open={v2RevenueSections.income ? "true" : "false"}
                      onClick={() =>
                        setV2RevenueSections((prev) => ({
                          ...prev,
                          income: !prev.income,
                        }))
                      }
                    >
                      {v2RevenueSections.income ? (
                        <ChevronDownIcon className="ui-v2-accordion__chev" />
                      ) : (
                        <ChevronRightIcon className="ui-v2-accordion__chev" />
                      )}
                      <span className="ui-v2-accordion__title">Income Summary</span>
                    </button>
                    {v2RevenueSections.income && (
                      <div className="ui-v2-accordion__body">
                        <div className="ui-v2-rowList">
                          <div className="ui-v2-row">
                            <span className="ui-v2-row__label">Annual Net Revenue</span>
                            <span className="ui-v2-row__val">{formatCurrencySAR(excelResult.costs.y1_income)}</span>
                            <V2InfoTip label="Annual Net Revenue info" body={incomeNote || "—"} />
                          </div>
                          <div className="ui-v2-row">
                            <span className="ui-v2-row__label">Annual Net Income</span>
                            <span className="ui-v2-row__val ui-v2-costRow__controls">
                              {formatCurrencySAR(excelResult.costs.y1_income_effective ?? effectiveIncome)}
                              <Input
                                type="number"
                                size="sm"
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
                                className="ui-v2-costInput"
                                aria-label={t("excel.effectiveIncomePct")}
                              />
                              <span className="ui-v2-chip">%</span>
                              <Button
                                type="button"
                                size="sm"
                                variant="secondary"
                                onClick={() => commitEffectiveIncomePct()}
                                disabled={effectiveIncomeApplyDisabled}
                              >
                                {t("common.apply")}
                              </Button>
                            </span>
                            <V2InfoTip label="Annual Net Income info" body={y1IncomeEffectiveNote || "—"} />
                          </div>
                          <div className="ui-v2-row">
                            <span className="ui-v2-row__label">OPEX</span>
                            <span className="ui-v2-row__val ui-v2-costRow__controls">
                              {formatCurrencySAR(opexCostResolved)}
                              {includedBadge}
                              <Input
                                type="number"
                                size="sm"
                                step="0.1"
                                min={0}
                                max={100}
                                value={opexPctDraft}
                                onChange={(event) => setOpexPctDraft(event.target.value)}
                                onKeyDown={(event) => {
                                  if (event.key === "Enter") {
                                    event.preventDefault();
                                    commitOpexPct();
                                  }
                                }}
                                className="ui-v2-costInput"
                                aria-label={t("excel.opex")}
                                disabled={!includeOpex}
                              />
                              <span className="ui-v2-chip">%</span>
                              <Button
                                type="button"
                                size="sm"
                                variant="secondary"
                                onClick={() => commitOpexPct()}
                                disabled={opexApplyDisabled}
                              >
                                {t("common.apply")}
                              </Button>
                            </span>
                            <V2InfoTip label="OPEX info" body={opexNote || "—"} />
                          </div>
                          <div className="ui-v2-row">
                            <span className="ui-v2-row__label">Annual NOI</span>
                            <span className="ui-v2-row__val">{formatCurrencySAR(y1NoiResolved)}</span>
                            <V2InfoTip label="Annual NOI info" body={t("excelNotes.noiYear1")} />
                          </div>
                        </div>
                      </div>
                    )}
                  </div>

                  <div className="ui-v2-revSection">
                    <button
                      type="button"
                      className="ui-v2-accHead"
                      data-open={v2RevenueSections.performance ? "true" : "false"}
                      onClick={() =>
                        setV2RevenueSections((prev) => ({
                          ...prev,
                          performance: !prev.performance,
                        }))
                      }
                    >
                      {v2RevenueSections.performance ? (
                        <ChevronDownIcon className="ui-v2-accordion__chev" />
                      ) : (
                        <ChevronRightIcon className="ui-v2-accordion__chev" />
                      )}
                      <span className="ui-v2-accordion__title">Investment Performance</span>
                    </button>
                    {v2RevenueSections.performance && (
                      <div className="ui-v2-accordion__body">
                        <div className="ui-v2-rowList">
                          <div className="ui-v2-row">
                            <span className="ui-v2-row__label">ROI</span>
                            <span className="ui-v2-row__val">{formatPercentValue(excelResult.roi)}</span>
                            <V2InfoTip label="ROI info" body={t("excelNotes.roiNoiFormula")} />
                          </div>
                          <div className="ui-v2-row">
                            <span className="ui-v2-row__label">Yield</span>
                            <span className="ui-v2-row__val">{formatPercentValue(yieldNoi, 1)}</span>
                            <V2InfoTip label="Yield info" body="Yield is annual NOI divided by total CAPEX." />
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              ) : (
              <Table>
                <thead>
                  <tr>
                    <th className="col-item">{t("excel.item")}</th>
                    <th className="col-num">{t("excel.amount")}</th>
                    <th className="col-calc">{t("excel.calculation")}</th>
                  </tr>
                </thead>
                <tbody>
                  {revenueItems.map((item) => (
                    <tr key={item.key}>
                      <td className="col-item">
                        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                          <span>{item.key.replace(/_/g, " ")}</span>
                          {item.upperAnnexHint ? (
                            <span
                              style={{
                                fontSize: "0.75rem",
                                opacity: 0.85,
                                lineHeight: 1.2,
                              }}
                              title={
                                "Upper annex (non-FAR) is excluded from FAR/cost structure but is counted for revenue by flowing into the dominant component (residential, else office)."
                              }
                            >
                              {item.upperAnnexHint}
                            </span>
                          ) : null}
                        </div>
                      </td>
                      <td className="col-num">{formatCurrencySAR(item.amount || 0)}</td>
                      <td className="col-calc">{item.note}</td>
                    </tr>
                  ))}
                  <tr>
                    <td className="col-item">{t("excel.year1Income")}</td>
                    <td className="col-num">
                      {formatCurrencySAR(excelResult.costs.y1_income)}
                    </td>
                    <td className="col-calc">
                      {incomeNote || t("excel.year1IncomeNote")}
                    </td>
                  </tr>
                  <tr>
                    <td className="col-item">
                      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                        <span>{t("excel.year1IncomeEffective")}</span>
                        <label className="calc-inline-controls">
                          <Input
                            type="number"
                            size="sm"
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
                            className="calc-input-coverage"
                            aria-label={t("excel.effectiveIncomePct")}
                          />
                          <span>%</span>
                        </label>
                        <Button
                          type="button"
                          size="sm"
                          variant="secondary"
                          onClick={() => commitEffectiveIncomePct()}
                          disabled={effectiveIncomeApplyDisabled}
                        >
                          {t("common.apply")}
                        </Button>
                      </div>
                    </td>
                    <td className="col-num">
                      {formatCurrencySAR(excelResult.costs.y1_income_effective ?? 0)}
                    </td>
                    <td className="col-calc">{y1IncomeEffectiveNote}</td>
                  </tr>
                  <tr>
                    <td className="col-item">
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          gap: "0.5rem",
                        }}
                      >
                        <span>{t("excel.opex")}</span>
                        <div className="calc-inline-controls">
                          <Input
                            type="number"
                            size="sm"
                            step="0.1"
                            min={0}
                            max={100}
                            value={opexPctDraft}
                            onChange={(event) => setOpexPctDraft(event.target.value)}
                            onKeyDown={(event) => {
                              if (event.key === "Enter") {
                                event.preventDefault();
                                commitOpexPct();
                              }
                            }}
                            className="calc-input-coverage"
                            aria-label={t("excel.opex")}
                            disabled={!includeOpex}
                          />
                          <span>%</span>
                          <Button
                            type="button"
                            size="sm"
                            variant="secondary"
                            onClick={() => commitOpexPct()}
                            disabled={opexApplyDisabled}
                          >
                            {t("common.apply")}
                          </Button>
                          <ToggleChip
                            active={includeOpex}
                            onClick={() => handleOpexToggle(!includeOpex)}
                            label={includeOpex ? t("excel.included") : t("excel.excluded")}
                          />
                        </div>
                      </div>
                    </td>
                    <td className="col-num">{formatCurrencySAR(opexCostResolved)}</td>
                    <td className="col-calc">{opexNote}</td>
                  </tr>
                  <tr>
                    <td className="col-item">{t("excel.noiYear1")}</td>
                    <td className="col-num">{formatCurrencySAR(y1NoiResolved)}</td>
                    <td className="col-calc">{t("excelNotes.noiYear1")}</td>
                  </tr>
                  <tr>
                    <td className="col-item">
                      <strong>{t("excel.unleveredRoi")}</strong>
                    </td>
                    <td className="col-num">
                      <strong>{formatPercentValue(excelResult.roi)}</strong>
                    </td>
                    <td className="col-calc">{t("excelNotes.roiNoiFormula")}</td>
                  </tr>
                </tbody>
              </Table>
              )}

            </div>
              )}

              {selectedResultsTab === "parking" &&
                (mode !== "v2" ? (
                  <ParkingSummary totals={excelResult.totals} notes={excelResult.notes} />
                ) : (
                  (() => {
                    const parking = resolveParking(excelResult);

                    return (
                      <div>
                        <div className="ui-v2-sectionTitle">Parking</div>

                        <div className="ui-v2-rowList">
                          <div className="ui-v2-row">
                            <span className="ui-v2-row__label">Required spaces (Riyadh minimum)</span>
                            <span className="ui-v2-row__val">
                              {parking.required == null ? "—" : formatNumberValue(parking.required, 0)}
                            </span>
                            <V2InfoTip
                              label="Required spaces info"
                              body="Calculated from Riyadh municipality minimum parking ratios per use."
                            />
                          </div>

                          <div className="ui-v2-row">
                            <span className="ui-v2-row__label">Provided spaces (from basement/parking area)</span>
                            <span className="ui-v2-row__val">
                              {parking.provided == null ? "—" : formatNumberValue(parking.provided, 0)}
                            </span>
                            <V2InfoTip
                              label="Provided spaces info"
                              body="Derived from allocated basement/parking area divided by m² per space."
                            />
                          </div>

                          <div className="ui-v2-row">
                            <span className="ui-v2-row__label">Deficit</span>
                            <span className="ui-v2-row__val">
                              {parking.deficit == null ? "—" : formatNumberValue(parking.deficit, 0)}
                            </span>
                            <V2InfoTip label="Deficit info" body="Required spaces minus provided spaces." />
                          </div>

                          <div className="ui-v2-row">
                            <span className="ui-v2-row__label">Compliant</span>
                            <span className="ui-v2-row__val">
                              {parking.compliant == null ? "—" : parking.compliant ? "Yes" : "No"}
                            </span>
                            <V2InfoTip
                              label="Compliance info"
                              body="Project is compliant when parking deficit is 0."
                            />
                          </div>

                          <div className="ui-v2-row">
                            <span className="ui-v2-row__label">Parking area counted (m²)</span>
                            <span className="ui-v2-row__val">
                              {parking.parkingAreaM2 == null ? "—" : formatNumberValue(parking.parkingAreaM2, 0)}
                            </span>
                            <V2InfoTip
                              label="Parking area info"
                              body="Total parking area counted in compliance calculation."
                            />
                          </div>

                          <div className="ui-v2-row">
                            <span className="ui-v2-row__label">Policy</span>
                            <span className="ui-v2-row__val">{parking.policy ?? "—"}</span>
                            <V2InfoTip
                              label="Policy info"
                              body="Parking adjustment policy applied during calculation."
                            />
                          </div>
                        </div>

                        {parking.autoAdjustment ? (
                          <div className="ui2-parking-auto-note">Auto-adjustment applied: {parking.autoAdjustment}</div>
                        ) : null}

                        {parking.requiredByComponent ? (
                          <div className="ui-v2-revSection">
                            <button
                              type="button"
                              className="ui-v2-accHead"
                              data-open={v2ParkingSections.requiredByComponent ? "true" : "false"}
                              onClick={() =>
                                setV2ParkingSections((prev) => ({
                                  ...prev,
                                  requiredByComponent: !prev.requiredByComponent,
                                }))
                              }
                            >
                              {v2ParkingSections.requiredByComponent ? (
                                <ChevronDownIcon className="ui-v2-accordion__chev" />
                              ) : (
                                <ChevronRightIcon className="ui-v2-accordion__chev" />
                              )}
                              <span className="ui-v2-accordion__title">Required by component</span>
                            </button>
                            {v2ParkingSections.requiredByComponent ? (
                              <div className="ui-v2-accordion__body">
                                <div className="ui-v2-rowList">
                                  {Object.entries(parking.requiredByComponent).map(([key, value]) => (
                                    <div key={key} className="ui-v2-row">
                                      <span className="ui-v2-row__label">{key}</span>
                                      <span className="ui-v2-row__val">
                                        {toNumericOrNull(value) == null ? "—" : formatNumberValue(toNumericOrNull(value), 0)}
                                      </span>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            ) : null}
                          </div>
                        ) : null}

                        <div className="ui-v2-revSection">
                          <button
                            type="button"
                            className="ui-v2-accHead"
                            data-open={v2ParkingSections.notes ? "true" : "false"}
                            onClick={() =>
                              setV2ParkingSections((prev) => ({
                                ...prev,
                                notes: !prev.notes,
                              }))
                            }
                          >
                            {v2ParkingSections.notes ? (
                              <ChevronDownIcon className="ui-v2-accordion__chev" />
                            ) : (
                              <ChevronRightIcon className="ui-v2-accordion__chev" />
                            )}
                            <span className="ui-v2-accordion__title">Notes / warnings</span>
                          </button>
                          {v2ParkingSections.notes ? (
                            <div className="ui-v2-accordion__body">
                              {parking.warnings.length ? (
                                <ul style={{ margin: 0, paddingInlineStart: "1.2rem" }}>
                                  {parking.warnings.map((warning, idx) => (
                                    <li key={`${warning}-${idx}`}>{warning}</li>
                                  ))}
                                </ul>
                              ) : (
                                <div className="ui-v2-rowList">
                                  <div className="ui-v2-row">
                                    <span className="ui-v2-row__label">No parking warnings reported.</span>
                                  </div>
                                </div>
                              )}
                            </div>
                          ) : null}
                        </div>
                      </div>
                    );
                  })()
                ))}
              </Card>
            )}
            {/* Right rail financial summary:
                - v2: ALWAYS render the same V2FinancialSummaryCard for every tab
                - legacy: keep legacy right panel (handled elsewhere / legacy layout) */}
            {mode === "v2" ? (
              // Summary tab already renders its own right rail earlier; avoid double rendering
              selectedResultsTab === "summary" ? null : (
                <div className="oak-right-panel calc-grid__right ui2-right-rail">
                  <V2FinancialSummaryCard className="ui2-right-rail__card" />
                </div>
              )
            ) : (
              <div className="oak-right-panel calc-grid__right">
                <Card title={t("excel.financialSummaryTitle")} className="oak-financial-summary fin-summary">
                  <div className="calc-summary-list">
                    <div className="calc-summary-row fin-summary__row">
                      <span className="calc-summary-key fin-summary__label">{t("excel.totalCapex")}</span>
                      <span className="calc-summary-value fin-summary__value">{formatCurrencySAR(excelResult.costs.grand_total_capex)}</span>
                    </div>
                    <div className="calc-summary-row fin-summary__row">
                      <span className="calc-summary-key fin-summary__label">{t("excel.year1Income")}</span>
                      <span className="calc-summary-value fin-summary__value">{formatCurrencySAR(excelResult.costs.y1_income)}</span>
                    </div>
                    <div className="calc-summary-row fin-summary__row">
                      <span className="calc-summary-key fin-summary__label">{t("excel.noiYear1")}</span>
                      <span className="calc-summary-value fin-summary__value">{formatCurrencySAR(y1NoiResolved)}</span>
                    </div>
                    <div className="calc-summary-row calc-summary-row--split fin-summary__split">
                      <div className="fin-summary__split-col">
                        <span className="calc-summary-key fin-summary__label">{t("excel.unleveredRoi")}</span>
                        <span className="calc-summary-value fin-summary__value">{formatPercentValue(excelResult.roi)}</span>
                      </div>
                      <div className="fin-summary__split-col">
                        <span className="calc-summary-key fin-summary__label">{t("excel.yield")}</span>
                        <span className="calc-summary-value fin-summary__value">{formatPercentValue(yieldNoi, 1)}</span>
                      </div>
                    </div>
                  </div>
                </Card>
              </div>
            )}
          </div>
          <div ref={feedbackSentinelRef} style={{ height: 1 }} />
        </section>
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
