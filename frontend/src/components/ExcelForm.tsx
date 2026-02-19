import { useEffect, useMemo, useRef, useState } from "react";
import type { Geometry } from "geojson";
import { useTranslation } from "react-i18next";

import "../styles/excel-form.css";

import { downloadMemoPdf, landPrice, makeEstimate, runScenario, trackEvent } from "../api";
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
import Button from "./ui/Button";
import Checkbox from "./ui/Checkbox";
import Field from "./ui/Field";
import Input from "./ui/Input";
import Select from "./ui/Select";

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
};

type MassingLock = "far" | "floors" | "coverage";

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

export default function ExcelForm({ parcel, landUseOverride }: ExcelFormProps) {
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
  const [isScenarioActive, setIsScenarioActive] = useState(false);
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
  const floorsInputStyle = {
    width: "110px",
    padding: "4px 6px",
    borderRadius: 6,
    border: "1px solid rgba(255,255,255,0.2)",
    background: "rgba(0,0,0,0.15)",
    color: "white",
    fontSize: "14px",
    textAlign: "right" as const,
  };
  const coverageInputStyle = {
    width: "72px",
    padding: "4px 6px",
    borderRadius: 6,
    border: "1px solid rgba(255,255,255,0.2)",
    background: "rgba(0,0,0,0.15)",
    color: "white",
    fontSize: "14px",
    textAlign: "right" as const,
  };
  const floorsApplyButtonStyle = {
    padding: "4px 8px",
    borderRadius: 6,
    border: "1px solid rgba(255,255,255,0.2)",
    background: "rgba(255,255,255,0.08)",
    color: "white",
    cursor: "pointer",
  } as const;
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
    setIsScenarioActive(false);
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
      amount: incomeComponents[key] ?? 0,
      note: resolveRevenueNote(key, baseNote, incomeComponents[key] ?? 0),
      upperAnnexHint: isUpperAnnexSink ? upperAnnexHintText : null,
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
    { key: "upper_annex_non_far", label: t("excel.unitCostUpperAnnexNonFar") },
  ];
  const activeUnitCostFields =
    effectiveLandUse === "m"
      ? unitCostFields
      : unitCostFields.filter((field) => field.key === "residential" || field.key === "basement");
  return (
    <div>
      <div className="excel-controls-row">
        <div className="excel-controls-row__left">
          <div className="excel-controls-row__grid">
            <Field label={t("excel.providerLabel").replace(/:$/, "")}>
              <Select
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

            <Field label={t("excel.overrideLandUse").replace(/:$/, "")}>
              <Select
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

            {showLandPriceOverride ? (
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
            ) : (
              <div style={{ display: "flex", alignItems: "end" }}>
                <button
                  type="button"
                  className="excel-controls-row__inline-link"
                  onClick={() => setShowLandPriceOverride(true)}
                >
                  {t("excel.overrideLandPrice")}
                </button>
              </div>
            )}
          </div>

          <div className="excel-controls-row__actions">
            <Button onClick={fetchPrice} variant="secondary">{t("excel.fetchPrice")}</Button>
            <Checkbox
              label={t("excel.componentResidential")}
              checked={componentsDraft.residential}
              onChange={() => toggleComponent("residential")}
            />
            <Checkbox
              label={t("excel.componentRetail")}
              checked={componentsDraft.retail}
              onChange={() => toggleComponent("retail")}
            />
            <Checkbox
              label={t("excel.componentOffice")}
              checked={componentsDraft.office}
              onChange={() => toggleComponent("office")}
            />
            <Button type="button" onClick={applyComponents} disabled={!componentsDirty} variant="secondary">{t("common.apply")}</Button>
            <Button onClick={handleEstimateClick}>{t("excel.calculateEstimate")}</Button>
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

        <aside className="ot-card unit-cost-panel">
          <h3 className="unit-cost-panel__title">{t("excel.unitCostTitle")}</h3>
          <div className="unit-cost-panel__list">
            {activeUnitCostFields.map((field) => (
              <div key={field.key} className="unit-cost-panel__item">
                <span>{field.label}</span>
                <span className="unit-cost-panel__value">{formatNumberValue(unitCostInputs[field.key] ?? 0, 0)}</span>
              </div>
            ))}
          </div>
        </aside>
      </div>

      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginTop: 12, alignItems: "center" }}>
        <Button
          type="button"
          onClick={() => setIsScenarioOpen(true)}
          disabled={!estimateId || isScenarioSubmitting}
          variant="secondary"
        >
          Scenario
        </Button>
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
            <Button
              type="button"
              onClick={() => {
                if (scenarioBaseResult) {
                  setExcelResult(scenarioBaseResult);
                  setScenarioBaseResult(null);
                  setIsScenarioActive(false);
                }
              }}
              variant="secondary"
            >
              Reset scenario
            </Button>
          </div>
        )}
        {estimateId && (
          <div className="excel-estimate-actions">
            <Button type="button" onClick={handleExportPdf} variant="secondary">
              Export PDF
            </Button>
            <div className="excel-estimate-actions__meta">
              <span>Estimate ID: {estimateId}</span>
              <Button
                type="button"
                onClick={copyEstimateId}
                aria-label="Copy estimate ID"
                variant="secondary"
                className="excel-estimate-actions__copy"
              >
                📋
              </Button>
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
                  <tr>
                    <td style={itemColumnStyle}>Coverage</td>
                    <td style={amountColumnStyle}>
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "flex-end",
                          gap: 8,
                          flexWrap: "wrap",
                        }}
                      >
                        <input
                          type="number"
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
                          style={coverageInputStyle}
                          aria-label="Coverage ratio"
                        />
                        <span style={{ opacity: 0.75 }}>%</span>
                        <button
                          type="button"
                          onClick={commitCoverage}
                          disabled={coverageApplyDisabled}
                          style={{
                            ...floorsApplyButtonStyle,
                            cursor: coverageApplyDisabled ? "not-allowed" : "pointer",
                            opacity: coverageApplyDisabled ? 0.6 : 1,
                          }}
                        >
                          {t("common.apply")}
                        </button>
                      </div>
                    </td>
                    <td style={calcColumnStyle}>
                      <div>Used with FAR to infer above-ground floors.</div>
                      {coverageEditError && <div style={farErrorStyle}>{coverageEditError}</div>}
                    </td>
                  </tr>
                  {farAboveGround != null && (
                    <tr>
                      <td style={itemColumnStyle}>{t("excel.effectiveFar")}</td>
                      <td style={amountColumnStyle}>
                        {isEditingFar ? (
                          <div style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", gap: 6 }}>
                            <input
                              type="number"
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
                    <td style={itemColumnStyle}>Implied floors</td>
                    <td style={amountColumnStyle}>
                      {impliedFloors != null && Number.isFinite(impliedFloors)
                        ? formatNumberValue(impliedFloors, 1)
                        : "—"}
                    </td>
                    <td style={calcColumnStyle}>FAR ÷ coverage.</td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>Floors (above-ground)</td>
                    <td style={amountColumnStyle}>
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "flex-end",
                          gap: 8,
                          flexWrap: "wrap",
                        }}
                      >
                        <input
                          type="number"
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
                          style={{
                            ...floorsInputStyle,
                          }}
                          aria-label="Floors above ground"
                        />
                        <button
                          type="button"
                          onClick={commitFloors}
                          disabled={floorsApplyDisabled}
                          style={{
                            ...floorsApplyButtonStyle,
                            cursor: floorsApplyDisabled ? "not-allowed" : "pointer",
                            opacity: floorsApplyDisabled ? 0.6 : 1,
                          }}
                        >
                          {t("common.apply")}
                        </button>
                      </div>
                    </td>
                    <td style={calcColumnStyle}>
                      {floorsNote}
                      {floorsEditError && <div style={farErrorStyle}>{floorsEditError}</div>}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>Massing locks</td>
                    <td style={amountColumnStyle}>
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "flex-end",
                          gap: 12,
                          flexWrap: "wrap",
                        }}
                      >
                        <label style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                          <input
                            type="radio"
                            name="massing-lock"
                            checked={massingLock === "floors"}
                            onChange={() => applyInputPatch({ massing_lock: "floors" })}
                          />
                          Lock floors
                        </label>
                        <label style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                          <input
                            type="radio"
                            name="massing-lock"
                            checked={massingLock === "far"}
                            onChange={() => applyInputPatch({ massing_lock: "far" })}
                          />
                          Lock FAR
                        </label>
                        <label style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                          <input
                            type="radio"
                            name="massing-lock"
                            checked={massingLock === "coverage"}
                            onChange={() => applyInputPatch({ massing_lock: "coverage" })}
                          />
                          Lock coverage
                        </label>
                      </div>
                    </td>
                    <td style={calcColumnStyle}>Choose a single driver for massing updates.</td>
                  </tr>
                  {components.residential && (
                    <tr>
                      <td style={itemColumnStyle}>{t("excel.residentialBua")}</td>
                      <td style={amountColumnStyle}>{formatArea(displayedBuiltArea.residential)}</td>
                      <td style={calcColumnStyle}>{buaNote("residential")}</td>
                    </tr>
                  )}
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
                  {effectiveLandUse === "m" && upperAnnexArea != null && upperAnnexArea > 0 && (
                    <tr>
                      <td style={itemColumnStyle}>{t("excel.upperAnnexNonFarBua")}</td>
                      <td style={amountColumnStyle}>{formatArea(upperAnnexArea)}</td>
                      <td style={calcColumnStyle}>{explanationsDisplay.upper_annex_non_far_bua}</td>
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
                      {explanationsDisplay?.construction_direct
                        ? directNote
                        : directNote
                        ? `${directNote}; ${t("excel.constructionDirectDefault")}`
                        : t("excel.constructionDirectDefault")}
                    </td>
                  </tr>
                  {effectiveLandUse === "m" && upperAnnexArea != null && upperAnnexArea > 0 && (
                    <tr>
                      <td style={itemColumnStyle}>{t("excel.upperAnnexNonFarCost")}</td>
                      <td style={amountColumnStyle}>
                        {formatCurrencySAR(upperAnnexCost ?? 0)}
                      </td>
                      <td style={calcColumnStyle}>{upperAnnexCostNote}</td>
                    </tr>
                  )}
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
                      <td style={itemColumnStyle}>
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
                          <span style={{ opacity: 0.75 }}>%</span>
                          <button
                            type="button"
                            onClick={() => commitOpexPct()}
                            disabled={opexApplyDisabled}
                            style={{
                              padding: "4px 8px",
                              borderRadius: 6,
                              border: "1px solid rgba(255,255,255,0.2)",
                              background: "rgba(255,255,255,0.08)",
                              color: "white",
                              cursor: opexApplyDisabled ? "not-allowed" : "pointer",
                              opacity: opexApplyDisabled ? 0.6 : 1,
                            }}
                          >
                            {t("common.apply")}
                          </button>
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
