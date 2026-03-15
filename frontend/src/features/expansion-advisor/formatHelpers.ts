import { formatCurrencySAR, formatNumber, formatInteger } from "../../i18n/format";

const FALLBACK = "—";

export function fmtSAR(value: number | null | undefined): string {
  return formatCurrencySAR(value, FALLBACK);
}

export function fmtScore(value: number | null | undefined, digits = 0): string {
  return formatNumber(value, { maximumFractionDigits: digits, minimumFractionDigits: digits }, FALLBACK);
}

export function fmtM2(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return FALLBACK;
  return `${formatInteger(Math.round(value), FALLBACK)} m²`;
}

export function fmtMeters(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return FALLBACK;
  if (value >= 1000) {
    return `${formatNumber(value / 1000, { maximumFractionDigits: 1, minimumFractionDigits: 1 }, FALLBACK)} km`;
  }
  return `${formatInteger(Math.round(value), FALLBACK)} m`;
}

export function fmtMonths(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return FALLBACK;
  return `${Math.round(value)} mo`;
}

export function fmtPct(value: number | null | undefined, digits = 0): string {
  if (value == null || !Number.isFinite(value)) return FALLBACK;
  return `${formatNumber(value, { maximumFractionDigits: digits, minimumFractionDigits: digits }, FALLBACK)}%`;
}

export function fmtSarPerM2(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return FALLBACK;
  return `${formatInteger(Math.round(value), FALLBACK)} SAR/m²`;
}

export function fmtSarPerM2Year(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return FALLBACK;
  return `${formatInteger(Math.round(value), FALLBACK)} SAR/m²/yr`;
}

/** Color semantic: >=70 green, >=40 amber, <40 red */
export function scoreColor(value: number | null | undefined): "green" | "amber" | "red" | "neutral" {
  if (value == null || !Number.isFinite(value)) return "neutral";
  if (value >= 70) return "green";
  if (value >= 40) return "amber";
  return "red";
}

/** Confidence grade color: A/B green, C amber, D/F red */
export function confidenceColor(grade: string | null | undefined): "green" | "amber" | "red" | "neutral" {
  if (!grade) return "neutral";
  const g = grade.toUpperCase();
  if (g === "A" || g === "B") return "green";
  if (g === "C") return "amber";
  return "red";
}

/** Payback band color */
export function paybackColor(band: string | null | undefined): "green" | "amber" | "red" | "neutral" {
  if (!band) return "neutral";
  const b = band.toLowerCase();
  if (b === "fast" || b === "promising" || b === "strong") return "green";
  if (b === "moderate" || b === "standard" || b === "borderline") return "amber";
  return "red";
}

/** Gate color */
export function gateColor(pass: boolean | null | undefined): "green" | "red" | "neutral" {
  if (pass === true) return "green";
  if (pass === false) return "red";
  return "neutral";
}

/* ─── Human-readable gate labels ─── */

const GATE_LABEL_MAP: Record<string, string> = {
  zoning_fit_pass: "Zoning fit",
  zoning_pass: "Zoning",
  area_fit_pass: "Area fit",
  frontage_access_pass: "Frontage / access",
  frontage_pass: "Frontage",
  access_pass: "Access",
  parking_pass: "Parking",
  district_pass: "District",
  visibility_pass: "Visibility",
  competition_pass: "Competition",
  brand_fit_pass: "Brand fit",
  economics_pass: "Economics",
  cannibalization_pass: "Cannibalization",
  delivery_market_pass: "Delivery market",
  overall_pass: "Overall",
  // Human-readable labels from backend (already humanized gate_reasons lists)
  "zoning fit": "Zoning fit",
  "area fit": "Area fit",
  "frontage/access": "Frontage / access",
  parking: "Parking",
  district: "District",
  cannibalization: "Cannibalization",
  "delivery market": "Delivery market",
  economics: "Economics",
};

/** Return a clean human-readable label for a gate key. */
export function humanGateLabel(key: string): string {
  if (GATE_LABEL_MAP[key]) return GATE_LABEL_MAP[key];
  return key
    .replace(/_/g, " ")
    .replace(/\bpass\b/gi, "")
    .trim()
    .replace(/^\w/, (c) => c.toUpperCase());
}

/** Produce a human-readable one-line explanation for a gate verdict. */
export function humanGateSentence(key: string, status: "pass" | "fail" | "unknown"): string {
  const label = humanGateLabel(key);
  if (status === "pass") return `${label} passed.`;
  if (status === "fail") return `${label} failed.`;
  return `${label} needs field verification.`;
}

/* ─── District label fallback ─── */

// Rough heuristic: if >40% of chars are replacement-character or within known
// garbled Arabic byte ranges, consider the string broken.
const GARBLED_RE = /[\uFFFD\uFFFE\uFFF0-\uFFFF]{2,}/;
// Single replacement/BOM chars mixed into otherwise readable text
const SINGLE_GARBLED_RE = /[\uFFFD\uFFFE]/;
const EMPTY_RE = /^\s*$/;
// Latin mojibake patterns common with mis-encoded Arabic text (e.g. Ø§Ù„, Ã, Â)
const MOJIBAKE_RE = /(?:[Ã\xC3][\x80-\xBF]|Ø[§-¿]Ù[„\x80-\x8F]|Ã¢|Ã©|Ã¨|Ã±){2,}/;

/** Return true if a string looks like garbled / broken text. */
export function isGarbledText(text: string | null | undefined): boolean {
  if (!text) return true;
  if (EMPTY_RE.test(text)) return true;
  if (GARBLED_RE.test(text)) return true;
  if (SINGLE_GARBLED_RE.test(text)) return true;
  if (MOJIBAKE_RE.test(text)) return true;
  return false;
}

/** Strip replacement characters and BOM from display strings */
export function cleanDisplayText(text: string | null | undefined): string | null {
  if (!text) return null;
  const cleaned = text.replace(/[\uFFFD\uFFFE\uFEFF]/g, "").trim();
  return cleaned || null;
}

/**
 * Pick the best available district label.
 * Prefers arabic → english → normalized key → fallback.
 */
export function safeDistrictLabel(
  arabic: string | null | undefined,
  english: string | null | undefined,
  key: string | null | undefined,
  fallback = "Unknown district",
): string {
  if (arabic && !isGarbledText(arabic)) return arabic;
  if (english && !isGarbledText(english)) return english;
  if (key && !isGarbledText(key)) return key.replace(/_/g, " ");
  return fallback;
}

/**
 * Extract the best district label from a candidate-like object.
 *
 * Prefers canonical fields from the backend (district_display, district_name_ar,
 * district_name_en, district_key) and falls back through safeDistrictLabel
 * if those are missing.  Returns "Unknown district" if everything is garbled.
 */
export function candidateDistrictLabel(
  candidate: {
    district_display?: string | null;
    district_name_ar?: string | null;
    district_name_en?: string | null;
    district_key?: string | null;
    district?: string | null;
  } | null | undefined,
  fallback = "Unknown district",
): string {
  if (!candidate) return fallback;

  // 1. Use backend canonical display if clean
  const display = candidate.district_display;
  if (display && !isGarbledText(display)) return display;

  // 2. Fall through safeDistrictLabel cascade
  return safeDistrictLabel(
    candidate.district_name_ar,
    candidate.district_name_en,
    candidate.district_key,
    // 3. Try raw district before giving up
    candidate.district && !isGarbledText(candidate.district)
      ? candidate.district
      : fallback,
  );
}
