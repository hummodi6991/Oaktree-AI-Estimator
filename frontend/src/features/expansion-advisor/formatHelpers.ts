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
  if (b === "fast" || b === "promising") return "green";
  if (b === "moderate" || b === "standard") return "amber";
  return "red";
}

/** Gate color */
export function gateColor(pass: boolean | null | undefined): "green" | "red" | "neutral" {
  if (pass === true) return "green";
  if (pass === false) return "red";
  return "neutral";
}
