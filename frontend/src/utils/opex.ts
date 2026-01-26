const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(value, max));

export const formatPercentDraftFromFraction = (fraction?: number | null, digits = 1) => {
  const safeFraction = Number.isFinite(fraction ?? NaN) ? clamp(fraction ?? 0, 0, 1) : 0;
  const percent = safeFraction * 100;
  const factor = Math.pow(10, digits);
  const rounded = Math.round(percent * factor) / factor;
  return rounded % 1 === 0 ? String(Math.round(rounded)) : String(rounded);
};

export const resolveFractionFromDraftPercent = (draft: string) => {
  if (draft.trim() === "") return null;
  const parsed = Number(draft);
  if (!Number.isFinite(parsed)) return null;
  const clamped = clamp(parsed, 0, 100);
  return clamped / 100;
};
