export type AreaRatioMap = Record<string, number | string>;

export const resolveAreaRatioBase = (candidates: Array<AreaRatioMap | null | undefined>): AreaRatioMap => {
  for (const candidate of candidates) {
    if (candidate && Object.keys(candidate).length > 0) {
      return candidate;
    }
  }
  return {};
};

const isBasementKey = (key: string) => {
  const normalized = key.trim().toLowerCase();
  return normalized === "basement" || /^basement([_-].+)?$/.test(normalized);
};

const toNumber = (value: unknown) => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

type ScaleResult = {
  nextAreaRatio: AreaRatioMap;
  currentAboveGroundFar: number;
  factor: number;
};

const isAboveGroundKey = (key: string) => !isBasementKey(key);

/**
 * Scale only above-ground area ratios so their sum matches the target FAR.
 * Basement ratios (keys named basement or prefixed with basement_) are left unchanged.
 */
export const scaleAboveGroundAreaRatio = (
  areaRatio: AreaRatioMap,
  targetFar: number,
): ScaleResult | null => {
  if (!Number.isFinite(targetFar) || targetFar <= 0) return null;

  const entries = Object.entries(areaRatio);
  let currentAboveGroundFar = 0;

  for (const [key, value] of entries) {
    if (!isAboveGroundKey(key)) continue;
    const numericValue = toNumber(value);
    if (numericValue == null) continue;
    currentAboveGroundFar += numericValue;
  }

  if (!Number.isFinite(currentAboveGroundFar) || currentAboveGroundFar <= 0) return null;

  const factor = targetFar / currentAboveGroundFar;
  if (!Number.isFinite(factor)) return null;

  const nextAreaRatio: AreaRatioMap = { ...areaRatio };

  for (const [key, value] of entries) {
    if (!isAboveGroundKey(key)) continue;
    const numericValue = toNumber(value);
    if (numericValue == null) continue;
    nextAreaRatio[key] = numericValue * factor;
  }

  return { nextAreaRatio, currentAboveGroundFar, factor };
};
