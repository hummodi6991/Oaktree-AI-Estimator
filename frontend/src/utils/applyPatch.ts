const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  Boolean(value) && typeof value === "object" && !Array.isArray(value);

export function applyPatch<T extends Record<string, unknown>>(base: T, patch: Partial<T>): T {
  if (!isPlainObject(base) || !isPlainObject(patch)) {
    return { ...(base as T), ...(patch as Partial<T>) };
  }

  const next: Record<string, unknown> = { ...base };
  for (const [key, value] of Object.entries(patch)) {
    if (value === undefined) {
      continue;
    }
    const baseValue = next[key];
    if (isPlainObject(baseValue) && isPlainObject(value)) {
      next[key] = applyPatch(baseValue, value);
    } else {
      next[key] = value;
    }
  }
  return next as T;
}
