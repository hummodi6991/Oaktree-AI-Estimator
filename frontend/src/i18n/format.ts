export const formatNumber = (
  value: number | string | null | undefined,
  locale: string,
  options: Intl.NumberFormatOptions = {},
  fallback = "—",
) => {
  const num = typeof value === "string" ? Number(value) : value;
  if (num == null || !Number.isFinite(num)) return fallback;
  return new Intl.NumberFormat(locale, { numberingSystem: "latn", ...options }).format(num);
};

export const formatPercent = (
  value: number | null | undefined,
  locale: string,
  options: Intl.NumberFormatOptions = { maximumFractionDigits: 1, minimumFractionDigits: 1 },
  fallback = "—",
) => {
  if (value == null || !Number.isFinite(value)) return fallback;
  const formatter = new Intl.NumberFormat(locale, {
    style: "percent",
    numberingSystem: "latn",
    ...options,
  });
  return formatter.format(value);
};
