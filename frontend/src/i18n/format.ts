import i18n from "./index";

const FALLBACK_VALUE = "—";

const isArabicLocale = (value: string) => value.toLowerCase().startsWith("ar");

const resolveNumberLocale = (language = i18n.language) =>
  isArabicLocale(language) ? "ar-SA-u-nu-arab" : "en-US";

const parseNumber = (value: number | string | null | undefined) => {
  if (value == null) return null;
  const num = typeof value === "string" ? Number(value) : value;
  if (!Number.isFinite(num)) return null;
  return num;
};

export const formatNumber = (
  value: number | string | null | undefined,
  options: Intl.NumberFormatOptions = {},
  fallback = FALLBACK_VALUE,
) => {
  const num = parseNumber(value);
  if (num == null) return fallback;
  return new Intl.NumberFormat(resolveNumberLocale(), options).format(num);
};

export const formatInteger = (value: number | string | null | undefined, fallback = FALLBACK_VALUE) =>
  formatNumber(value, { maximumFractionDigits: 0, minimumFractionDigits: 0 }, fallback);

export const formatCurrencySAR = (value: number | string | null | undefined, fallback = FALLBACK_VALUE) => {
  const num = parseNumber(value);
  if (num == null) return fallback;
  return new Intl.NumberFormat(resolveNumberLocale(), {
    style: "currency",
    currency: "SAR",
    maximumFractionDigits: 0,
  }).format(num);
};

export const formatAreaM2 = (value: number | string | null | undefined, options?: Intl.NumberFormatOptions, fallback = FALLBACK_VALUE) => {
  const num = parseNumber(value);
  if (num == null) return fallback;
  const unit = isArabicLocale(i18n.language) ? "م²" : "m²";
  const formatted = new Intl.NumberFormat(resolveNumberLocale(), {
    maximumFractionDigits: 0,
    minimumFractionDigits: 0,
    ...(options ?? {}),
  }).format(num);
  return `${formatted} ${unit}`;
};

export const formatPercent = (
  value: number | null | undefined,
  options: Intl.NumberFormatOptions = { maximumFractionDigits: 1, minimumFractionDigits: 1 },
  fallback = FALLBACK_VALUE,
) => {
  if (value == null || !Number.isFinite(value)) return fallback;
  const formatter = new Intl.NumberFormat(resolveNumberLocale(), {
    style: "percent",
    ...options,
  });
  return formatter.format(value);
};

export const runFormatSanityCheck = (language = i18n.language) => {
  const isDev = typeof import.meta !== "undefined" && Boolean((import.meta as any)?.env?.DEV);
  if (!isDev || !isArabicLocale(language)) return;

  const sampleNumber = formatNumber(123456.78, { maximumFractionDigits: 2, minimumFractionDigits: 2 });
  const hasArabicDigits = /[٠-٩]/.test(sampleNumber);
  const hasArabicSeparator = /[٬٫]/.test(sampleNumber);
  if (!hasArabicDigits || !hasArabicSeparator) {
    console.warn("Arabic number formatting failed sanity check", { sampleNumber });
  }

  const sampleCurrency = formatCurrencySAR(17082.099);
  const hasArabicCurrencyDigits = /[٠-٩]/.test(sampleCurrency);
  if (!hasArabicCurrencyDigits) {
    console.warn("Arabic currency formatting failed sanity check", { sampleCurrency });
  }
};
