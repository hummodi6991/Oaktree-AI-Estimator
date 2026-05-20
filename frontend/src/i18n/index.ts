import i18n from "i18next";
import { initReactI18next } from "react-i18next";

import en from "./en.json";
import ar from "./ar.json";

const LOCALE_STORAGE_KEY = "oaktree_locale";

const isArabicLocale = (value: string) => value.toLowerCase().startsWith("ar");
const normalizeLocale = (value: string) => (isArabicLocale(value) ? "ar" : "en");

const getInitialLocale = () => {
  if (typeof window !== "undefined") {
    const stored = window.localStorage.getItem(LOCALE_STORAGE_KEY);
    if (stored) return normalizeLocale(stored);

    const browserLocale = window.navigator.language || "";
    if (isArabicLocale(browserLocale)) return "ar";
  }
  return "en";
};

const applyDocumentLocale = (locale: string) => {
  if (typeof document === "undefined") return;
  const isArabic = isArabicLocale(locale);
  document.documentElement.dir = isArabic ? "rtl" : "ltr";
  document.documentElement.lang = isArabic ? "ar" : "en";
  document.documentElement.dataset.locale = isArabic ? "ar" : "en";
  document.documentElement.classList.toggle("rtl", isArabic);
  if (document.body) {
    document.body.dir = isArabic ? "rtl" : "ltr";
    document.body.classList.toggle("rtl", isArabic);
  }
};

i18n
  .use(initReactI18next)
  .init({
    resources: {
      en: { translation: en },
      ar: { translation: ar },
    },
    lng: getInitialLocale(),
    fallbackLng: "en",
    interpolation: {
      escapeValue: false,
    },
  });

i18n.on("languageChanged", (locale) => {
  const normalized = normalizeLocale(locale);
  if (typeof window !== "undefined") {
    window.localStorage.setItem(LOCALE_STORAGE_KEY, normalized);
  }
  applyDocumentLocale(normalized);
});

// Hard-reload the page on a user-initiated language change so no stale,
// locale-bound state (memo/report payloads, expanded panels, candidate
// selections, MapLibre tiles, React Query cache) survives the toggle.
//
// __lastObservedLocale tracks the last locale we saw. It absorbs i18next's
// boot-time languageChanged fire (so opening the app in AR does not loop
// reload) and suppresses identity changes (changeLanguage(currentLocale)).
// We track manually instead of comparing document.documentElement.lang
// because the listener above already mutates that attribute before this
// one runs (listeners fire in attachment order), so a doc-based check
// would always read the new value and never reload.
let __lastObservedLocale: string | null = null;

i18n.on("languageChanged", (locale) => {
  const normalized = normalizeLocale(locale);

  if (__lastObservedLocale === null) {
    __lastObservedLocale = normalized;
    return;
  }

  if (__lastObservedLocale === normalized) {
    return;
  }

  __lastObservedLocale = normalized;

  if (typeof window !== "undefined") {
    window.location.reload();
  }
});

applyDocumentLocale(normalizeLocale(i18n.language));

export { LOCALE_STORAGE_KEY, applyDocumentLocale, getInitialLocale, normalizeLocale };
export default i18n;
