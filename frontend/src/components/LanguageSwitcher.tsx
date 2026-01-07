import { useEffect, useRef } from "react";
import type { ChangeEvent } from "react";
import { useTranslation } from "react-i18next";
import { runFormatSanityCheck } from "../i18n/format";

const LANGUAGE_OPTIONS = [
  { value: "en", labelKey: "language.english" },
  { value: "ar", labelKey: "language.arabic" },
];

export default function LanguageSwitcher() {
  const { t, i18n } = useTranslation();
  const labelRef = useRef<HTMLSpanElement | null>(null);

  const handleChange = (event: ChangeEvent<HTMLSelectElement>) => {
    void i18n.changeLanguage(event.target.value);
  };

  const currentLanguage = i18n.language.startsWith("ar") ? "ar" : "en";

  useEffect(() => {
    if (!labelRef.current) return;
    const expected = t("language.label");
    if (labelRef.current.textContent !== expected) {
      console.warn("Locale label did not update as expected", {
        expected,
        actual: labelRef.current.textContent,
      });
    }

    const isArabic = i18n.language.startsWith("ar");
    const expectedDir = isArabic ? "rtl" : "ltr";
    if (document.documentElement.dir !== expectedDir) {
      console.warn("Document direction did not update", {
        expected: expectedDir,
        actual: document.documentElement.dir,
      });
    }

    runFormatSanityCheck(i18n.language);
  }, [i18n.language, t]);

  return (
    <label className="language-switcher">
      <span ref={labelRef}>{t("language.label")}</span>
      <select value={currentLanguage} onChange={handleChange} aria-label={t("language.label")}>
        {LANGUAGE_OPTIONS.map((option) => (
          <option key={option.value} value={option.value}>
            {t(option.labelKey)}
          </option>
        ))}
      </select>
    </label>
  );
}
