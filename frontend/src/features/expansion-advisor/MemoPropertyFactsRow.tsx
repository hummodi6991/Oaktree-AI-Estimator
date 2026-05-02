import { useTranslation } from "react-i18next";
import { fmtSARCompact } from "./formatHelpers";

type Candidate = {
  area_m2?: number | null;
  unit_area_sqm?: number | null;
  unit_street_width_m?: number | null;
  estimated_annual_rent_sar?: number | null;
  listing_age_days?: number | null;
  is_vacant?: boolean | null;
};

type MemoPropertyFactsRowProps = {
  candidate: Candidate;
  lang: "en" | "ar";
};

export default function MemoPropertyFactsRow({ candidate, lang }: MemoPropertyFactsRowProps) {
  const { t } = useTranslation();

  const area = candidate.area_m2 ?? candidate.unit_area_sqm ?? null;
  const streetWidth = candidate.unit_street_width_m ?? null;
  const rent = candidate.estimated_annual_rent_sar ?? null;
  const isVacant = candidate.is_vacant === true;
  const vacantDays = isVacant ? candidate.listing_age_days ?? null : null;

  const segments: string[] = [];

  if (area != null && Number.isFinite(area)) {
    segments.push(`${Math.round(area)} m²`);
  }
  if (streetWidth != null && Number.isFinite(streetWidth)) {
    segments.push(t("expansionAdvisor.memoFacts.frontage", { width: streetWidth }));
  }
  if (rent != null && Number.isFinite(rent)) {
    segments.push(t("expansionAdvisor.memoFacts.rentPerYear", { rent: fmtSARCompact(rent) }));
  }
  if (isVacant) {
    if (vacantDays != null && Number.isFinite(vacantDays)) {
      segments.push(t("expansionAdvisor.memoFacts.vacantDays", { days: vacantDays }));
    } else {
      segments.push(t("expansionAdvisor.memoFacts.currentlyVacant"));
    }
  }

  if (segments.length === 0) return null;

  const dir = lang === "ar" ? "rtl" : "ltr";

  return (
    <div className="ea-memo-property-facts" dir={dir}>
      {segments.map((seg, i) => (
        <span key={i} className="ea-memo-property-facts__segment">
          {seg}
        </span>
      ))}
    </div>
  );
}
