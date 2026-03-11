import { useTranslation } from "react-i18next";
import type { FilterKey, SortKey } from "./studyAdapters";

type Props = {
  activeFilter: FilterKey;
  activeSort: SortKey;
  districtFilter: string;
  districts: string[];
  totalCount: number;
  filteredCount: number;
  onFilterChange: (filter: FilterKey) => void;
  onSortChange: (sort: SortKey) => void;
  onDistrictFilterChange: (district: string) => void;
};

const FILTERS: { key: FilterKey; labelKey: string }[] = [
  { key: "all", labelKey: "expansionAdvisor.filterAll" },
  { key: "pass_only", labelKey: "expansionAdvisor.filterPassOnly" },
  { key: "fastest_payback", labelKey: "expansionAdvisor.filterFastestPayback" },
  { key: "strongest_economics", labelKey: "expansionAdvisor.filterStrongestEconomics" },
  { key: "strongest_brand_fit", labelKey: "expansionAdvisor.filterStrongestBrandFit" },
  { key: "lowest_cannibalization", labelKey: "expansionAdvisor.filterLowestCannibalization" },
  { key: "strongest_delivery", labelKey: "expansionAdvisor.filterStrongestDelivery" },
];

const SORTS: { key: SortKey; labelKey: string }[] = [
  { key: "rank", labelKey: "expansionAdvisor.sortRank" },
  { key: "payback", labelKey: "expansionAdvisor.sortPayback" },
  { key: "economics", labelKey: "expansionAdvisor.sortEconomics" },
  { key: "brand_fit", labelKey: "expansionAdvisor.sortBrandFit" },
  { key: "cannibalization", labelKey: "expansionAdvisor.sortCannibalization" },
  { key: "delivery", labelKey: "expansionAdvisor.sortDelivery" },
  { key: "district", labelKey: "expansionAdvisor.sortDistrict" },
];

export default function SortFilterBar({
  activeFilter,
  activeSort,
  districtFilter,
  districts,
  totalCount,
  filteredCount,
  onFilterChange,
  onSortChange,
  onDistrictFilterChange,
}: Props) {
  const { t } = useTranslation();
  const isFiltered = activeFilter !== "all" || districtFilter !== "";
  const isSorted = activeSort !== "rank";

  return (
    <div className="ea-sort-filter-bar">
      <div className="ea-sort-filter-bar__row">
        <div className="ea-sort-filter-bar__group">
          <label className="ea-sort-filter-bar__label">{t("expansionAdvisor.filterLabel")}:</label>
          <select
            className="ea-form__select ea-sort-filter-bar__select"
            value={activeFilter}
            onChange={(e) => onFilterChange(e.target.value as FilterKey)}
          >
            {FILTERS.map((f) => (
              <option key={f.key} value={f.key}>
                {t(f.labelKey)}
              </option>
            ))}
          </select>
        </div>
        <div className="ea-sort-filter-bar__group">
          <label className="ea-sort-filter-bar__label">{t("expansionAdvisor.sortLabel")}:</label>
          <select
            className="ea-form__select ea-sort-filter-bar__select"
            value={activeSort}
            onChange={(e) => onSortChange(e.target.value as SortKey)}
          >
            {SORTS.map((s) => (
              <option key={s.key} value={s.key}>
                {t(s.labelKey)}
              </option>
            ))}
          </select>
        </div>
        {districts.length > 1 && (
          <div className="ea-sort-filter-bar__group">
            <label className="ea-sort-filter-bar__label">{t("expansionAdvisor.district")}:</label>
            <select
              className="ea-form__select ea-sort-filter-bar__select"
              value={districtFilter}
              onChange={(e) => onDistrictFilterChange(e.target.value)}
            >
              <option value="">{t("expansionAdvisor.allDistricts")}</option>
              {districts.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
          </div>
        )}
      </div>
      {(isFiltered || isSorted) && (
        <div className="ea-sort-filter-bar__status">
          <span className="ea-sort-filter-bar__count">
            {t("expansionAdvisor.showingOf", { shown: filteredCount, total: totalCount })}
          </span>
          {isSorted && (
            <span className="ea-badge ea-badge--neutral">
              {t("expansionAdvisor.localSort")}
            </span>
          )}
        </div>
      )}
    </div>
  );
}
