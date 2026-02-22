import type { ParcelSummary } from "../api";
import { formatAreaM2 } from "../i18n/format";
import { useTranslation } from "react-i18next";
import { ArrowUpTrayIcon } from "@heroicons/react/24/outline";

type ParcelInfoBarProps = {
  parcel: ParcelSummary;
  landUseLabel: string;
  methodLabel: string;
  onToggleMap: () => void;
  isMapHidden: boolean;
};

export default function ParcelInfoBar({
  parcel,
  landUseLabel,
  methodLabel,
  onToggleMap,
  isMapHidden,
}: ParcelInfoBarProps) {
  const { t } = useTranslation();
  const primaryArea = parcel.parcel_area_m2 ?? parcel.area_m2 ?? null;
  const footprintArea = parcel.footprint_area_m2 ?? null;
  const showFootprint = footprintArea != null && Number.isFinite(footprintArea) && parcel.parcel_area_m2 != null;
  const areaValue = formatAreaM2(primaryArea, { maximumFractionDigits: 0 }, t("common.notAvailable"));
  const areaLabel = showFootprint
    ? `${areaValue} (${t("app.footprintLabel")}: ${formatAreaM2(
        footprintArea,
        { maximumFractionDigits: 0 },
        t("common.notAvailable"),
      )})`
    : areaValue;
  const parcelSummary = [
    `${t("app.parcelLabel")}: ${parcel.parcel_id}`,
    `${t("app.areaLabel")}: ${areaLabel}`,
    `${t("app.landUseLabel")}: ${landUseLabel}`,
    `${t("app.methodLabel")}: ${methodLabel}`,
  ].join(" | ");

  return (
    <div className="ui-v2-parcelstrip parcel-strip">
      <div className="ui-v2-parcelstrip__left parcel-strip__meta">{parcelSummary}</div>
      <div className="ui-v2-parcelstrip__actions parcel-strip__action">
        <button className="ui-v2-parcelstrip__iconBtn" type="button" aria-label="Export">
          <ArrowUpTrayIcon width={18} height={18} />
        </button>
        <button className="ui-v2-parcelstrip__primaryBtn" type="button" onClick={onToggleMap}>
          {isMapHidden ? t("ui.showMap") : t("ui.hideMap")}
        </button>
      </div>
    </div>
  );
}
