import type { ParcelSummary } from "../api";
import { formatAreaM2 } from "../i18n/format";
import { useTranslation } from "react-i18next";

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

  return (
    <div className="ui-v2-parcelbar">
      <div className="ui-v2-parcelbar__left">
        <b>{t("app.parcelLabel")}:</b> {parcel.parcel_id} | <b>{t("app.areaLabel")}:</b>{" "}
        {formatAreaM2(primaryArea, { maximumFractionDigits: 0 }, t("common.notAvailable"))}
        {showFootprint
          ? ` (${t("app.footprintLabel")}: ${formatAreaM2(
              footprintArea,
              { maximumFractionDigits: 0 },
              t("common.notAvailable"),
            )})`
          : ""}{" "}
        | <b>{t("app.landUseLabel")}:</b> {landUseLabel} | <b>{t("app.methodLabel")}:</b> {methodLabel}
      </div>
      <button type="button" className="ui-v2-parcelbar__btn" onClick={onToggleMap}>
        {isMapHidden ? "Show Map" : "Hide Map"}
      </button>
    </div>
  );
}
