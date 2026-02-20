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

  return (
    <div className="oak-infobar">
      <div className="oak-container oak-infobar-inner">
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
      <div className="oak-infobar-actions">
        <button type="button" className="oak-btn oak-btn--tertiary oak-btn--md oak-icon-btn" aria-label="Export">
          <ArrowUpTrayIcon />
        </button>
        <button type="button" className="oak-btn oak-btn--primary oak-btn--md" onClick={onToggleMap}>
          {isMapHidden ? t("ui.showMap") : t("ui.hideMap")}
        </button>
      </div>
      </div>
    </div>
  );
}
