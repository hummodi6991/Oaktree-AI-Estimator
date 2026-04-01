import { useTranslation } from "react-i18next";

interface TierBadgeProps {
  sourceTier?: number | null;
  sourceType?: string | null;
  isVacant?: boolean | null;
  currentCategory?: string | null;
  clAvgRating?: number | null;
  listingUrl?: string | null;
  rentConfidence?: string | null;
}

const tierConfig: Record<number, {
  labelKey: string;
  bgColor: string;
  textColor: string;
  borderColor: string;
  icon: string;
}> = {
  1: {
    labelKey: "expansionAdvisor.tierAvailableUnit",
    bgColor: "#dcfce7",
    textColor: "#166534",
    borderColor: "#86efac",
    icon: "🏪",
  },
  2: {
    labelKey: "expansionAdvisor.tierProvenLocation",
    bgColor: "#dbeafe",
    textColor: "#1e40af",
    borderColor: "#93c5fd",
    icon: "📍",
  },
  3: {
    labelKey: "expansionAdvisor.tierHighPotential",
    bgColor: "#f3f4f6",
    textColor: "#374151",
    borderColor: "#d1d5db",
    icon: "📐",
  },
};

/**
 * Color-coded tier badge for expansion advisor candidates.
 *
 * Tier 1 (Aqar): Green "Available Unit"
 * Tier 2 (Delivery/POI): Blue "Proven Location"
 * Tier 3 (ArcGIS): Gray "High Potential"
 */
export default function TierBadge({
  sourceTier,
  sourceType,
  currentCategory,
  clAvgRating,
  listingUrl,
  rentConfidence,
}: TierBadgeProps) {
  const { t } = useTranslation();

  if (!sourceTier) return null;
  const config = tierConfig[sourceTier];
  if (!config) return null;

  return (
    <div style={{ marginBottom: 8 }}>
      {/* Badge pill */}
      <span
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 4,
          padding: "2px 10px",
          borderRadius: 9999,
          fontSize: 12,
          fontWeight: 600,
          backgroundColor: config.bgColor,
          color: config.textColor,
          border: `1px solid ${config.borderColor}`,
        }}
      >
        <span>{config.icon}</span>
        <span>{t(config.labelKey)}</span>
      </span>

      {/* Rent confidence indicator */}
      {rentConfidence === "actual" && (
        <span
          style={{
            marginInlineStart: 6,
            fontSize: 11,
            color: "#166534",
            fontWeight: 500,
          }}
        >
          ✓ {t("expansionAdvisor.tierActualRent")}
        </span>
      )}

      {/* Tier 2: Current category + rating */}
      {sourceTier === 2 && currentCategory && (
        <div style={{ fontSize: 12, color: "#6b7280", marginTop: 4 }}>
          {t("expansionAdvisor.tierCurrently")}: {currentCategory}
          {clAvgRating ? ` · ${clAvgRating.toFixed(1)}★` : ""}
          {sourceType ? ` · ${sourceType}` : ""}
        </div>
      )}

      {/* Tier 1: Aqar link */}
      {sourceTier === 1 && listingUrl && (
        <div style={{ marginTop: 4 }}>
          <a
            href={listingUrl}
            target="_blank"
            rel="noopener noreferrer"
            style={{ fontSize: 12, color: "#2563eb", textDecoration: "underline" }}
            onClick={(e) => e.stopPropagation()}
          >
            {t("expansionAdvisor.tierViewAqar")} &#8599;
          </a>
        </div>
      )}
    </div>
  );
}
