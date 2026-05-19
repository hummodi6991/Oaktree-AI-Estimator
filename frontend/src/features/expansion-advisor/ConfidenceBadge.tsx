import { useTranslation } from "react-i18next";
import { confidenceColor } from "./formatHelpers";

type ConfidenceBadgeProps = {
  grade: string | null | undefined;
  /** When true, just show the letter without "Data:" prefix. */
  compact?: boolean;
};

export default function ConfidenceBadge({ grade, compact }: ConfidenceBadgeProps) {
  const { t } = useTranslation();
  const color = confidenceColor(grade);
  const label = grade || "—";
  return (
    <span className={`ea-badge ea-badge--${color}`} title={t("expansionAdvisor.confidenceBadge.tooltip")}>
      {compact ? label : t("expansionAdvisor.confidenceBadge.prefix", { grade: label })}
    </span>
  );
}
