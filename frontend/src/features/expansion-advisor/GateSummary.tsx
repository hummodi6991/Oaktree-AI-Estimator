import { useTranslation } from "react-i18next";

type GateSummaryProps = {
  gates: Record<string, boolean> | null | undefined;
};

function gateLabel(key: string): string {
  return key
    .replace(/_/g, " ")
    .replace(/\bpass\b/gi, "")
    .trim();
}

export default function GateSummary({ gates }: GateSummaryProps) {
  const { t } = useTranslation();
  if (!gates || Object.keys(gates).length === 0) {
    return <span className="ea-badge ea-badge--neutral">{t("expansionAdvisor.gateUnknown")}</span>;
  }

  return (
    <div className="ea-gate-list">
      {Object.entries(gates).map(([key, passed]) => (
        <span
          key={key}
          className={`ea-gate-item ${passed ? "ea-gate-item--pass" : "ea-gate-item--fail"}`}
          title={key}
        >
          {passed ? "\u2713" : "\u2717"} {gateLabel(key)}
        </span>
      ))}
    </div>
  );
}
