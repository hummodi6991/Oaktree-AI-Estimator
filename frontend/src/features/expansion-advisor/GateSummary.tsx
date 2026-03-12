import { useTranslation } from "react-i18next";

type GateSummaryProps = {
  gates: Record<string, boolean> | null | undefined;
  unknownGates?: string[];
};

function gateLabel(key: string): string {
  return key
    .replace(/_/g, " ")
    .replace(/\bpass\b/gi, "")
    .trim();
}

function gateIcon(status: "pass" | "fail" | "unknown"): string {
  if (status === "pass") return "\u2713";
  if (status === "fail") return "\u2717";
  return "?";
}

export default function GateSummary({ gates, unknownGates }: GateSummaryProps) {
  const { t } = useTranslation();
  if (!gates || Object.keys(gates).length === 0) {
    return <span className="ea-badge ea-badge--neutral">{t("expansionAdvisor.gateUnknown")}</span>;
  }

  const unknownSet = new Set(unknownGates || []);

  return (
    <div className="ea-gate-list">
      {Object.entries(gates).map(([key, passed]) => {
        const isUnknown = unknownSet.has(key) || passed === null || passed === undefined;
        const status: "pass" | "fail" | "unknown" = isUnknown ? "unknown" : (passed ? "pass" : "fail");
        return (
          <span
            key={key}
            className={`ea-gate-item ea-gate-item--${status}`}
            title={key}
          >
            {gateIcon(status)} {gateLabel(key)}
          </span>
        );
      })}
    </div>
  );
}
