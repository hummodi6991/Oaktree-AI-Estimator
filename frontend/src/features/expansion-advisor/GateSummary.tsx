import { useTranslation } from "react-i18next";
import { humanGateLabel } from "./formatHelpers";

type GateValue = boolean | null | undefined;

type GateSummaryProps = {
  gates: Record<string, GateValue> | null | undefined;
  unknownGates?: string[];
};

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
        if (key === "overall_pass") return null;
        // Tri-state: explicit null/undefined or in unknown list → unknown (not fail)
        const isUnknown = unknownSet.has(key) || passed === null || passed === undefined;
        const status: "pass" | "fail" | "unknown" = isUnknown ? "unknown" : (passed === true ? "pass" : "fail");
        return (
          <span
            key={key}
            className={`ea-gate-item ea-gate-item--${status}`}
            title={humanGateLabel(key)}
          >
            {gateIcon(status)} {humanGateLabel(key)}
          </span>
        );
      })}
    </div>
  );
}
