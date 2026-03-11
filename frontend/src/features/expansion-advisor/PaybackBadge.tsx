import { paybackColor, fmtMonths } from "./formatHelpers";

type PaybackBadgeProps = {
  band: string | null | undefined;
  months: number | null | undefined;
};

export default function PaybackBadge({ band, months }: PaybackBadgeProps) {
  const color = paybackColor(band);
  const label = band || "—";
  const detail = months != null && Number.isFinite(months) ? fmtMonths(months) : "";
  return (
    <span className={`ea-badge ea-badge--${color}`}>
      {label}
      {detail ? <span style={{ fontWeight: 400, marginInlineStart: 4 }}>{detail}</span> : null}
    </span>
  );
}
