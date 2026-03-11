import { fmtScore, scoreColor } from "./formatHelpers";

type ScorePillProps = {
  value: number | null | undefined;
  label?: string;
  large?: boolean;
};

export default function ScorePill({ value, label, large }: ScorePillProps) {
  const color = scoreColor(value);
  return (
    <span className={`ea-pill ea-pill--${color}${large ? " ea-pill--lg" : ""}`}>
      {label ? <span>{label}</span> : null}
      {fmtScore(value)}
    </span>
  );
}
