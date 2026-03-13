import { confidenceColor } from "./formatHelpers";

type ConfidenceBadgeProps = {
  grade: string | null | undefined;
  /** When true, just show the letter without "Data:" prefix. */
  compact?: boolean;
};

export default function ConfidenceBadge({ grade, compact }: ConfidenceBadgeProps) {
  const color = confidenceColor(grade);
  const label = grade || "—";
  return (
    <span className={`ea-badge ea-badge--${color}`} title="Data confidence grade">
      {compact ? label : `Data: ${label}`}
    </span>
  );
}
