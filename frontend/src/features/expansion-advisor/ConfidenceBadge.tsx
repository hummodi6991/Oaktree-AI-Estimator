import { confidenceColor } from "./formatHelpers";

type ConfidenceBadgeProps = {
  grade: string | null | undefined;
};

export default function ConfidenceBadge({ grade }: ConfidenceBadgeProps) {
  const color = confidenceColor(grade);
  return (
    <span className={`ea-badge ea-badge--${color}`}>
      {grade || "—"}
    </span>
  );
}
