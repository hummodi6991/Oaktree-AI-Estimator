/**
 * Skeleton loading placeholders for the Expansion Advisor.
 * Uses the `.ea-skeleton` CSS shimmer defined in expansion-advisor.css.
 */

export function SkeletonCard() {
  return <div className="ea-skeleton ea-skeleton--card" aria-hidden="true" />;
}

export function SkeletonText({ width = "100%" }: { width?: string }) {
  return <div className="ea-skeleton ea-skeleton--text" style={{ width }} aria-hidden="true" />;
}

export function SkeletonBadge() {
  return <span className="ea-skeleton ea-skeleton--badge" aria-hidden="true" />;
}

export function CandidateListSkeleton({ count = 5 }: { count?: number }) {
  return (
    <div style={{ display: "grid", gap: 12 }}>
      {Array.from({ length: count }, (_, i) => (
        <SkeletonCard key={i} />
      ))}
    </div>
  );
}

export function DetailSkeleton() {
  return (
    <div style={{ display: "grid", gap: 10, padding: 16 }}>
      <SkeletonText width="60%" />
      <SkeletonText width="80%" />
      <SkeletonText width="40%" />
      <div style={{ display: "flex", gap: 8 }}>
        <SkeletonBadge />
        <SkeletonBadge />
        <SkeletonBadge />
      </div>
      <SkeletonText />
      <SkeletonText width="70%" />
    </div>
  );
}
