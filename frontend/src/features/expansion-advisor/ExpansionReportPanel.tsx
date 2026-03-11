export default function ExpansionReportPanel({ report }: { report: Record<string, any> | null }) {
  if (!report) return null;
  const rec = report.recommendation || {};
  const top = report.top_candidates || [];
  return (
    <div>
      <h4>Recommendation report</h4>
      <div>Best: {rec.best_candidate_id || "-"}</div>
      <div>Runner-up: {rec.runner_up_candidate_id || "-"}</div>
      <div>Why best: {rec.why_best || "-"}</div>
      <div>Main risk: {rec.main_risk || "-"}</div>
      <div>Best format: {rec.best_format || "-"}</div>
      <div>Summary: {rec.report_summary || "-"}</div>
      <ul>{top.slice(0, 3).map((item: any) => <li key={item.id}>{item.id} — {item.final_score}</li>)}</ul>
    </div>
  );
}
