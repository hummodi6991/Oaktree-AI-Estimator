import type { ReactNode } from "react";

type AnalysisLayoutProps = {
  top: ReactNode;
  controls: ReactNode;
  calculations?: ReactNode;
};

export default function AnalysisLayout({ top, controls, calculations }: AnalysisLayoutProps) {
  return (
    <div className="ui-v2-analysis-layout">
      {top}
      <div className="ui-v2-form-wrap">{controls}</div>
      {calculations ? <div className="ui-v2-form-wrap">{calculations}</div> : null}
    </div>
  );
}
