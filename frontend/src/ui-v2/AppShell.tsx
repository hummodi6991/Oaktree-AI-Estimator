import type { ReactNode } from "react";

type AppShellProps = {
  header: ReactNode;
  map: ReactNode;
  content: ReactNode;
  layout?: "stacked" | "split";
};

export default function AppShell({ header, map, content, layout = "stacked" }: AppShellProps) {
  const mainCls = `ui-v2-main${layout === "split" ? " ui-v2-main--split" : ""}`;
  return (
    <div className="ui-v2-shell">
      {header}
      <main className={mainCls}>
        <section className="ui-v2-map-hero">{map}</section>
        <section className="ui-v2-content">{content}</section>
      </main>
    </div>
  );
}
