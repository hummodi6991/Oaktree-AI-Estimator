import type { ReactNode } from "react";

type AppShellProps = {
  header: ReactNode;
  map: ReactNode;
  content: ReactNode;
};

export default function AppShell({ header, map, content }: AppShellProps) {
  return (
    <div className="ui-v2-shell">
      {header}
      <main className="ui-v2-main">
        <section className="ui-v2-map-hero">{map}</section>
        <section className="ui-v2-content">{content}</section>
      </main>
    </div>
  );
}
