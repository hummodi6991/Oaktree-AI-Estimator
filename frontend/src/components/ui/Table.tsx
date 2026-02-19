import type { ReactNode } from "react";

type TableProps = {
  children: ReactNode;
};

export default function Table({ children }: TableProps) {
  return (
    <div className="calc-table-wrap">
      <table className="calc-table">{children}</table>
    </div>
  );
}
