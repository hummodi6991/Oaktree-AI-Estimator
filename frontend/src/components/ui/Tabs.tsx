import type { ReactNode } from "react";

type TabItem = {
  id: string;
  label: ReactNode;
};

type TabsProps = {
  items: TabItem[];
  value: string;
  onChange: (id: string) => void;
};

export default function Tabs({ items, value, onChange }: TabsProps) {
  return (
    <div className="ot-tabs" role="tablist" aria-orientation="horizontal">
      {items.map((item) => {
        const isActive = item.id === value;
        return (
          <button
            key={item.id}
            type="button"
            role="tab"
            aria-selected={isActive}
            className={`ot-tabs__tab${isActive ? " is-active" : ""}`}
            onClick={() => onChange(item.id)}
          >
            {item.label}
          </button>
        );
      })}
    </div>
  );
}
