import { ArrowsRightLeftIcon, CalculatorIcon, Squares2X2Icon } from "@heroicons/react/24/outline";

const TILES = [
  { title: "Cost & Revenue Calculations", Icon: CalculatorIcon },
  { title: "Detailed Breakdown", Icon: Squares2X2Icon },
  { title: "Scenario Comparison", Icon: ArrowsRightLeftIcon },
];

export default function EmptyState() {
  return (
    <section className="ui-v2-empty" aria-label="Empty analysis state">
      <div className="ui-v2-empty__copy">
        <h2 className="ui-v2-empty__title">Select one or multiple parcel(s) to begin analysis</h2>
        <p className="ui-v2-empty__subtitle">
          Ctrl +Click (desktop) or Long-press (touch) to multi-select.
        </p>
      </div>
      <div className="ui-v2-empty__tiles">
        {TILES.map((tile) => (
          <div key={tile.title} className="ui-v2-empty__tile ui-v2-card">
            <tile.Icon className="ui-v2-empty__tileIcon" aria-hidden="true" />
            <div className="ui-v2-empty__tileTitle">{tile.title}</div>
          </div>
        ))}
      </div>
    </section>
  );
}
