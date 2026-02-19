import { CalculatorIcon, ChartBarSquareIcon, ScaleIcon } from "@heroicons/react/24/outline";

const TILES = [
  { title: "Cost & Revenue Calculations", Icon: CalculatorIcon },
  { title: "Detailed Breakdown", Icon: ChartBarSquareIcon },
  { title: "Scenario Comparison", Icon: ScaleIcon },
];

export default function EmptyState() {
  return (
    <section className="ui-v2-empty-state" aria-label="Empty analysis state">
      <h2>Select one or multiple parcel(s) to begin analysis</h2>
      <p>
        Begin by selecting parcels directly on the map or through search. Your estimate and breakdown modules will
        appear here once a selection is active.
      </p>
      <div className="ui-v2-empty-state__tiles">
        {TILES.map((tile) => (
          <button key={tile.title} type="button" className="ui-v2-empty-state__tile">
            <tile.Icon width={20} height={20} />
            <span>{tile.title}</span>
          </button>
        ))}
      </div>
    </section>
  );
}
