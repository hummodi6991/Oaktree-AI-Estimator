import { CalculatorIcon, ChartBarSquareIcon, ScaleIcon, SparklesIcon } from "@heroicons/react/24/outline";

const TILES = [
  { title: "Cost & Revenue Calculations", Icon: CalculatorIcon },
  { title: "Detailed Breakdown", Icon: ChartBarSquareIcon },
  { title: "Scenario Comparison", Icon: ScaleIcon },
];

export default function EmptyState() {
  return (
    <section className="ui-v2-empty-state" aria-label="Empty analysis state">
      <div className="ui-v2-empty-state__icon" aria-hidden="true">
        <SparklesIcon width={24} height={24} />
      </div>
      <h2>Select one or multiple parcel(s) to begin analysis</h2>
      <p>
        Choose a parcel to start development assumptions and view financial projections.
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
