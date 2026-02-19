import TILES from "./tiles";

export default function EmptyState() {
  return (
    <section className="ui-v2-empty-state" aria-label="Empty analysis state">
      <div className="ui-v2-empty-state__icon" aria-hidden="true">
        ▦
      </div>
      <h2>Select one or multiple parcel(s) to begin analysis</h2>
      <p>
        Begin by selecting parcels directly on the map or through search.
        <br />
        Your estimate and breakdown modules will appear here once a selection is active.
      </p>
      <div className="ui-v2-empty-state__tiles">
        {TILES.map((tile) => (
          <button key={tile.title} type="button" className="ui-v2-empty-state__tile">
            <span>{tile.title}</span>
            <small>{tile.description}</small>
          </button>
        ))}
      </div>
    </section>
  );
}
