import type { FormEvent } from "react";
import { useEffect, useRef, useState } from "react";

export type ScenarioProviderOption = {
  value: string;
  label: string;
};

type ScenarioModalProps = {
  isOpen: boolean;
  providers: ScenarioProviderOption[];
  isSubmitting?: boolean;
  onClose: () => void;
  onSubmit: (patch: Record<string, unknown>, meta: Record<string, unknown>) => void;
};

const numberFields = [
  { key: "land_price_sar_m2", label: "Land price SAR/m²" },
  { key: "far", label: "FAR (above-ground)" },
  { key: "price_uplift_pct", label: "Price uplift %" },
];

export default function ScenarioModal({
  isOpen,
  providers,
  isSubmitting = false,
  onClose,
  onSubmit,
}: ScenarioModalProps) {
  const [landPrice, setLandPrice] = useState("");
  const [far, setFar] = useState("");
  const [priceUplift, setPriceUplift] = useState("");
  const [provider, setProvider] = useState("");
  const [error, setError] = useState<string | null>(null);
  const firstInputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (!isOpen) return;
    setLandPrice("");
    setFar("");
    setPriceUplift("");
    setProvider("");
    setError(null);
    const frame = requestAnimationFrame(() => firstInputRef.current?.focus());
    return () => cancelAnimationFrame(frame);
  }, [isOpen]);

  if (!isOpen) return null;

  const parseNumberField = (
    value: string,
    key: string,
    patch: Record<string, unknown>,
    meta: Record<string, unknown>,
  ) => {
    const trimmed = value.trim();
    if (!trimmed) return true;
    const parsed = Number(trimmed);
    if (!Number.isFinite(parsed)) {
      setError("Please enter valid numbers for scenario inputs.");
      return false;
    }
    patch[key] = parsed;
    meta[key] = parsed;
    return true;
  };

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(null);
    const patch: Record<string, unknown> = {};
    const meta: Record<string, unknown> = {};
    if (!parseNumberField(landPrice, "land_price_sar_m2", patch, meta)) return;
    if (!parseNumberField(far, "far", patch, meta)) return;
    if (!parseNumberField(priceUplift, "price_uplift_pct", patch, meta)) return;
    const providerValue = provider.trim();
    if (providerValue) {
      patch.provider = providerValue;
      meta.provider = providerValue;
    }
    if (!Object.keys(patch).length) {
      setError("Set at least one input.");
      return;
    }
    onSubmit(patch, meta);
  };

  return (
    <div className="scenario-modal-overlay" role="presentation">
      <div className="scenario-modal" role="dialog" aria-modal="true" aria-labelledby="scenario-modal-title">
        <div className="scenario-modal-header">
          <h2 id="scenario-modal-title" className="scenario-modal-title">Run scenario</h2>
          <button className="scenario-modal-close" type="button" onClick={onClose} aria-label="Close scenario">
            ×
          </button>
        </div>
        <p className="scenario-modal-body">
          Adjust optional levers below to see how outcomes change. Leave fields blank to keep current values.
        </p>
        <form className="scenario-modal-form" onSubmit={handleSubmit}>
          <label className="scenario-modal-label" htmlFor="scenario-land-price">
            {numberFields[0].label}
          </label>
          <input
            id="scenario-land-price"
            ref={firstInputRef}
            className="scenario-modal-input"
            type="number"
            inputMode="decimal"
            value={landPrice}
            onChange={(event) => setLandPrice(event.target.value)}
            placeholder="Optional"
          />

          <label className="scenario-modal-label" htmlFor="scenario-far">
            {numberFields[1].label}
          </label>
          <input
            id="scenario-far"
            className="scenario-modal-input"
            type="number"
            inputMode="decimal"
            value={far}
            onChange={(event) => setFar(event.target.value)}
            placeholder="Optional"
          />

          <label className="scenario-modal-label" htmlFor="scenario-provider">
            Provider
          </label>
          <select
            id="scenario-provider"
            className="scenario-modal-select"
            value={provider}
            onChange={(event) => setProvider(event.target.value)}
          >
            <option value="">Optional</option>
            {providers.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>

          <label className="scenario-modal-label" htmlFor="scenario-uplift">
            {numberFields[2].label}
          </label>
          <input
            id="scenario-uplift"
            className="scenario-modal-input"
            type="number"
            inputMode="decimal"
            value={priceUplift}
            onChange={(event) => setPriceUplift(event.target.value)}
            placeholder="Optional"
          />

          {error && <div className="scenario-modal-error">{error}</div>}

          <div className="scenario-modal-actions">
            <button className="secondary-button" type="button" onClick={onClose} disabled={isSubmitting}>
              Cancel
            </button>
            <button className="primary-button" type="submit" disabled={isSubmitting}>
              Run scenario
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
