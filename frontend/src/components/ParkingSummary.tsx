import React from "react";

type AnyDict = Record<string, any>;

function unwrapNotes(notes: any): AnyDict | undefined {
  if (!notes || typeof notes !== "object") return undefined;
  // GET /estimates/{id} stores notes like: { bands: ..., notes: {...actual notes...} }
  if ("notes" in notes && (notes as any).notes && typeof (notes as any).notes === "object") {
    return (notes as any).notes as AnyDict;
  }
  return notes as AnyDict;
}

function fmtInt(v: any): string {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return Math.round(n).toLocaleString();
}

function fmtNum(v: any, decimals = 0): string {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString(undefined, { maximumFractionDigits: decimals, minimumFractionDigits: decimals });
}

function yesNo(v: any): string {
  if (v === true) return "Yes";
  if (v === false) return "No";
  return "—";
}

export default function ParkingSummary(props: { totals?: AnyDict; notes?: any }) {
  const totals = props.totals || {};
  const notes = unwrapNotes(props.notes);
  const parking: AnyDict | undefined = notes?.parking;

  // Prefer totals (simple), fallback to notes.parking.*_final
  const required =
    totals.parking_required_spaces ??
    parking?.required_spaces_final ??
    parking?.required_spaces;

  const provided =
    totals.parking_provided_spaces ??
    parking?.provided_spaces_final ??
    parking?.provided_spaces_before;

  const deficit =
    totals.parking_deficit_spaces ??
    parking?.deficit_spaces_final ??
    parking?.deficit_spaces_before;

  const compliant =
    totals.parking_compliant ??
    parking?.compliant;

  const parkingAreaM2 =
    parking?.parking_area_m2_final ??
    parking?.parking_area_m2_before;

  const policy = parking?.policy;
  const basementAddedM2 = parking?.basement_area_added_m2;
  const basementKey = parking?.basement_key_used;
  const basementBefore = parking?.basement_ratio_before;
  const basementAfter = parking?.basement_ratio_after;

  const sourceUrl =
    parking?.requirement_meta?.source_url ??
    parking?.source_url;

  const rulesetName =
    parking?.requirement_meta?.ruleset_name ??
    parking?.ruleset_name;

  const warnings: string[] =
    Array.isArray(parking?.requirement_meta?.warnings) ? parking.requirement_meta.warnings : [];

  const requiredByComponent: AnyDict | undefined =
    parking?.required_by_component && typeof parking.required_by_component === "object"
      ? parking.required_by_component
      : undefined;

  // If nothing exists, don't render anything
  const hasAnything =
    required !== undefined ||
    provided !== undefined ||
    deficit !== undefined ||
    compliant !== undefined ||
    parkingAreaM2 !== undefined ||
    (parking && Object.keys(parking).length > 0);

  if (!hasAnything) return null;

  return (
    <section style={{ marginTop: 16 }}>
      <h3 style={{ margin: "8px 0" }}>Parking</h3>

      <div style={{ display: "grid", gap: 8 }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
          <div>Required spaces (Riyadh minimum)</div>
          <div><strong>{fmtInt(required)}</strong></div>
        </div>

        <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
          <div>Provided spaces (from basement/parking area)</div>
          <div><strong>{fmtInt(provided)}</strong></div>
        </div>

        <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
          <div>Deficit</div>
          <div><strong>{fmtInt(deficit)}</strong></div>
        </div>

        <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
          <div>Compliant</div>
          <div><strong>{yesNo(compliant)}</strong></div>
        </div>

        {parkingAreaM2 !== undefined && (
          <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
            <div>Parking area counted (m²)</div>
            <div><strong>{fmtNum(parkingAreaM2, 0)}</strong></div>
          </div>
        )}

        {policy && (
          <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
            <div>Policy</div>
            <div><strong>{String(policy)}</strong></div>
          </div>
        )}

        {Number(basementAddedM2) > 0 && (
          <div style={{ marginTop: 6, opacity: 0.9 }}>
            Auto-adjustment applied: added <strong>{fmtNum(basementAddedM2, 0)} m²</strong> to{" "}
            <strong>{String(basementKey || "basement")}</strong>
            {Number.isFinite(Number(basementBefore)) && Number.isFinite(Number(basementAfter)) ? (
              <> (ratio {fmtNum(basementBefore, 3)} → {fmtNum(basementAfter, 3)})</>
            ) : null}
            .
          </div>
        )}

        {requiredByComponent && Object.keys(requiredByComponent).length > 0 && (
          <details style={{ marginTop: 6 }}>
            <summary>Required by component</summary>
            <ul>
              {Object.entries(requiredByComponent).map(([k, v]) => (
                <li key={k}>
                  {k}: {fmtInt(v)}
                </li>
              ))}
            </ul>
          </details>
        )}

        {warnings.length > 0 && (
          <details style={{ marginTop: 6 }}>
            <summary>Notes / warnings</summary>
            <ul>
              {warnings.map((w, idx) => (
                <li key={idx}>{w}</li>
              ))}
            </ul>
          </details>
        )}

        {(rulesetName || sourceUrl) && (
          <div style={{ marginTop: 6, fontSize: 13, opacity: 0.9 }}>
            {rulesetName ? <div>Ruleset: {String(rulesetName)}</div> : null}
            {sourceUrl ? (
              <div>
                Source:{" "}
                <a href={String(sourceUrl)} target="_blank" rel="noreferrer">
                  View parking guide
                </a>
              </div>
            ) : null}
          </div>
        )}
      </div>
    </section>
  );
}
