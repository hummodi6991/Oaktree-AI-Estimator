import React from "react";

// Figma asset URLs from MCP (kept remote for pixel parity; can be downloaded later into /public)
const imgMap = "https://www.figma.com/api/mcp/asset/6c2232ad-c320-4bea-85d3-790085653a7c";
const imgFrame3 = "https://www.figma.com/api/mcp/asset/4c837fbd-014f-4481-b270-d7e963f122ca";
const imgFrame = "https://www.figma.com/api/mcp/asset/e15ee4c9-5d76-4ae1-879a-e9d217948c4c";
const imgFrame1 = "https://www.figma.com/api/mcp/asset/ddfae11d-50ad-48bf-9a85-7d932c501df7";
const imgFrame2 = "https://www.figma.com/api/mcp/asset/ac779a22-3b0d-4f4f-92e0-daaac0d87625";
const imgInfoIcon = "https://www.figma.com/api/mcp/asset/4c2e49cc-3088-468d-bf22-3fa2153105aa";

export default function Desktop6Figma(props: {
  parcelLabel: string;
  onCalculateEstimate: () => void;
  exportPdfHref: string | null;
  busy: boolean;
  error: string | null;
  mapUnderlayId: string;
}) {
  return (
    <div className="bg-white relative w-[1920px] h-[3299px] overflow-hidden">
      {/* Header */}
      <div className="absolute bg-[#14312c] h-[83px] left-0 top-0 w-[1920px] overflow-hidden">
        <div className="absolute left-[59px] top-[12px] w-[237px] h-[59px] text-[#efecdc]">
          <div className="font-['Heebo',sans-serif] font-semibold text-[24px] leading-none">Oaktree Estimator</div>
          <div className="font-['Heebo',sans-serif] text-[16px] mt-[6px]">Riyadh Commercial Development</div>
        </div>

        <div className="absolute left-[768px] top-[21px] w-[464px] h-[40px] bg-[#3a524e] rounded-[6px] px-[12px] flex items-center">
          <div className="text-[#efecdc] text-[16px] font-['Heebo',sans-serif]">
            Search by parcels, streets, districts
          </div>
        </div>

        <div className="absolute left-[1560px] top-[23px] flex items-center gap-[24px]">
          <button className="bg-[#3a524e] h-[36px] rounded-[4px] px-[16px] flex items-center gap-[6px]">
            <img src={imgFrame} className="w-[18px] h-[18px]" alt="" />
            <span className="text-[#efecdc] text-[14px] font-semibold">العربية</span>
          </button>
          <div className="flex items-center gap-[4px]">
            <div className="w-[38px] h-[38px] rounded-full border border-[#efecdc] overflow-hidden">
              <img src={imgFrame3} className="w-full h-full object-cover" alt="" />
            </div>
            <div className="text-[#efecdc] text-[16px] font-semibold">Asad ur rehman</div>
            <img src={imgFrame1} className="w-[20px] h-[20px]" alt="" />
          </div>
        </div>
      </div>

      {/* Map block: keep pixel-perfect image + mount MapLibre underlay */}
      <div className="absolute left-0 top-[83px] w-[1920px] h-[560px]">
        <div id={props.mapUnderlayId} className="absolute inset-0" />
        {/* Figma image as visual fallback (can be removed once MapLibre styling matches exactly) */}
        <img src={imgMap} className="absolute inset-0 w-full h-full object-cover pointer-events-none opacity-0" alt="" />
      </div>

      {/* Parcel info bar */}
      <div className="absolute left-0 top-[643px] w-[1920px] bg-[#fffbea] px-[8px] py-[20px] flex items-center justify-center gap-[10px]">
        <div className="text-black text-[18px] font-semibold w-[1447px] text-center whitespace-pre-wrap">
          {props.parcelLabel}
        </div>

        {props.exportPdfHref ? (
          <a
            href={props.exportPdfHref}
            target="_blank"
            rel="noreferrer"
            className="bg-[#efecdc] h-[40px] rounded-[8px] px-[16px] flex items-center justify-center text-[#3a524e] font-semibold"
          >
            Export PDF
          </a>
        ) : (
          <button
            className="bg-[#efecdc] h-[40px] rounded-[8px] px-[16px] flex items-center justify-center text-[#3a524e] font-semibold opacity-70 cursor-not-allowed"
            title="Run Calculate Estimate first"
            disabled
          >
            Export PDF
          </button>
        )}

        <button className="absolute left-[1758px] top-[20px] bg-[#3a524e] h-[40px] rounded-[8px] px-[16px] text-[#efecdc] font-semibold">
          Hide Map
        </button>
      </div>

      {/* Error toast (non-invasive) */}
      {props.error ? (
        <div className="absolute left-[59px] top-[725px] bg-red-50 border border-red-200 text-red-800 px-4 py-3 rounded-lg">
          {props.error}
        </div>
      ) : null}

      {/* Controls row (pixel-positioned like Figma) */}
      <div className="absolute left-[59px] top-[745px] w-[1347px]">
        {/* Provider / overrides row (static styling; wire later to backend if needed) */}
        <div className="flex items-end gap-[16px]">
          <div className="flex flex-col gap-[4px]">
            <div className="text-[16px] font-medium text-[#171717]">Provider</div>
            <div className="bg-white border border-[#d7d7d7] rounded-[6px] w-[340px] h-[40px] px-[12px] flex items-center justify-between">
              <div className="text-[14px] text-[#171717]">Blended v1 (Suhail + Aqar)</div>
              <img src={imgFrame2} className="w-[18px] h-[18px]" alt="" />
            </div>
          </div>
          <div className="flex flex-col gap-[4px]">
            <div className="text-[16px] font-medium text-[#171717]">Override land use (optional)</div>
            <div className="bg-white border border-[#d7d7d7] rounded-[6px] w-[340px] h-[40px] px-[12px] flex items-center justify-between">
              <div className="text-[14px] text-[#171717]">auto: use parcel</div>
              <img src={imgFrame2} className="w-[18px] h-[18px]" alt="" />
            </div>
          </div>
          <div className="flex flex-col gap-[4px]">
            <div className="text-[16px] font-medium text-[#171717]">Override land price (SAR/m2, optional)</div>
            <div className="bg-white border border-[#d7d7d7] rounded-[6px] w-[340px] h-[40px] px-[12px] flex items-center justify-between">
              <div className="text-[14px] text-[#171717]">---</div>
              <img src={imgFrame2} className="w-[18px] h-[18px]" alt="" />
            </div>
          </div>
          <button className="bg-[#14312c] h-[40px] rounded-[8px] px-[16px] text-white font-semibold">
            Fetch land price
          </button>
        </div>

        {/* Checkbox row */}
        <div className="mt-[15px] flex gap-[10px]">
          {["Residential", "Retail", "Office"].map((t) => (
            <label key={t} className="flex items-center gap-[7px]">
              <input type="checkbox" className="w-[16px] h-[16px] accent-[#3a524e]" />
              <span className="text-[14px] text-[#171717]">{t}</span>
            </label>
          ))}
        </div>

        {/* Action buttons */}
        <div className="mt-[21px] flex gap-[16px]">
          <button
            onClick={props.onCalculateEstimate}
            disabled={props.busy}
            className="bg-[#14312c] h-[40px] rounded-[8px] px-[16px] text-white font-semibold disabled:opacity-70"
          >
            {props.busy ? "Working…" : "Calculate Estimate"}
          </button>
          <button className="bg-[#efecdc] h-[40px] rounded-[8px] px-[16px] text-[#171717] font-semibold">
            Scenario
          </button>
        </div>
      </div>

      {/* Right summary panel (pixel-accurate look; values can be replaced by backend results later) */}
      <div className="absolute left-[1406px] top-[730px] w-[454px] h-[385px] bg-white overflow-hidden">
        <div className="absolute left-[19px] top-[0px] text-[#3a524e] font-semibold text-[22px]">
          Construction unit costs (SAR/m²)
        </div>
        <div className="absolute left-[15px] top-[55px] w-[438px]">
          {[
            ["Residential (SAR/m²)", "2200", "SAR"],
            ["Retail (SAR/m²)", "2600", "SAR"],
            ["Office (SAR/m²)", "2400", "SAR"],
            ["Basement (SAR/m²)", "2200", "SAR"],
            ["Upper annex (non-FAR) unit cost (SAR/m²)", "2200", "SAR"]
          ].map(([label, v, u]) => (
            <div key={label} className="bg-white border-b border-[#ccc] px-[4px] py-[8px]">
              <div className="font-semibold text-[18px] text-[#171717]">{label}</div>
              <div className="flex items-center gap-[4px] text-[18px] text-[#171717] mt-[8px]">
                <div>{v}</div>
                <div>{u}</div>
                <img src={imgInfoIcon} className="w-[24px] h-[24px] ml-auto" alt="" />
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
