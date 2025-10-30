export async function identify(lng: number, lat: number) {
  const r = await fetch(`${import.meta.env.VITE_API_BASE_URL || ""}/v1/geo/identify`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ lng, lat }),
  });
  if (!r.ok) throw new Error(await r.text());
  return (await r.json()).items[0];
}

export async function landPrice(
  city: string,
  district?: string,
  provider: "srem" | "suhail" = "srem",
  parcelId?: string,
) {
  const params = new URLSearchParams({ city });
  if (district) params.set("district", district);
  params.set("provider", provider);
  if (parcelId) params.set("parcel_id", parcelId);
  const r = await fetch(
    `${import.meta.env.VITE_API_BASE_URL || ""}/v1/pricing/land?${params.toString()}`,
  );
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function makeEstimate(geometry: any, excel_inputs: any) {
  const r = await fetch(`${import.meta.env.VITE_API_BASE_URL || ""}/v1/estimates`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ geometry, excel_inputs }),
  });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}
