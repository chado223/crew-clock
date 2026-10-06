import "server-only";

/**
 * Address -> coordinates with the free US Census geocoder (server only, no key).
 * Returns null when the address can't be matched.
 */
export async function geocodeUS(address: string): Promise<{ latitude: number; longitude: number } | null> {
  const a = address.trim();
  if (a.length < 5) return null;
  const url = `https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=${encodeURIComponent(a)}&benchmark=Public_AR_Current&format=json`;
  try {
    const res = await fetch(url, { cache: "no-store", signal: AbortSignal.timeout(10_000) });
    if (!res.ok) return null;
    const body = (await res.json()) as { result?: { addressMatches?: { coordinates?: { x?: number; y?: number } }[] } };
    const c = body.result?.addressMatches?.[0]?.coordinates;
    return typeof c?.x === "number" && typeof c?.y === "number" ? { latitude: c.y, longitude: c.x } : null;
  } catch {
    return null;
  }
}

export const oneLine = (p: { address_line1?: string | null; city?: string | null; region?: string | null; postal_code?: string | null }) =>
  [p.address_line1, p.city, [p.region, p.postal_code].filter(Boolean).join(" ")].filter((x) => x && String(x).trim()).join(", ");
