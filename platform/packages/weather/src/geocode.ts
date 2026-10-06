// Address -> coordinates using the free US Census Bureau geocoder (no key).
// Only used to fill properties that have no coordinates yet; coordinates an
// office user entered are never overwritten (enforced in the database too).

import { getJson, type Fetcher } from "./nws.ts";

export interface Coords {
  latitude: number;
  longitude: number;
  matched: string;
}

export const CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress";

export function parseCensus(body: unknown): Coords | null {
  const m = (body as { result?: { addressMatches?: { coordinates?: { x?: unknown; y?: unknown }; matchedAddress?: unknown }[] } })
    ?.result?.addressMatches?.[0];
  const x = m?.coordinates?.x;
  const y = m?.coordinates?.y;
  if (typeof x !== "number" || typeof y !== "number") return null;
  return { latitude: y, longitude: x, matched: typeof m?.matchedAddress === "string" ? m.matchedAddress : "" };
}

export async function geocodeAddress(fetcher: Fetcher, address: string): Promise<Coords | null> {
  const a = address.trim();
  if (a.length < 5) return null;
  const url = `${CENSUS_URL}?address=${encodeURIComponent(a)}&benchmark=Public_AR_Current&format=json`;
  return parseCensus(await getJson(fetcher, url, 2));
}
