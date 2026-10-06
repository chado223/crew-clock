/**
 * Route planning behind one interface.
 *
 * Today: a free straight-line planner (nearest neighbour + 2-opt) that needs no
 * account. Later: a road-routing provider (Google Routes, Mapbox Optimization,
 * etc.) implements the same RoutingProvider on the server with its API key, and
 * nothing else changes: the result is saved through set_route_order().
 */

export interface GeoPoint {
  lat: number;
  lon: number;
}

export interface RouteStopInput {
  id: string;
  point: GeoPoint | null;
  serviceMinutes?: number | null;
}

export interface RoutePlanInput {
  start?: GeoPoint | null; // yard; null = start at the first stop
  end?: GeoPoint | null; // defaults to start (back to the yard)
  stops: RouteStopInput[];
}

export interface RoutePlanResult {
  provider: string;
  roadBased: boolean;
  order: string[]; // every stop id, exactly once
  driveMinutes: number | null;
  driveMiles: number | null;
  unplaced: string[]; // stops without coordinates, kept at the end in their old order
}

export interface RoutingProvider {
  readonly name: string;
  readonly roadBased: boolean;
  plan(input: RoutePlanInput): Promise<RoutePlanResult>;
}

const EARTH_MILES = 3958.8;
/** Straight-line distance is shorter than the road; this factor brings it close for suburban routes. */
export const ROAD_FACTOR = 1.3;
export const AVG_MPH = 28;

export function haversineMiles(a: GeoPoint, b: GeoPoint): number {
  const rad = (d: number) => (d * Math.PI) / 180;
  const dLat = rad(b.lat - a.lat);
  const dLon = rad(b.lon - a.lon);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(rad(a.lat)) * Math.cos(rad(b.lat)) * Math.sin(dLon / 2) ** 2;
  return 2 * EARTH_MILES * Math.asin(Math.min(1, Math.sqrt(h)));
}

function pathMiles(points: GeoPoint[]): number {
  let m = 0;
  for (let i = 1; i < points.length; i++) m += haversineMiles(points[i - 1]!, points[i]!);
  return m;
}

/** Free planner: nearest neighbour from the yard, improved with 2-opt. Good to ~100 stops. */
export const straightLineProvider: RoutingProvider = {
  name: "straight_line",
  roadBased: false,
  async plan(input) {
    const placed = input.stops.filter((s): s is RouteStopInput & { point: GeoPoint } => !!s.point);
    const unplaced = input.stops.filter((s) => !s.point).map((s) => s.id);
    const start = input.start ?? placed[0]?.point ?? null;
    const end = input.end === undefined ? (input.start ?? null) : input.end;

    // nearest neighbour
    const left = [...placed];
    const tour: typeof placed = [];
    let here = start;
    while (left.length) {
      let best = 0;
      if (here) {
        let bestD = Infinity;
        left.forEach((s, i) => {
          const d = haversineMiles(here!, s.point);
          if (d < bestD) { bestD = d; best = i; }
        });
      }
      const next = left.splice(best, 1)[0]!;
      tour.push(next);
      here = next.point;
    }

    // 2-opt
    const pts = (t: typeof placed) => [...(start ? [start] : []), ...t.map((s) => s.point), ...(end ? [end] : [])];
    let improved = true;
    let guard = 0;
    while (improved && guard++ < 50) {
      improved = false;
      for (let i = 0; i < tour.length - 1; i++) {
        for (let k = i + 1; k < tour.length; k++) {
          const candidate = [...tour.slice(0, i), ...tour.slice(i, k + 1).reverse(), ...tour.slice(k + 1)];
          if (pathMiles(pts(candidate)) + 1e-9 < pathMiles(pts(tour))) {
            tour.splice(0, tour.length, ...candidate);
            improved = true;
          }
        }
      }
    }

    const miles = placed.length ? pathMiles(pts(tour)) * ROAD_FACTOR : 0;
    return {
      provider: "straight_line",
      roadBased: false,
      order: [...tour.map((s) => s.id), ...unplaced],
      driveMiles: placed.length ? Math.round(miles * 10) / 10 : null,
      driveMinutes: placed.length ? Math.round((miles / AVG_MPH) * 60) : null,
      unplaced,
    };
  },
};

/**
 * Pick the provider by name. Paid providers register here once purchased; until
 * then asking for one falls back to the free planner and says so.
 */
const PROVIDERS: Record<string, RoutingProvider> = { straight_line: straightLineProvider };

export function registerRoutingProvider(p: RoutingProvider) {
  PROVIDERS[p.name] = p;
}

export function routingProvider(name?: string | null): RoutingProvider {
  return (name && PROVIDERS[name]) || straightLineProvider;
}

// ---------------------------------------------------------------------------
// Navigation handoff
// ---------------------------------------------------------------------------

export type NavTarget = GeoPoint | string; // coordinates, or an address when there are none

const navText = (t: NavTarget) => (typeof t === "string" ? t : `${t.lat},${t.lon}`);
// Built by hand: React Native's URLSearchParams is incomplete.
const query = (params: [string, string][]) => params.map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join("&");

/** Google Maps allows a destination plus up to 9 waypoints in a link. */
export const GOOGLE_MAX_STOPS = 10;

/**
 * Directions through the next stops in order, starting from where the phone is.
 * Opens the Google Maps app when installed, the website otherwise.
 */
export function googleRouteUrl(stops: NavTarget[]): string | null {
  const s = stops.slice(0, GOOGLE_MAX_STOPS);
  if (!s.length) return null;
  const dest = s[s.length - 1]!;
  const via = s.slice(0, -1);
  const params: [string, string][] = [["api", "1"], ["destination", navText(dest)], ["travelmode", "driving"]];
  if (via.length) params.push(["waypoints", via.map(navText).join("|")]);
  return `https://www.google.com/maps/dir/?${query(params)}`;
}

/** Apple Maps takes one destination per link: the next stop. */
export function appleStopUrl(stop: NavTarget): string {
  return `https://maps.apple.com/?${query([["daddr", navText(stop)], ["dirflg", "d"]])}`;
}
