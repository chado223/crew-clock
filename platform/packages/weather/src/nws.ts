// National Weather Service (api.weather.gov) client and forecast parsing.
// Free, no key; NWS asks every caller to send a User-Agent that identifies the
// app and a contact. Coverage is the United States only.

export type Fetcher = (url: string, init?: { headers?: Record<string, string> }) => Promise<{
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
}>;

export interface GridPoint {
  office: string;
  x: number;
  y: number;
}

/** One forecast day as stored by weather_worker_save_forecast(). */
export interface DailyForecast {
  date: string; // YYYY-MM-DD, local to the property
  precip_pct: number | null;
  prior_night_precip_pct: number | null;
  wind_mph: number | null;
  temp_high_f: number | null;
  temp_low_f: number | null;
  summary: string | null;
}

export interface NwsPeriod {
  startTime: string;
  endTime: string;
  isDaytime: boolean;
  temperature: number | null;
  temperatureUnit?: string;
  probabilityOfPrecipitation?: { value: number | null } | null;
  windSpeed?: string | null;
  shortForecast?: string | null;
}

export const NWS_BASE = "https://api.weather.gov";

export function userAgent(contact = process.env.WEATHER_CONTACT): string {
  return `CrewClock/1.0 (${contact && contact.trim() ? contact.trim() : "github.com/chado223/crew-clock"})`;
}

export class NwsError extends Error {
  readonly status: number;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** GET JSON with NWS headers; retries the transient 5xx/429 errors NWS is known for. */
export async function getJson(fetcher: Fetcher, url: string, tries = 3, backoffMs = 1500): Promise<unknown> {
  let last: NwsError | undefined;
  for (let i = 0; i < tries; i++) {
    const res = await fetcher(url, { headers: { "User-Agent": userAgent(), Accept: "application/geo+json" } });
    if (res.ok) return res.json();
    last = new NwsError(`NWS ${res.status} for ${url}`, res.status);
    if (res.status !== 429 && res.status < 500) break; // 404 = outside coverage, etc.
    if (i < tries - 1) await sleep(backoffMs * (i + 1));
  }
  throw last ?? new NwsError(`NWS request failed for ${url}`, 0);
}

/** NWS rejects coordinates with more than 4 decimals. */
export const coord = (n: number) => Number(n.toFixed(4));

export function parsePoint(body: unknown): GridPoint {
  const p = (body as { properties?: { gridId?: unknown; gridX?: unknown; gridY?: unknown } })?.properties;
  if (!p || typeof p.gridId !== "string" || typeof p.gridX !== "number" || typeof p.gridY !== "number") {
    throw new NwsError("Unexpected NWS points response", 0);
  }
  return { office: p.gridId, x: p.gridX, y: p.gridY };
}

export async function resolvePoint(fetcher: Fetcher, lat: number, lon: number): Promise<GridPoint> {
  return parsePoint(await getJson(fetcher, `${NWS_BASE}/points/${coord(lat)},${coord(lon)}`));
}

export function forecastUrl(g: GridPoint): string {
  return `${NWS_BASE}/gridpoints/${encodeURIComponent(g.office)}/${g.x},${g.y}/forecast?units=us`;
}

export async function fetchPeriods(fetcher: Fetcher, g: GridPoint): Promise<NwsPeriod[]> {
  const body = (await getJson(fetcher, forecastUrl(g))) as { properties?: { periods?: unknown } };
  const periods = body?.properties?.periods;
  if (!Array.isArray(periods)) throw new NwsError("Unexpected NWS forecast response", 0);
  return periods as NwsPeriod[];
}

/** "5 mph" | "5 to 10 mph" | "10 to 20 mph" -> highest number. */
export function maxWindMph(s: string | null | undefined): number | null {
  if (!s) return null;
  const nums = (s.match(/\d+/g) ?? []).map(Number);
  return nums.length ? Math.max(...nums) : null;
}

function toF(t: number | null, unit?: string): number | null {
  if (t === null || t === undefined || Number.isNaN(t)) return null;
  return unit === "C" ? Math.round((t * 9) / 5 + 32) : Math.round(t);
}

const pop = (p: NwsPeriod) => p.probabilityOfPrecipitation?.value ?? null;

/**
 * Turns NWS 12-hour periods into one row per local day:
 *  - daytime period drives rain chance, wind, high and summary (that's when crews work)
 *  - a day with no daytime period left (late "Tonight") uses its night period
 *  - the night ending on a day's morning gives that day's low and prior-night rain chance
 * Dates come from the period's own offset, i.e. local to the property.
 */
export function dailyFromPeriods(periods: NwsPeriod[]): DailyForecast[] {
  const days = new Map<string, DailyForecast>();
  const get = (date: string) => {
    let d = days.get(date);
    if (!d) {
      d = { date, precip_pct: null, prior_night_precip_pct: null, wind_mph: null, temp_high_f: null, temp_low_f: null, summary: null };
      days.set(date, d);
    }
    return d;
  };
  const hasDay = new Set(periods.filter((p) => p.isDaytime).map((p) => p.startTime.slice(0, 10)));

  for (const p of periods) {
    const start = p.startTime.slice(0, 10);
    if (p.isDaytime) {
      const d = get(start);
      d.precip_pct = pop(p) ?? 0;
      d.wind_mph = maxWindMph(p.windSpeed);
      d.temp_high_f = toF(p.temperature, p.temperatureUnit);
      d.summary = p.shortForecast ?? null;
    } else {
      const end = p.endTime.slice(0, 10);
      if (end !== start || p.startTime.slice(11, 13) < "12") {
        // Night that ends on the morning of `end` (or an early "Overnight" period).
        const e = get(end);
        e.temp_low_f = toF(p.temperature, p.temperatureUnit);
        e.prior_night_precip_pct = pop(p) ?? 0;
      }
      if (!hasDay.has(start) && !(p.startTime.slice(11, 13) < "12")) {
        const d = get(start);
        d.precip_pct = Math.max(d.precip_pct ?? 0, pop(p) ?? 0);
        d.wind_mph = Math.max(d.wind_mph ?? 0, maxWindMph(p.windSpeed) ?? 0);
        d.summary = d.summary ?? p.shortForecast ?? null;
      }
    }
  }
  return [...days.values()]
    .filter((d) => d.precip_pct !== null || d.temp_high_f !== null)
    .sort((a, b) => a.date.localeCompare(b.date));
}
