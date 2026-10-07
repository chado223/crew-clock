// One weather pass for every company that has weather turned on.
// Storage is behind WeatherDb so the same pass runs from a GitHub Actions job
// (staging today), a hosted cron route, or a Supabase scheduled function later.

import { dailyFromPeriods, fetchPeriods, resolvePoint, type DailyForecast, type Fetcher, type GridPoint } from "./nws.ts";
import { geocodeAddress } from "./geocode.ts";

export interface WeatherTarget {
  tenant_id: string;
  property_id: string;
  address: string;
  latitude: number | null;
  longitude: number | null;
  grid_office: string | null;
  grid_x: number | null;
  grid_y: number | null;
  first_date: string;
  last_date: string;
}

export interface WeatherDb {
  targets(): Promise<WeatherTarget[]>;
  saveCoords(propertyId: string, lat: number, lon: number): Promise<void>;
  savePoint(propertyId: string, g: GridPoint): Promise<void>;
  saveForecast(propertyId: string, days: DailyForecast[]): Promise<number>;
  enabledTenants(): Promise<string[]>;
  evaluate(tenantId: string): Promise<number>;
}

export interface RunSummary {
  properties: number;
  geocoded: number;
  gridsResolved: number;
  forecastsFetched: number;
  daysSaved: number;
  alertsOpened: number;
  skipped: { property_id: string; reason: string }[];
}

export interface RunOptions {
  fetcher: Fetcher;
  log?: (msg: string) => void;
  /** Pause between outbound calls; NWS and Census are shared public services. */
  politeMs?: number;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function runWeather(db: WeatherDb, opts: RunOptions): Promise<RunSummary> {
  const log = opts.log ?? (() => {});
  const polite = opts.politeMs ?? 250;
  const s: RunSummary = { properties: 0, geocoded: 0, gridsResolved: 0, forecastsFetched: 0, daysSaved: 0, alertsOpened: 0, skipped: [] };
  const forecasts = new Map<string, DailyForecast[]>(); // one fetch per NWS grid cell, shared by neighbors

  const targets = await db.targets();
  s.properties = targets.length;
  log(`${targets.length} properties need a forecast`);

  for (const t of targets) {
    try {
      let lat = t.latitude;
      let lon = t.longitude;
      if (lat === null || lon === null) {
        const c = await geocodeAddress(opts.fetcher, t.address);
        await sleep(polite);
        if (!c) {
          s.skipped.push({ property_id: t.property_id, reason: "address_not_found" });
          continue;
        }
        lat = c.latitude;
        lon = c.longitude;
        await db.saveCoords(t.property_id, lat, lon);
        s.geocoded++;
      }

      let g: GridPoint;
      if (t.grid_office && t.grid_x !== null && t.grid_y !== null) {
        g = { office: t.grid_office, x: t.grid_x, y: t.grid_y };
      } else {
        g = await resolvePoint(opts.fetcher, lat, lon);
        await sleep(polite);
        await db.savePoint(t.property_id, g);
        s.gridsResolved++;
      }

      const key = `${g.office}/${g.x},${g.y}`;
      let days = forecasts.get(key);
      if (!days) {
        days = dailyFromPeriods(await fetchPeriods(opts.fetcher, g));
        await sleep(polite);
        forecasts.set(key, days);
        s.forecastsFetched++;
      }
      const wanted = days.filter((d) => d.date >= t.first_date && d.date <= t.last_date);
      if (wanted.length) s.daysSaved += await db.saveForecast(t.property_id, wanted);
    } catch (e) {
      const reason = e instanceof Error ? e.message : String(e);
      s.skipped.push({ property_id: t.property_id, reason });
      log(`skip ${t.property_id}: ${reason}`);
    }
  }

  for (const tenant of await db.enabledTenants()) {
    // One company's problem doesn't stop the others from getting their alerts.
    try {
      s.alertsOpened += await db.evaluate(tenant);
    } catch (e) {
      const reason = e instanceof Error ? e.message : String(e);
      s.skipped.push({ property_id: `company:${tenant}`, reason });
      log(`evaluate ${tenant} failed: ${reason}`);
    }
  }
  log(`done: ${JSON.stringify({ ...s, skipped: s.skipped.length })}`);
  return s;
}
