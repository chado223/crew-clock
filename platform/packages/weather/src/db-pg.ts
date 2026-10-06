// WeatherDb over a direct Postgres connection. The connecting role must be able
// to run the service-only weather_worker_* functions (service_role/postgres).

import type { DailyForecast, GridPoint } from "./nws.ts";
import type { WeatherDb, WeatherTarget } from "./runner.ts";

export interface Queryable {
  query(sql: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

const iso = (v: unknown) => (v instanceof Date ? v.toISOString().slice(0, 10) : String(v));

export function pgWeatherDb(c: Queryable): WeatherDb {
  return {
    async targets() {
      const { rows } = await c.query("select * from public.weather_worker_targets()");
      return rows.map((r) => ({ ...r, first_date: iso(r.first_date), last_date: iso(r.last_date) }) as unknown as WeatherTarget);
    },
    async saveCoords(id, lat, lon) {
      await c.query("select public.weather_worker_save_coords($1, $2, $3, 'census')", [id, lat, lon]);
    },
    async savePoint(id, g: GridPoint) {
      await c.query("select public.weather_worker_save_point($1, $2, $3, $4)", [id, g.office, g.x, g.y]);
    },
    async saveForecast(id, days: DailyForecast[]) {
      const { rows } = await c.query("select public.weather_worker_save_forecast($1, $2::jsonb) as n", [id, JSON.stringify(days)]);
      return Number(rows[0]?.n ?? 0);
    },
    async enabledTenants() {
      const { rows } = await c.query("select t::text as id from public.weather_worker_tenants() t");
      return rows.map((r) => String(r.id));
    },
    async evaluate(tenant) {
      const { rows } = await c.query("select public.weather_evaluate($1) as n", [tenant]);
      return Number(rows[0]?.n ?? 0);
    },
  };
}
