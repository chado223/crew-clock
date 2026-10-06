// Weather worker command line.
//
//   node src/cli.ts probe [lat] [lon]   live NWS forecast for a point (no database)
//   node src/cli.ts run                 one real pass against DB_URL
//   node src/cli.ts e2e                 full pass against DB_URL on a throwaway test
//                                       company inside a transaction, then ROLLBACK
//
// DB_URL must be a server-side connection (never shipped to browsers or phones).

import { dailyFromPeriods, fetchPeriods, resolvePoint, type Fetcher } from "./nws.ts";
import { runWeather } from "./runner.ts";
import { pgWeatherDb } from "./db-pg.ts";

const fetcher: Fetcher = (url, init) => fetch(url, init);
const log = (m: string) => console.log(`[weather] ${m}`);

function fail(msg: string): never {
  console.log(`::error title=weather::${msg}`);
  throw new Error(msg);
}

function guardDbUrl(url: string | undefined): string {
  if (!url) fail("DB_URL is not set");
  // Never run worker tests against production from this tool.
  if (url.includes("iwowjrnrbjiydckhjsfi") && process.argv[2] === "e2e") fail("Refusing to run e2e against production");
  return url;
}

async function connect() {
  const { default: pg } = await import("pg");
  const client = new pg.Client({ connectionString: guardDbUrl(process.env.DB_URL), ssl: process.env.DB_SSL === "off" ? false : { rejectUnauthorized: false } });
  await client.connect();
  return client;
}

async function probe(lat: number, lon: number) {
  const g = await resolvePoint(fetcher, lat, lon);
  const days = dailyFromPeriods(await fetchPeriods(fetcher, g));
  log(`grid ${g.office} ${g.x},${g.y}`);
  for (const d of days) {
    log(`${d.date}  rain ${d.precip_pct ?? "-"}%  wind ${d.wind_mph ?? "-"} mph  high ${d.temp_high_f ?? "-"}F  low ${d.temp_low_f ?? "-"}F  ${d.summary ?? ""}`);
  }
  if (days.length < 5) fail(`expected about a week of forecast days, got ${days.length}`);
  console.log(`::notice title=weather-probe::NWS ${g.office} ${g.x},${g.y}: ${days.length} days, first ${days[0]?.date}`);
}

async function run() {
  const c = await connect();
  try {
    const s = await runWeather(pgWeatherDb(c), { fetcher, log });
    console.log(`::notice title=weather-run::${JSON.stringify({ ...s, skipped: s.skipped.length })}`);
  } finally {
    await c.end();
  }
}

// A real address (a public county courthouse) so the free geocoder and NWS are exercised end to end.
const E2E_ADDRESS = { line1: "125 Court Ave", city: "Sevierville", region: "TN", postal: "37862" };

async function e2e() {
  const c = await connect();
  try {
    await c.query("begin");
    const t = (await c.query(`insert into public.tenants (name, timezone) values ('Weather E2E (rolled back)', 'America/New_York') returning id`)).rows[0]!.id;
    const cl = (await c.query(`insert into public.clients (tenant_id, name) values ($1, 'E2E Customer') returning id`, [t])).rows[0]!.id;
    const p = (await c.query(
      `insert into public.properties (tenant_id, client_id, address_line1, city, region, postal_code) values ($1,$2,$3,$4,$5,$6) returning id`,
      [t, cl, E2E_ADDRESS.line1, E2E_ADDRESS.city, E2E_ADDRESS.region, E2E_ADDRESS.postal])).rows[0]!.id;
    const j = (await c.query(`insert into public.jobs (tenant_id, client_id, property_id, title) values ($1,$2,$3,'E2E mow') returning id`, [t, cl, p])).rows[0]!.id;
    await c.query(`insert into public.visits (tenant_id, job_id, scheduled_date) values ($1, $2, private.tenant_today($1) + 1)`, [t, j]);
    // A minimum temperature no real day reaches guarantees exactly one 'cold' alert.
    await c.query(`insert into public.weather_settings (tenant_id, enabled, min_temp_f, max_temp_f) values ($1, true, 119, 140)`, [t]);

    const db = pgWeatherDb(c);
    const s = await runWeather(db, { fetcher, log });
    const mine = await c.query(
      `select a.reasons, f.precip_pct, f.temp_high_f, p.latitude, p.geocode_source, wp.grid_office
         from public.properties p
         left join public.weather_points wp on wp.property_id = p.id
         left join public.weather_forecasts f on f.property_id = p.id
         left join public.weather_alerts a on a.property_id = p.id and a.forecast_date = f.forecast_date
        where p.id = $1 order by f.forecast_date`, [p]);
    const r = mine.rows;
    log(JSON.stringify(r));
    if (!r[0]?.latitude || r[0]?.geocode_source !== "census") fail("property was not geocoded");
    if (!r[0]?.grid_office) fail("NWS grid point was not saved");
    if (!r.some((x) => x.temp_high_f !== null)) fail("no forecast days saved");
    if (!r.some((x) => Array.isArray(x.reasons) && (x.reasons as string[]).includes("cold"))) fail("expected a weather alert on tomorrow's visit");
    const again = await db.evaluate(t);
    if (again !== 0) fail("second evaluation should open nothing");
    console.log(`::notice title=weather-e2e::geocoded, grid ${r[0]?.grid_office}, ${s.daysSaved} days saved, ${s.alertsOpened} alert(s); rolled back`);
  } finally {
    await c.query("rollback").catch(() => {});
    await c.end();
  }
}

const [cmd, a, b] = process.argv.slice(2);
const job = cmd === "probe" ? probe(Number(a ?? 35.8906), Number(b ?? -83.7743))
  : cmd === "run" ? run()
  : cmd === "e2e" ? e2e()
  : Promise.reject(new Error("usage: cli.ts probe [lat lon] | run | e2e"));
job.catch((e) => {
  console.error(e instanceof Error ? e.message : e);
  process.exit(1);
});
