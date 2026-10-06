import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { coord, dailyFromPeriods, getJson, maxWindMph, parsePoint, type NwsPeriod, type Fetcher } from "./nws.ts";
import { parseCensus } from "./geocode.ts";

const periods = (JSON.parse(readFileSync(new URL("./fixtures/forecast.json", import.meta.url), "utf8")) as {
  properties: { periods: NwsPeriod[] };
}).properties.periods;

test("wind strings", () => {
  assert.equal(maxWindMph("5 mph"), 5);
  assert.equal(maxWindMph("10 to 15 mph"), 15);
  assert.equal(maxWindMph(""), null);
  assert.equal(maxWindMph(null), null);
});

test("coordinates are trimmed to 4 decimals for NWS", () => {
  assert.equal(coord(35.889912345), 35.8899);
});

test("periods become one row per local day", () => {
  const days = dailyFromPeriods(periods);
  assert.deepEqual(days.map((d) => d.date), ["2026-10-05", "2026-10-06", "2026-10-07"]);
  const [tonight, tue, wed] = days;
  // Evening with only a night period left: night drives the day
  assert.equal(tonight!.precip_pct, 30);
  assert.equal(tonight!.temp_high_f, null);
  // Tuesday: daytime values, low + prior-night rain from Monday night
  assert.deepEqual(tue, {
    date: "2026-10-06", precip_pct: 70, prior_night_precip_pct: 30, wind_mph: 15,
    temp_high_f: 71, temp_low_f: 52, summary: "Showers Likely",
  });
  // Null rain chance means none forecast
  assert.equal(wed!.precip_pct, 0);
  assert.equal(wed!.temp_low_f, 49);
});

test("overnight period after midnight belongs to that morning", () => {
  const days = dailyFromPeriods([
    { startTime: "2026-10-06T00:00:00-04:00", endTime: "2026-10-06T06:00:00-04:00", isDaytime: false, temperature: 40, probabilityOfPrecipitation: { value: 60 }, windSpeed: "5 mph" },
    { startTime: "2026-10-06T06:00:00-04:00", endTime: "2026-10-06T18:00:00-04:00", isDaytime: true, temperature: 65, probabilityOfPrecipitation: { value: 10 }, windSpeed: "5 mph" },
  ]);
  assert.equal(days.length, 1);
  assert.equal(days[0]!.temp_low_f, 40);
  assert.equal(days[0]!.prior_night_precip_pct, 60);
  assert.equal(days[0]!.precip_pct, 10);
});

test("Celsius is converted", () => {
  const [d] = dailyFromPeriods([{ startTime: "2026-10-06T06:00:00-04:00", endTime: "2026-10-06T18:00:00-04:00", isDaytime: true, temperature: 20, temperatureUnit: "C" }]);
  assert.equal(d!.temp_high_f, 68);
});

test("points response", () => {
  assert.deepEqual(parsePoint({ properties: { gridId: "MRX", gridX: 47, gridY: 61 } }), { office: "MRX", x: 47, y: 61 });
  assert.throws(() => parsePoint({ status: 404 }));
});

test("census geocoder response", () => {
  assert.deepEqual(parseCensus({ result: { addressMatches: [{ matchedAddress: "125 COURT AVE", coordinates: { x: -83.56, y: 35.87 } }] } }),
    { latitude: 35.87, longitude: -83.56, matched: "125 COURT AVE" });
  assert.equal(parseCensus({ result: { addressMatches: [] } }), null);
});

test("retries NWS 5xx, sends a User-Agent, stops on 404", async () => {
  const seen: (string | undefined)[] = [];
  let calls = 0;
  const flaky: Fetcher = async (_u, init) => {
    seen.push(init?.headers?.["User-Agent"]);
    calls++;
    return calls < 3 ? { ok: false, status: 500, json: async () => ({}) } : { ok: true, status: 200, json: async () => ({ ok: 1 }) };
  };
  assert.deepEqual(await getJson(flaky, "https://x", 3, 1), { ok: 1 });
  assert.equal(calls, 3);
  assert.ok(seen.every((ua) => ua?.startsWith("CrewClock/")));

  let n = 0;
  const missing: Fetcher = async () => { n++; return { ok: false, status: 404, json: async () => ({}) }; };
  await assert.rejects(getJson(missing, "https://x", 3, 1), /404/);
  assert.equal(n, 1);
});
