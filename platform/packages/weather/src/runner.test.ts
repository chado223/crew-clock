import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { runWeather, type WeatherDb, type WeatherTarget } from "./runner.ts";
import type { Fetcher } from "./nws.ts";

const forecast = JSON.parse(readFileSync(new URL("./fixtures/forecast.json", import.meta.url), "utf8"));

function fakeHttp() {
  const urls: string[] = [];
  const fetcher: Fetcher = async (url) => {
    urls.push(url);
    if (url.includes("geocoding.geo.census.gov")) {
      return url.includes("Nowhere")
        ? { ok: true, status: 200, json: async () => ({ result: { addressMatches: [] } }) }
        : { ok: true, status: 200, json: async () => ({ result: { addressMatches: [{ coordinates: { x: -83.77, y: 35.89 } }] } }) };
    }
    if (url.includes("/points/")) return { ok: true, status: 200, json: async () => ({ properties: { gridId: "MRX", gridX: 47, gridY: 61 } }) };
    if (url.includes("/gridpoints/")) return { ok: true, status: 200, json: async () => forecast };
    return { ok: false, status: 404, json: async () => ({}) };
  };
  return { fetcher, urls };
}

function fakeDb(targets: WeatherTarget[]) {
  const calls: string[] = [];
  const saved: Record<string, string[]> = {};
  const db: WeatherDb = {
    targets: async () => targets,
    saveCoords: async (id) => { calls.push(`coords:${id}`); },
    savePoint: async (id, g) => { calls.push(`point:${id}:${g.office}`); },
    saveForecast: async (id, days) => { saved[id] = days.map((d) => d.date); return days.length; },
    enabledTenants: async () => ["t1"],
    evaluate: async () => 2,
  };
  return { db, calls, saved };
}

const base = { tenant_id: "t1", grid_office: null, grid_x: null, grid_y: null, first_date: "2026-10-06", last_date: "2026-10-09" };

test("geocodes, resolves grid once per property, shares one forecast per grid cell, saves only the window", async () => {
  const { fetcher, urls } = fakeHttp();
  const { db, calls, saved } = fakeDb([
    { ...base, property_id: "p1", address: "1 A St, Seymour, TN", latitude: null, longitude: null },
    { ...base, property_id: "p2", address: "2 B St", latitude: 35.9, longitude: -83.8, grid_office: "MRX", grid_x: 47, grid_y: 61 },
    { ...base, property_id: "p3", address: "Nowhere", latitude: null, longitude: null },
  ]);
  const s = await runWeather(db, { fetcher, politeMs: 0 });
  assert.equal(s.geocoded, 1);
  assert.equal(s.gridsResolved, 1);
  assert.equal(s.forecastsFetched, 1, "p1 and p2 share the MRX 47,61 forecast");
  assert.equal(urls.filter((u) => u.includes("/gridpoints/")).length, 1);
  assert.deepEqual(saved.p1, ["2026-10-06", "2026-10-07"]);
  assert.deepEqual(saved.p2, ["2026-10-06", "2026-10-07"]);
  assert.deepEqual(s.skipped, [{ property_id: "p3", reason: "address_not_found" }]);
  assert.deepEqual(calls, ["coords:p1", "point:p1:MRX"]);
  assert.equal(s.alertsOpened, 2);
});

test("one failing property does not stop the run", async () => {
  const failing: Fetcher = async (url) =>
    url.includes("/points/") ? { ok: false, status: 404, json: async () => ({}) } : { ok: true, status: 200, json: async () => forecast };
  const { db } = fakeDb([
    { ...base, property_id: "bad", address: "x", latitude: 10, longitude: 10 },
    { ...base, property_id: "good", address: "y", latitude: 35.9, longitude: -83.8, grid_office: "MRX", grid_x: 1, grid_y: 1 },
  ]);
  const s = await runWeather(db, { fetcher: failing, politeMs: 0 });
  assert.equal(s.skipped.length, 1);
  assert.match(s.skipped[0]!.reason, /404/);
  assert.equal(s.daysSaved, 2);
});
