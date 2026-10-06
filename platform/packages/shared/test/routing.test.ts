import { test } from "node:test";
import assert from "node:assert/strict";
import { straightLineProvider, googleRouteUrl, appleStopUrl, haversineMiles, routingProvider } from "../src/routing.ts";
test("planner", async () => {
  const yard = { lat: 35.89, lon: -83.77 };
  // points along a line east of the yard, given scrambled
  const stops = [3, 1, 4, 2].map((i) => ({ id: "s" + i, point: { lat: 35.89, lon: -83.77 + i * 0.05 } }));
  const r = await straightLineProvider.plan({ start: yard, stops: [...stops, { id: "nocoords", point: null }] });
  assert.deepEqual(r.order, ["s1", "s2", "s3", "s4", "nocoords"]);
  assert.deepEqual(r.unplaced, ["nocoords"]);
  assert.ok(r.driveMiles! > 2 * haversineMiles(yard, stops[2]!.point) * 1.29);
  assert.ok(r.driveMinutes! > 0);
  // crossing case fixed by 2-opt
  const sq = [{id:"a",point:{lat:0,lon:0}},{id:"c",point:{lat:1,lon:1}},{id:"b",point:{lat:0,lon:1}},{id:"d",point:{lat:1,lon:0}}];
  const r2 = await straightLineProvider.plan({ start: null, stops: sq });
  assert.equal(r2.order.length, 4);
  assert.equal(new Set(r2.order).size, 4);
  const empty = await straightLineProvider.plan({ stops: [] });
  assert.deepEqual(empty.order, []); assert.equal(empty.driveMiles, null);
  assert.equal(routingProvider("google_routes").name, "straight_line");
});
test("nav", () => {
  const u = googleRouteUrl([{ lat: 1, lon: 2 }, "12 Main St, Seymour TN", { lat: 3, lon: 4 }])!;
  assert.match(u, /destination=3%2C4/); assert.match(u, /waypoints=1%2C2%7C12%20Main/);
  assert.equal(googleRouteUrl([]), null);
  assert.match(appleStopUrl({ lat: 1, lon: 2 }), /daddr=1%2C2/);
  assert.equal(new URL(googleRouteUrl(Array.from({length: 15}, (_, i) => ({lat: i, lon: i})))!).searchParams.get("waypoints")!.split("|").length, 9);
});
