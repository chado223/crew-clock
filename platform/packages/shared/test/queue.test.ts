import { test } from "node:test";
import assert from "node:assert/strict";
import { createQueue, type QueueStorage } from "../src/queue.ts";

type A = { kind: string; n?: number };
const memory = (): QueueStorage & { data: Map<string, string> } => {
  const data = new Map<string, string>();
  return { data, get: async (k) => data.get(k) ?? null, set: async (k, v) => { data.set(k, v); }, remove: async (k) => { data.delete(k); } };
};
const netErr = () => Object.assign(new Error("Network request failed"), { network: true });
let id = 0;
const opts = (storage: QueueStorage, send: (a: A & { eventId: string }) => Promise<void>) => ({
  storage, key: "q", send, newId: () => `e${++id}`, isNetworkError: (e: unknown) => !!(e as { network?: boolean })?.network,
});

test("online: sent immediately, nothing left", async () => {
  const sent: string[] = [];
  const q = createQueue<A>(opts(memory(), async (a) => { sent.push(a.kind); }));
  assert.equal(await q.perform({ kind: "in" }), "sent");
  assert.deepEqual(sent, ["in"]);
  assert.equal((await q.pending()).length, 0);
});

test("offline: kept in order, attempts counted, sent in order when signal returns", async () => {
  let online = false;
  const sent: string[] = [];
  const s = memory();
  const q = createQueue<A>(opts(s, async (a) => { if (!online) throw netErr(); sent.push(a.kind); }));
  assert.equal(await q.perform({ kind: "in" }), "queued");
  assert.equal(await q.perform({ kind: "start_visit" }), "queued");
  assert.equal(await q.perform({ kind: "complete_visit" }), "queued");
  const p = await q.pending();
  assert.deepEqual(p.map((x) => x.kind), ["in", "start_visit", "complete_visit"]);
  assert.ok(p[0]!.attempts >= 3, "first item retried on each tap");
  online = true;
  await q.flush();
  assert.deepEqual(sent, ["in", "start_visit", "complete_visit"]);
  assert.equal((await q.pending()).length, 0);
});

test("the phone's time is kept for queued actions", async () => {
  let t = new Date("2026-10-06T12:00:00Z");
  let online = false;
  const at: string[] = [];
  const q = createQueue<A>({ ...opts(memory(), async (a) => { if (!online) throw netErr(); at.push((a as unknown as { at: string }).at); }), now: () => t });
  await q.perform({ kind: "in" });
  t = new Date("2026-10-06T14:30:00Z");
  online = true;
  await q.flush();
  assert.deepEqual(at, ["2026-10-06T12:00:00.000Z"]);
});

test("a conflict is removed and reported; later actions still go", async () => {
  let online = false;
  const sent: string[] = [];
  const q = createQueue<A>(opts(memory(), async (a) => {
    if (!online) throw netErr();
    if (a.kind === "complete_visit") throw new Error("visit_not_open");
    sent.push(a.kind);
  }));
  await q.perform({ kind: "complete_visit" });
  await q.perform({ kind: "out" });
  online = true;
  const { rejected } = await q.flush();
  assert.equal(rejected.length, 1);
  assert.match(String((rejected[0]!.error as Error).message), /visit_not_open/);
  assert.deepEqual(sent, ["out"]);
  assert.equal((await q.pending()).length, 0);
});

test("refusal of the action just taken is thrown to the screen", async () => {
  const q = createQueue<A>(opts(memory(), async () => { throw new Error("already_clocked_in"); }));
  await assert.rejects(q.perform({ kind: "in" }), /already_clocked_in/);
});

test("two flushes at once never send an action twice", async () => {
  let gate!: () => void;
  const opened = new Promise<void>((r) => { gate = r; });
  const sends: string[] = [];
  const s = memory();
  let online = false;
  const q = createQueue<A>(opts(s, async (a) => { if (!online) throw netErr(); sends.push(a.eventId); await opened; }));
  await q.perform({ kind: "in" });
  online = true;
  const a = q.flush();
  const b = q.flush();
  gate();
  await Promise.all([a, b]);
  assert.equal(sends.length, 1);
});

test("an action added while another is sending is not lost", async () => {
  let release!: () => void;
  const hold = new Promise<void>((r) => { release = r; });
  const sent: string[] = [];
  const q = createQueue<A>(opts(memory(), async (a) => { sent.push(a.kind); if (a.kind === "in") await hold; }));
  const first = q.perform({ kind: "in" });
  await new Promise((r) => setTimeout(r, 5));
  const second = q.perform({ kind: "out" });
  release();
  assert.equal(await first, "sent");
  assert.equal(await second, "sent");
  assert.deepEqual(sent, ["in", "out"]);
});

test("old app version's saved punches are carried forward first", async () => {
  const s = memory();
  s.data.set("legacy", JSON.stringify([{ kind: "in", eventId: "old1", at: "2026-10-01T10:00:00Z", attempts: 2 }]));
  const sent: string[] = [];
  const q = createQueue<A>({ ...opts(s, async (a) => { sent.push(a.eventId); }), legacyKeys: ["legacy"] });
  await q.perform({ kind: "out" });
  assert.deepEqual(sent.slice(0, 1), ["old1"]);
  assert.equal(s.data.has("legacy"), false);
});
