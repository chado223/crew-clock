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

test("refusals stay on the phone's problem list until dismissed, and the office is told", async () => {
  let online = false;
  const told: string[] = [];
  const q = createQueue<A>({
    ...opts(memory(), async (a) => { if (!online) throw netErr(); if (a.kind === "in") throw new Error("punch_too_old"); }),
    onReject: async (r) => { told.push(String((r.error as Error).message)); },
  });
  await q.perform({ kind: "in" });
  online = true;
  await q.flush();
  const p = await q.problems();
  assert.equal(p.length, 1);
  assert.equal(p[0]!.error, "punch_too_old");
  assert.deepEqual(told, ["punch_too_old"]);
  await q.dismissProblem(p[0]!.action.eventId);
  assert.equal((await q.problems()).length, 0);
});

test("a server hiccup or expired sign-in is retried, not dropped", async () => {
  let mode: "500" | "ok" = "500";
  const sent: string[] = [];
  const q = createQueue<A>({
    storage: memory(), key: "q", newId: () => `h${++id}`,
    send: async (a) => { if (mode === "500") throw Object.assign(new Error("Internal Server Error"), { status: 500 }); sent.push(a.kind); },
    classify: (e) => ((e as { status?: number }).status ?? 0) >= 500 ? "retry" : "reject",
  });
  assert.equal(await q.perform({ kind: "out" }), "queued");
  assert.equal((await q.problems()).length, 0);
  mode = "ok";
  await q.flush();
  assert.deepEqual(sent, ["out"]);
});

test("shared phone: one person's saved actions are never sent under another's sign-in", async () => {
  let who: string | null = "amy";
  let online = false;
  const sentAs: string[] = [];
  const q = createQueue<A>({
    ...opts(memory(), async (a) => { if (!online) throw netErr(); sentAs.push(`${a.kind}:${who}`); }),
    owner: async () => who,
  });
  await q.perform({ kind: "out" });                  // Amy clocks out with no signal
  who = null;                                       // signs out
  online = true;
  await q.flush();
  assert.deepEqual(sentAs, [], "nothing sent while signed out");
  who = "ben";                                      // Ben signs in on the same phone
  assert.equal(await q.perform({ kind: "in" }), "sent");
  assert.deepEqual(sentAs, ["in:ben"], "only Ben's own action went");
  assert.equal(await q.heldForOthers(), 1);
  assert.equal((await q.pending()).length, 0, "Ben doesn't see Amy's action as his");
  who = "amy";
  await q.flush();
  assert.deepEqual(sentAs, ["in:ben", "out:amy"]);
  assert.equal(await q.heldForOthers(), 0);
});
