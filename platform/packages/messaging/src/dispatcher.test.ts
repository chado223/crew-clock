import { test } from "node:test";
import assert from "node:assert/strict";
import { dispatch, type MessageProvider, type MessagesDb, type OutboundMessage } from "./dispatcher.ts";

function fakeDb(batch: OutboundMessage[]) {
  const results: { id: string; ok: boolean; provider: string; retry: boolean; error: string | null }[] = [];
  const db: MessagesDb = {
    claim: async () => batch,
    result: async (id, ok, provider, _pid, error, retry) => { results.push({ id, ok, provider, retry, error }); },
    workflowTenants: async () => ["t1"],
    queueVisitReminders: async () => 2,
    queueInvoiceReminders: async () => 1,
  };
  return { db, results };
}
const msg = (id: string, over: Partial<OutboundMessage> = {}): OutboundMessage =>
  ({ id, tenant_id: "t1", channel: "email", mode: "test", delivered_to: "office@test", subject: "s", body: "b", ...over });

test("test-mode messages go to the log provider by default; nothing real is used", async () => {
  const { db, results } = fakeDb([msg("a"), msg("b", { channel: "sms", delivered_to: "+15555550100" })]);
  const s = await dispatch(db);
  assert.deepEqual(s, { queuedByWorkflows: 3, claimed: 2, sent: 2, failed: 0, heldLive: 0 });
  assert.ok(results.every((r) => r.provider === "log" && r.ok));
});

test("live messages are held unless the dispatcher is cleared for live sending", async () => {
  let realCalls = 0;
  const real: MessageProvider = { name: "vendor", channel: "email", send: async () => { realCalls++; return { ok: true, providerMessageId: "v1" }; } };
  const { db, results } = fakeDb([msg("live1", { mode: "live", delivered_to: "customer@x" })]);
  const s = await dispatch(db, { providers: { email: real } });
  assert.equal(s.heldLive, 1);
  assert.equal(realCalls, 0);
  assert.deepEqual(results[0], { id: "live1", ok: false, provider: "none", retry: true, error: "live sending is not enabled on this dispatcher" });

  const again = fakeDb([msg("live2", { mode: "live" })]);
  await dispatch(again.db, { providers: { email: real }, allowLive: true });
  assert.equal(realCalls, 1);
  assert.equal(again.results[0]!.provider, "vendor");
});

test("provider errors are retried, provider rejections are recorded", async () => {
  const flaky: MessageProvider = { name: "vendor", channel: "email", send: async (m) => {
    if (m.id === "boom") throw new Error("network down");
    return { ok: false, error: "invalid address" };
  } };
  const { db, results } = fakeDb([msg("boom"), msg("bad")]);
  const s = await dispatch(db, { providers: { email: flaky } });
  assert.equal(s.failed, 2);
  assert.deepEqual(results.map((r) => [r.id, r.retry, r.error]), [["boom", true, "network down"], ["bad", false, "invalid address"]]);
});
