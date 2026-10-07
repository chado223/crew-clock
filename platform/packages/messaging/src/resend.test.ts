import { test } from "node:test";
import assert from "node:assert/strict";
import { resendProvider, emailProviderFromEnv } from "./resend.ts";
import type { OutboundMessage } from "./dispatcher.ts";

const msg = (to: string): OutboundMessage => ({
  id: "m1", tenant_id: "t1", channel: "email", mode: "test", delivered_to: to, subject: "Hi", body: "Tomorrow <b>8am</b>",
});

function fakeFetch(status: number, body: unknown = { id: "re_1" }) {
  const calls: { url: string; init: RequestInit }[] = [];
  const f = (async (url: string, init: RequestInit) => {
    calls.push({ url, init });
    return new Response(typeof body === "string" ? body : JSON.stringify(body), { status });
  }) as unknown as typeof fetch;
  return { f, calls };
}

test("not live: only approved test inboxes can be reached", async () => {
  const { f, calls } = fakeFetch(200);
  const p = resendProvider({ apiKey: "k", from: "x@y.test", allowedTo: ["Inbox@Test.example"], live: false, fetchImpl: f });
  const blocked = await p.send(msg("customer@real.example"));
  assert.equal(blocked.ok, false);
  assert.equal(blocked.retry, false);
  assert.equal(calls.length, 0, "nothing was sent to the real address");
  const ok = await p.send(msg("inbox@test.example"));
  assert.deepEqual(ok, { ok: true, providerMessageId: "re_1" });
  assert.equal(calls.length, 1);
});

test("one idempotency key per message, body escaped", async () => {
  const { f, calls } = fakeFetch(200);
  const p = resendProvider({ apiKey: "k", from: "x@y.test", allowedTo: [], live: true, fetchImpl: f });
  await p.send(msg("a@b.example"));
  await p.send(msg("a@b.example"));
  const keys = calls.map((c) => (c.init.headers as Record<string, string>)["Idempotency-Key"]);
  assert.deepEqual(keys, ["crew-clock-m1", "crew-clock-m1"]);
  const body = JSON.parse(String(calls[0]!.init.body));
  assert.ok(body.html.includes("&lt;b&gt;8am&lt;/b&gt;"));
  assert.equal(body.text, "Tomorrow <b>8am</b>");
});

test("temporary failures retry, permanent ones don't", async () => {
  const p429 = resendProvider({ apiKey: "k", from: "x", allowedTo: [], live: true, fetchImpl: fakeFetch(429, "slow down").f });
  assert.equal((await p429.send(msg("a@b.example"))).retry, true);
  const p422 = resendProvider({ apiKey: "k", from: "x", allowedTo: [], live: true, fetchImpl: fakeFetch(422, "bad").f });
  assert.equal((await p422.send(msg("a@b.example"))).retry, false);
  const net = resendProvider({ apiKey: "k", from: "x", allowedTo: [], live: true,
    fetchImpl: (async () => { throw new Error("ECONNRESET"); }) as unknown as typeof fetch });
  assert.equal((await net.send(msg("a@b.example"))).retry, true);
});

test("environment: off by default; live needs both switches", () => {
  assert.equal(emailProviderFromEnv({}), undefined);
  assert.throws(() => emailProviderFromEnv({ EMAIL_PROVIDER: "resend" }));
  const p = emailProviderFromEnv({ EMAIL_PROVIDER: "resend", RESEND_API_KEY: "k", EMAIL_FROM: "x", MESSAGING_LIVE: "1" });
  assert.equal(p?.name, "resend");
});
