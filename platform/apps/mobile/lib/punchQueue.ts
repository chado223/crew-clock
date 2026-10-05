import AsyncStorage from "@react-native-async-storage/async-storage";
import * as Crypto from "expo-crypto";
import { supabase } from "./supabase";

/**
 * Offline-safe punches.
 *
 * Every punch gets a client event id (UUID) and the time it happened on the
 * device. It is saved locally first, then sent. If the send fails because of
 * connectivity, it stays queued and is retried later. The server treats a
 * repeated event id as the same punch, so retries can never double-count.
 */
export type PunchKind = "in" | "out";

export interface QueuedPunch {
  eventId: string;
  kind: PunchKind;
  tenantId: string;
  at: string;
  attempts: number;
}

const KEY = "crew.punchQueue.v1";

async function load(): Promise<QueuedPunch[]> {
  const raw = await AsyncStorage.getItem(KEY);
  return raw ? (JSON.parse(raw) as QueuedPunch[]) : [];
}

async function save(queue: QueuedPunch[]) {
  await AsyncStorage.setItem(KEY, JSON.stringify(queue));
}

export async function pendingPunches() {
  return load();
}

function isNetworkError(err: unknown) {
  const msg = err instanceof Error ? err.message : String((err as { message?: unknown })?.message ?? err);
  return /network|fetch|timeout|offline|Failed to fetch/i.test(msg);
}

async function send(p: QueuedPunch) {
  const fn = p.kind === "in" ? "clock_in" : "clock_out";
  const args =
    p.kind === "in"
      ? { p_tenant_id: p.tenantId, p_client_event_id: p.eventId, p_at: p.at, p_source: "mobile" }
      : { p_tenant_id: p.tenantId, p_client_event_id: p.eventId, p_at: p.at };
  const { error } = await supabase.rpc(fn, args);
  if (error) throw error;
}

export interface Rejection {
  punch: QueuedPunch;
  error: unknown;
}

/**
 * Record a punch. Resolves "sent" when the server confirmed it, "queued" when
 * offline. Throws when the server rejects THIS punch (e.g. already clocked
 * in) so the screen can explain it.
 */
export async function punch(kind: PunchKind, tenantId: string): Promise<"sent" | "queued"> {
  const p: QueuedPunch = { eventId: Crypto.randomUUID(), kind, tenantId, at: new Date().toISOString(), attempts: 0 };
  await save([...(await load()), p]);
  const { rejected } = await flush();
  const mine = rejected.find((r) => r.punch.eventId === p.eventId);
  if (mine) throw mine.error;
  return (await load()).some((q) => q.eventId === p.eventId) ? "queued" : "sent";
}

/**
 * Send queued punches oldest first. Stops at the first connectivity failure
 * (order matters: an "out" must follow its "in"). A punch the server rejects
 * is removed and reported, never retried forever.
 */
export async function flush(): Promise<{ rejected: Rejection[] }> {
  const rejected: Rejection[] = [];
  let queue = await load();
  while (queue.length > 0) {
    const next = queue[0]!;
    try {
      await send(next);
    } catch (err) {
      if (isNetworkError(err)) {
        next.attempts += 1;
        await save(queue);
        break;
      }
      rejected.push({ punch: next, error: err });
    }
    queue = queue.slice(1);
    await save(queue);
  }
  return { rejected };
}
