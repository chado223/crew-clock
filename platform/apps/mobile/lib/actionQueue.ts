import AsyncStorage from "@react-native-async-storage/async-storage";
import * as Crypto from "expo-crypto";
import { supabase } from "./supabase";

/**
 * Offline-safe field actions (punches and visit updates).
 *
 * Every action is saved on the phone first, then sent in order. If there's no
 * signal it stays queued and is retried. The server treats repeats as the same
 * action (punch event ids; start/complete are idempotent), so retries never
 * double-count. A real rejection is removed from the queue and reported.
 */
export type FieldAction =
  | { kind: "in"; tenantId: string }
  | { kind: "out"; tenantId: string }
  | { kind: "start_visit"; tenantId: string; visitId: string }
  | { kind: "complete_visit"; tenantId: string; visitId: string; notes?: string };

export type QueuedAction = FieldAction & { eventId: string; at: string; attempts: number };

export interface Rejection {
  action: QueuedAction;
  error: unknown;
}

const KEY = "crew.actionQueue.v2";
const LEGACY_KEY = "crew.punchQueue.v1";

async function load(): Promise<QueuedAction[]> {
  const raw = await AsyncStorage.getItem(KEY);
  const legacy = await AsyncStorage.getItem(LEGACY_KEY);
  if (legacy) {
    // Carry forward punches saved by the previous app version.
    const old = JSON.parse(legacy) as QueuedAction[];
    const merged = [...old, ...(raw ? (JSON.parse(raw) as QueuedAction[]) : [])];
    await AsyncStorage.setItem(KEY, JSON.stringify(merged));
    await AsyncStorage.removeItem(LEGACY_KEY);
    return merged;
  }
  return raw ? (JSON.parse(raw) as QueuedAction[]) : [];
}

async function save(queue: QueuedAction[]) {
  await AsyncStorage.setItem(KEY, JSON.stringify(queue));
}

export async function pendingActions() {
  return load();
}

function isNetworkError(err: unknown) {
  const msg = err instanceof Error ? err.message : String((err as { message?: unknown })?.message ?? err);
  return /network|fetch|timeout|offline|Failed to fetch/i.test(msg);
}

async function send(a: QueuedAction) {
  let result;
  switch (a.kind) {
    case "in":
      result = await supabase.rpc("clock_in", { p_tenant_id: a.tenantId, p_client_event_id: a.eventId, p_at: a.at, p_source: "mobile" });
      break;
    case "out":
      result = await supabase.rpc("clock_out", { p_tenant_id: a.tenantId, p_client_event_id: a.eventId, p_at: a.at });
      break;
    case "start_visit":
      result = await supabase.rpc("start_visit", { p_visit_id: a.visitId, p_at: a.at });
      break;
    case "complete_visit":
      result = await supabase.rpc("complete_visit", { p_visit_id: a.visitId, p_notes: a.notes ?? null, p_at: a.at });
      break;
  }
  if (result.error) throw result.error;
}

/**
 * Record an action. Resolves "sent" when the server confirmed it, "queued"
 * when offline. Throws when the server rejects THIS action so the screen can
 * explain why.
 */
export async function perform(action: FieldAction): Promise<"sent" | "queued"> {
  const a = { ...action, eventId: Crypto.randomUUID(), at: new Date().toISOString(), attempts: 0 } as QueuedAction;
  await save([...(await load()), a]);
  const { rejected } = await flush();
  const mine = rejected.find((r) => r.action.eventId === a.eventId);
  if (mine) throw mine.error;
  return (await load()).some((q) => q.eventId === a.eventId) ? "queued" : "sent";
}

/** Send queued actions oldest first; stop at the first connectivity failure. */
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
      rejected.push({ action: next, error: err });
    }
    queue = queue.slice(1);
    await save(queue);
  }
  return { rejected };
}

/** What the person should see while actions are still waiting to send. */
export function pendingClockState(queue: QueuedAction[]): { onClock: boolean; since: string } | null {
  const last = [...queue].reverse().find((q) => q.kind === "in" || q.kind === "out");
  return last ? { onClock: last.kind === "in", since: last.at } : null;
}

export function pendingVisitStatus(queue: QueuedAction[], visitId: string): "in_progress" | "completed" | null {
  const last = [...queue].reverse().find((q) => (q.kind === "start_visit" || q.kind === "complete_visit") && q.visitId === visitId);
  if (!last) return null;
  return last.kind === "complete_visit" ? "completed" : "in_progress";
}
