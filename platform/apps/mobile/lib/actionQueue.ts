import AsyncStorage from "@react-native-async-storage/async-storage";
import * as Crypto from "expo-crypto";
import { createQueue, type Queued } from "@crew/shared";
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
  | { kind: "complete_visit"; tenantId: string; visitId: string; notes?: string }
  | { kind: "break_start"; tenantId: string }
  | { kind: "break_end"; tenantId: string }
  | { kind: "report_problem"; tenantId: string; visitId: string; reason: string };

export type QueuedAction = Queued<FieldAction>;

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
    case "break_start":
      result = await supabase.rpc("start_break", { p_tenant_id: a.tenantId, p_client_event_id: a.eventId, p_at: a.at });
      break;
    case "break_end":
      result = await supabase.rpc("end_break", { p_tenant_id: a.tenantId, p_at: a.at });
      // Already ended (a retry after a lost reply) is the result we wanted.
      if (result.error && /not_on_break/.test(result.error.message)) return;
      break;
    case "report_problem":
      result = await supabase.rpc("report_visit_problem", { p_visit_id: a.visitId, p_reason: a.reason, p_at: a.at });
      break;
    case "complete_visit":
      result = await supabase.rpc("complete_visit", { p_visit_id: a.visitId, p_notes: a.notes ?? null, p_at: a.at });
      break;
  }
  if (result.error) throw result.error;
}

const queue = createQueue<FieldAction>({
  storage: {
    get: (k) => AsyncStorage.getItem(k),
    set: (k, v) => AsyncStorage.setItem(k, v),
    remove: (k) => AsyncStorage.removeItem(k),
  },
  key: "crew.actionQueue.v2",
  legacyKeys: ["crew.punchQueue.v1"],
  send,
  newId: () => Crypto.randomUUID(),
  isNetworkError: (err) => {
    const msg = err instanceof Error ? err.message : String((err as { message?: unknown })?.message ?? err);
    return /network|fetch|timeout|offline|Failed to fetch/i.test(msg);
  },
});

/** Record an action: "sent" when confirmed, "queued" when offline; throws when refused. */
export const perform = queue.perform;
/** Send waiting actions oldest first; stops at the first connectivity failure. */
export const flush = queue.flush;
export const pendingActions = queue.pending;
export type { Rejection } from "@crew/shared";

/** What the person should see while actions are still waiting to send. */
export function pendingClockState(queue: QueuedAction[]): { onClock: boolean; since: string } | null {
  const last = [...queue].reverse().find((q) => q.kind === "in" || q.kind === "out");
  return last ? { onClock: last.kind === "in", since: last.at } : null;
}

export function pendingVisitStatus(queue: QueuedAction[], visitId: string): "in_progress" | "completed" | "skipped" | null {
  const last = [...queue].reverse().find(
    (q) => (q.kind === "start_visit" || q.kind === "complete_visit" || q.kind === "report_problem") && q.visitId === visitId,
  );
  if (!last) return null;
  return last.kind === "complete_visit" ? "completed" : last.kind === "report_problem" ? "skipped" : "in_progress";
}

export function pendingBreakState(queue: QueuedAction[]): boolean | null {
  const last = [...queue].reverse().find((q) => q.kind === "break_start" || q.kind === "break_end" || q.kind === "out");
  return last ? last.kind === "break_start" : null;
}
