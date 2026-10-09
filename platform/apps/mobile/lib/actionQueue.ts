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
 * double-count. A real rejection moves to a problem list on the phone and is
 * reported to the office. Actions only ever send under the sign-in that took them.
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

/** No signal, a server hiccup or an expired sign-in: keep it and try again later. */
export function classifyError(err: unknown): "retry" | "reject" {
  const e = err as { message?: unknown; status?: number; code?: string };
  const msg = err instanceof Error ? err.message : String(e?.message ?? err);
  if (/network|fetch|timeout|offline|Failed to fetch|aborted/i.test(msg)) return "retry";
  if (/JWT|expired|not_authenticated|invalid claim|refresh token/i.test(msg) || e?.code === "PGRST301") return "retry";
  if (typeof e?.status === "number" && (e.status >= 500 || e.status === 401 || e.status === 408 || e.status === 429)) return "retry";
  if (/^5\d\d\b|Internal Server Error|Bad Gateway|Service Unavailable|Gateway Timeout/i.test(msg)) return "retry";
  return "reject";
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
  classify: classifyError,
  // Read from the phone, so it works with no signal. Each action is sent only under the sign-in that took it.
  owner: async () => (await supabase.auth.getSession()).data.session?.user.id ?? null,
  // Let the office know, so hours can be fixed; the phone keeps it on its problem list too.
  onReject: async ({ action, error }) => {
    const message = error instanceof Error ? error.message : String((error as { message?: unknown })?.message ?? error);
    const { error: reportError } = await supabase.rpc("report_sync_problem", {
      p_tenant_id: action.tenantId, p_kind: action.kind, p_at: action.at, p_error: message, p_client_event_id: action.eventId,
    });
    if (reportError) throw reportError; // kept as "not yet reported" and retried on the next send
  },
});

/** Record an action: "sent" when confirmed, "queued" when offline; throws when refused. */
export const perform = queue.perform;
/** Send waiting actions oldest first; stops at the first connectivity failure. */
export const flush = queue.flush;
export const pendingActions = queue.pending;
/** Refused actions the person hasn't dismissed yet. */
export const syncProblems = queue.problems;
export const dismissSyncProblem = queue.dismissProblem;
/** Actions saved by someone else on this phone, waiting for them to sign back in. */
export const heldForOthers = queue.heldForOthers;
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
