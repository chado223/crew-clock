// Message dispatcher: claims queued messages, hands them to a provider, and
// reports the result. Providers are plug-ins; until an email/SMS vendor is
// approved, the log provider "sends" by recording the message and nothing leaves.

export interface OutboundMessage {
  id: string;
  tenant_id: string;
  channel: "email" | "sms";
  mode: "test" | "live";
  delivered_to: string | null;
  subject: string | null;
  body: string;
}

export interface SendResult {
  ok: boolean;
  providerMessageId?: string;
  error?: string;
  retry?: boolean; // temporary problem: try again later
}

export interface MessageProvider {
  readonly name: string;
  readonly channel: "email" | "sms";
  send(m: OutboundMessage): Promise<SendResult>;
}

export interface MessagesDb {
  claim(limit: number): Promise<OutboundMessage[]>;
  result(id: string, ok: boolean, provider: string, providerMessageId: string | null, error: string | null, retry: boolean): Promise<void>;
  workflowTenants(): Promise<string[]>;
  queueVisitReminders(tenant: string): Promise<number>;
  queueInvoiceReminders(tenant: string): Promise<number>;
}

/** Records the message and reports it sent. Nothing leaves the system. */
export function logProvider(channel: "email" | "sms", log: (s: string) => void = () => {}): MessageProvider {
  return {
    name: "log",
    channel,
    async send(m) {
      log(`[${m.mode}] ${m.channel} -> ${m.delivered_to}: ${m.subject ?? m.body.slice(0, 60)}`);
      return { ok: true, providerMessageId: `log-${m.id}` };
    },
  };
}

export interface DispatchOptions {
  /** Providers by channel. Missing channel = log provider. */
  providers?: Partial<Record<"email" | "sms", MessageProvider>>;
  /** Second lock: live messages are only handed to a real provider when this is true. */
  allowLive?: boolean;
  limit?: number;
  log?: (s: string) => void;
}

export interface DispatchSummary {
  queuedByWorkflows: number;
  claimed: number;
  sent: number;
  failed: number;
  heldLive: number;
}

export async function dispatch(db: MessagesDb, opts: DispatchOptions = {}): Promise<DispatchSummary> {
  const log = opts.log ?? (() => {});
  const s: DispatchSummary = { queuedByWorkflows: 0, claimed: 0, sent: 0, failed: 0, heldLive: 0 };

  for (const t of await db.workflowTenants()) {
    s.queuedByWorkflows += await db.queueVisitReminders(t);
    s.queuedByWorkflows += await db.queueInvoiceReminders(t);
  }

  const batch = await db.claim(opts.limit ?? 50);
  s.claimed = batch.length;
  for (const m of batch) {
    if (m.mode === "live" && !opts.allowLive) {
      // Never deliver to a real customer from a dispatcher that isn't cleared for it.
      await db.result(m.id, false, "none", null, "live sending is not enabled on this dispatcher", true);
      s.heldLive++;
      continue;
    }
    const provider = opts.providers?.[m.channel] ?? logProvider(m.channel, log);
    if (provider.channel !== m.channel) throw new Error(`provider ${provider.name} cannot send ${m.channel}`);
    let r: SendResult;
    try {
      r = await provider.send(m);
    } catch (e) {
      r = { ok: false, error: e instanceof Error ? e.message : String(e), retry: true };
    }
    await db.result(m.id, r.ok, provider.name, r.providerMessageId ?? null, r.ok ? null : (r.error ?? "send failed"), !!r.retry);
    if (r.ok) s.sent++;
    else s.failed++;
  }
  return s;
}

/** MessagesDb over a direct Postgres connection with service_role rights. */
export interface Queryable {
  query(sql: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

export function pgMessagesDb(c: Queryable): MessagesDb {
  const n = async (sql: string, p: unknown[]) => Number((await c.query(sql, p)).rows[0]?.n ?? 0);
  return {
    async claim(limit) {
      const { rows } = await c.query(
        "select id, tenant_id, channel, mode, delivered_to, subject, body from public.messages_worker_claim($1)", [limit]);
      return rows as unknown as OutboundMessage[];
    },
    async result(id, ok, provider, pid, error, retry) {
      await c.query("select public.messages_worker_result($1, $2, $3, $4, $5, $6)", [id, ok, provider, pid, error, retry]);
    },
    async workflowTenants() {
      return (await c.query("select t::text as id from public.messages_worker_tenants() t")).rows.map((r) => String(r.id));
    },
    queueVisitReminders: (t) => n("select public.queue_visit_reminders($1) as n", [t]),
    queueInvoiceReminders: (t) => n("select public.queue_invoice_reminders($1) as n", [t]),
  };
}
