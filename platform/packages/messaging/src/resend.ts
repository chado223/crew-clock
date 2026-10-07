// Resend email provider (prepared, NOT enabled).
//
// Nothing here runs unless the dispatcher is started with EMAIL_PROVIDER=resend
// and RESEND_API_KEY set. Even then, three locks stay in place:
//   1. The database decides test vs live per message (communication_settings +
//      the platform `live_messaging` flag, which is off).
//   2. The dispatcher holds every live message unless MESSAGING_LIVE=1.
//   3. This provider refuses any address not on RESEND_ALLOWED_TO unless
//      `live` is set — so a test run can only reach our own test inboxes.
// No SDK: one HTTPS call, so there is nothing extra to install or audit.
import type { MessageProvider, OutboundMessage, SendResult } from "./dispatcher.ts";

export interface ResendConfig {
  apiKey: string;
  /** e.g. "CWLC <no-reply@mail.example.com>" — must be on a domain verified in Resend. */
  from: string;
  replyTo?: string;
  /** Addresses this provider may send to while not live (test inboxes). */
  allowedTo: string[];
  /** Third lock: only true when the owner has approved real sending. */
  live: boolean;
  fetchImpl?: typeof fetch;
}

const escapeHtml = (s: string) => s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!);

export function resendProvider(cfg: ResendConfig): MessageProvider {
  const f = cfg.fetchImpl ?? fetch;
  const allowed = new Set(cfg.allowedTo.map((a) => a.trim().toLowerCase()).filter(Boolean));
  return {
    name: "resend",
    channel: "email",
    async send(m: OutboundMessage): Promise<SendResult> {
      const to = (m.delivered_to ?? "").trim().toLowerCase();
      if (!to) return { ok: false, error: "no recipient" };
      if (!cfg.live && !allowed.has(to)) {
        return { ok: false, error: `blocked: ${to} is not an approved test inbox`, retry: false };
      }
      let res: Response;
      try {
        res = await f("https://api.resend.com/emails", {
          method: "POST",
          headers: {
            Authorization: `Bearer ${cfg.apiKey}`,
            "Content-Type": "application/json",
            // Same message id → Resend sends once, even if we retry after a timeout.
            "Idempotency-Key": `crew-clock-${m.id}`,
          },
          body: JSON.stringify({
            from: cfg.from,
            to: [to],
            subject: m.subject ?? "Message from your lawn care company",
            text: m.body,
            html: `<div style="font-family:system-ui,sans-serif;font-size:15px;line-height:1.5;white-space:pre-wrap">${escapeHtml(m.body)}</div>`,
            ...(cfg.replyTo ? { reply_to: cfg.replyTo } : {}),
          }),
        });
      } catch (e) {
        return { ok: false, error: `network: ${e instanceof Error ? e.message : String(e)}`, retry: true };
      }
      if (res.ok) {
        const j = (await res.json().catch(() => ({}))) as { id?: string };
        return { ok: true, providerMessageId: j.id };
      }
      const text = (await res.text().catch(() => "")).slice(0, 300);
      // 429 / 5xx are temporary; 4xx (bad address, unverified domain) are not.
      const retry = res.status === 429 || res.status >= 500;
      return { ok: false, error: `resend ${res.status}: ${text}`, retry };
    },
  };
}

/** Build the provider from environment, or return undefined (= log provider). */
export function emailProviderFromEnv(env: Record<string, string | undefined>): MessageProvider | undefined {
  if (env.EMAIL_PROVIDER !== "resend") return undefined;
  if (!env.RESEND_API_KEY || !env.EMAIL_FROM) throw new Error("EMAIL_PROVIDER=resend needs RESEND_API_KEY and EMAIL_FROM");
  return resendProvider({
    apiKey: env.RESEND_API_KEY,
    from: env.EMAIL_FROM,
    replyTo: env.EMAIL_REPLY_TO,
    allowedTo: (env.RESEND_ALLOWED_TO ?? "").split(","),
    live: env.MESSAGING_LIVE === "1" && env.EMAIL_LIVE_APPROVED === "yes",
  });
}
