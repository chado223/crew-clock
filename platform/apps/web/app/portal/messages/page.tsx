import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { currentPortalAccount } from "@/lib/portal";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../portal.module.css";

export const metadata: Metadata = { title: "Messages" };
export const dynamic = "force-dynamic";

async function save(formData: FormData) {
  "use server";
  const { account } = await currentPortalAccount();
  const on = (k: string) => formData.get(k) === "on";
  const { error } = await (await supabaseServer()).rpc("portal_set_preferences", {
    p_client_id: account.client_id,
    p_email_ok: on("email_ok"),
    p_sms_ok: on("sms_ok"),
    p_visit_reminders: on("visit_reminders"),
    p_invoice_reminders: on("invoice_reminders"),
  });
  redirect(`/portal/messages${error ? `?error=${encodeURIComponent(friendlyError(error))}` : "?saved=1"}`);
}

export default async function PortalMessages({ searchParams }: { searchParams: Promise<{ error?: string; saved?: string }> }) {
  const { account } = await currentPortalAccount();
  const sp = await searchParams;
  const supabase = await supabaseServer();
  const [{ data: msgs }, { data: prefs }] = await Promise.all([
    supabase.rpc("portal_messages", { p_client_id: account.client_id }),
    supabase.rpc("portal_preferences", { p_client_id: account.client_id }),
  ]);
  const messages = (msgs ?? []) as { message_id: string; channel: string; subject: string | null; body: string; sent_at: string }[];
  const p = ((prefs ?? []) as { email_ok: boolean; sms_ok: boolean; visit_reminders: boolean; invoice_reminders: boolean }[])[0]
    ?? { email_ok: true, sms_ok: false, visit_reminders: true, invoice_reminders: true };
  const when = (iso: string) =>
    new Intl.DateTimeFormat("en-US", { timeZone: account.company_timezone, month: "short", day: "numeric", year: "numeric" }).format(new Date(iso));

  return (
    <>
      <section className={styles.section}>
        <h1>Messages</h1>
        {sp.saved && <p className="notice" role="status">Saved.</p>}
        {sp.error && <p className="error-text" role="alert">{sp.error}</p>}
        {messages.length === 0 ? (
          <p className={styles.empty}>No messages from {account.company_name} yet.</p>
        ) : (
          <ul className={styles.list}>
            {messages.map((m) => (
              <li key={m.message_id}>
                <details>
                  <summary>
                    <strong>{m.subject ?? (m.channel === "sms" ? "Text message" : "Email")}</strong>{" "}
                    <span className={styles.muted}>{when(m.sent_at)}</span>
                  </summary>
                  <p style={{ whiteSpace: "pre-wrap", marginTop: 8 }}>{m.body}</p>
                </details>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className={styles.section} aria-labelledby="prefs">
        <h2 id="prefs">How {account.company_name} can reach you</h2>
        <form action={save} className={styles.form}>
          <label className={styles.choice}><input type="checkbox" name="email_ok" defaultChecked={p.email_ok} /> Email me estimates, invoices and updates</label>
          <label className={styles.choice}><input type="checkbox" name="sms_ok" defaultChecked={p.sms_ok} /> Text me (message and data rates may apply; reply STOP to opt out)</label>
          <label className={styles.choice}><input type="checkbox" name="visit_reminders" defaultChecked={p.visit_reminders} /> Remind me the day before a visit</label>
          <label className={styles.choice}><input type="checkbox" name="invoice_reminders" defaultChecked={p.invoice_reminders} /> Remind me about unpaid invoices</label>
          <button className="button" type="submit">Save</button>
        </form>
      </section>
    </>
  );
}
