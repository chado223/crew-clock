import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import { describeMessage, MESSAGE_COLUMNS, TEMPLATE_LABEL, type MessageRow } from "@/lib/messages";
import styles from "./messages.module.css";

export const metadata: Metadata = { title: "Messages" };
export const dynamic = "force-dynamic";

type Settings = {
  delivery_mode: "off" | "test" | "live";
  test_email: string | null;
  test_phone: string | null;
  from_name: string | null;
  reply_to: string | null;
  portal_url: string | null;
  visit_reminders: boolean;
  invoice_reminders: boolean;
  invoice_reminder_days: number;
};
type Template = { template_key: string; channel: "email" | "sms"; label: string; subject: string | null; body: string; variables: string[] };
type Override = { id: string; template_key: string; channel: string; subject: string | null; body: string; active: boolean };

const DEFAULTS: Settings = {
  delivery_mode: "test", test_email: null, test_phone: null, from_name: null, reply_to: null, portal_url: null,
  visit_reminders: false, invoice_reminders: false, invoice_reminder_days: 7,
};
const go = (q: string): never => redirect(`/messages?${q}`);
const err = (e: unknown): never => go(`error=${encodeURIComponent(
  e && typeof e === "object" && "message" in e && String((e as { message: string }).message).includes("check")
    ? "Check the addresses: one isn't in a valid format." : friendlyError(e))}`);

async function saveSettings(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const t = (k: string) => String(formData.get(k) ?? "").trim() || null;
  const mode = t("delivery_mode");
  const { error } = await (await supabaseServer()).from("communication_settings").upsert(
    {
      tenant_id: company.tenant_id,
      delivery_mode: mode === "off" ? "off" : "test",   // live is never set from here without owner approval
      test_email: t("test_email"),
      test_phone: t("test_phone")?.replace(/[^\d+]/g, "") ?? null,
      from_name: t("from_name"),
      reply_to: t("reply_to"),
      portal_url: t("portal_url"),
      visit_reminders: formData.get("visit_reminders") === "on",
      invoice_reminders: formData.get("invoice_reminders") === "on",
      invoice_reminder_days: Number(t("invoice_reminder_days") ?? 7),
    },
    { onConflict: "tenant_id" },
  );
  if (error) err(error);
  go("done=settings");
}

async function saveTemplate(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const supabase = await supabaseServer();
  const key = String(formData.get("template_key"));
  const channel = String(formData.get("channel"));
  const row = { subject: String(formData.get("subject") ?? "").trim() || null, body: String(formData.get("body") ?? "").trim(), active: true };
  const id = String(formData.get("override_id") ?? "");
  const { error } = id
    ? await supabase.from("message_templates").update(row).eq("id", id)
    : await supabase.from("message_templates").insert({ ...row, tenant_id: company.tenant_id, template_key: key, channel });
  if (error) err(error);
  go("done=template");
}

async function resetTemplate(formData: FormData) {
  "use server";
  const { error } = await (await supabaseServer()).from("message_templates").update({ active: false }).eq("id", String(formData.get("override_id")));
  if (error) err(error);
  go("done=reset");
}

async function cancel(formData: FormData) {
  "use server";
  const { error } = await (await supabaseServer()).rpc("cancel_message", { p_message_id: String(formData.get("id")) });
  if (error) err(error);
  go("done=canceled");
}

const DONE: Record<string, string> = {
  settings: "Message settings saved.",
  template: "Template saved. New messages use it.",
  reset: "Back to the built-in wording.",
  canceled: "Message canceled.",
};

export default async function MessagesPage({ searchParams }: { searchParams: Promise<{ error?: string; done?: string }> }) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const supabase = await supabaseServer();
  const [{ data: s }, { data: msgs }, { data: defaults }, { data: overrides }] = await Promise.all([
    supabase.from("communication_settings").select("*").eq("tenant_id", company.tenant_id).maybeSingle(),
    supabase.from("messages").select(`${MESSAGE_COLUMNS}, client_id, clients(name)`).eq("tenant_id", company.tenant_id)
      .order("created_at", { ascending: false }).limit(100),
    supabase.rpc("default_message_templates"),
    supabase.from("message_templates").select("id, template_key, channel, subject, body, active").eq("tenant_id", company.tenant_id),
  ]);
  const settings: Settings = { ...DEFAULTS, ...((s as Partial<Settings> | null) ?? {}) };
  const rows = (msgs ?? []) as unknown as (MessageRow & { client_id: string | null; clients: { name: string } | null })[];
  const templates = (defaults ?? []) as Template[];
  const custom = (overrides ?? []) as Override[];
  const when = (iso: string) =>
    new Intl.DateTimeFormat("en-US", { timeZone: company.timezone, month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(new Date(iso));

  const modeText =
    settings.delivery_mode === "off"
      ? { title: "Messages are off", body: "Nothing is sent. Sends are still recorded so you can see what would have gone out." }
      : settings.delivery_mode === "live"
        ? { title: "Live", body: "Messages go to customers and employees." }
        : {
            title: "Test mode",
            body: settings.test_email || settings.test_phone
              ? `Every message goes only to your test address${settings.test_email ? ` (${settings.test_email}` : ""}${settings.test_phone ? `${settings.test_email ? ", " : " ("}${settings.test_phone}` : ""}). Customers receive nothing. The real recipient is shown so you can check it.`
              : "Every message would go only to a test address, and none is set, so nothing goes out. Add one below to see what customers would get.",
          };

  return (
    <div className={styles.page}>
      <header>
        <h1>Messages</h1>
        <p className={styles.lede}>Estimates, invoices, reminders and invites sent by email or text, and what happened to each one.</p>
      </header>

      <div className={`${styles.mode} ${settings.delivery_mode === "off" ? styles.modeOff : ""}`} role="status">
        <strong>{modeText.title}</strong>
        <span>{modeText.body}</span>
        {settings.delivery_mode !== "live" && (
          <span className={styles.muted}>Real delivery to customers is switched on later, once an email/text provider is set up and approved.</span>
        )}
      </div>

      {sp.done && <p className="notice" role="status">{DONE[sp.done] ?? "Saved."}</p>}
      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}

      <section aria-labelledby="history">
        <h2 id="history">Recent</h2>
        {rows.length === 0 ? (
          <p className={styles.muted}>Nothing yet. Use “Email to customer” on an estimate or invoice.</p>
        ) : (
          <table className={styles.table}>
            <thead>
              <tr><th>When</th><th>What</th><th>Customer</th><th>Status</th><th>Details</th><th><span className={styles.srOnly}>Actions</span></th></tr>
            </thead>
            <tbody>
              {rows.map((m) => (
                <tr key={m.id}>
                  <td>{when(m.created_at)}</td>
                  <td>{TEMPLATE_LABEL[m.template_key] ?? m.template_key}<span className={styles.muted}> · {m.channel === "sms" ? "text" : "email"}</span></td>
                  <td>{m.client_id ? <Link href={`/clients/${m.client_id}`}>{m.clients?.name ?? "Customer"}</Link> : m.to_address ?? "–"}</td>
                  <td className={`${styles.status} ${styles[`s_${m.status}`] ?? ""}`}>{m.mode === "test" && m.status !== "suppressed" ? `${m.status} (test)` : m.status}</td>
                  <td className={styles.muted}>{describeMessage(m)}</td>
                  <td>
                    {m.status === "queued" && (
                      <form action={cancel}><input type="hidden" name="id" value={m.id} /><button className={styles.textButton} type="submit">Cancel</button></form>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <details className={styles.panel} open={!s}>
        <summary>Settings</summary>
        <form action={saveSettings} className={styles.form}>
          <div className="field">
            <label htmlFor="delivery_mode">Delivery</label>
            <select id="delivery_mode" name="delivery_mode" defaultValue={settings.delivery_mode === "off" ? "off" : "test"} className="select">
              <option value="test">Test mode (test address only)</option>
              <option value="off">Off</option>
              <option value="live" disabled>Live (needs approval)</option>
            </select>
          </div>
          <div className="field">
            <label htmlFor="test_email">Test email</label>
            <input id="test_email" name="test_email" type="email" defaultValue={settings.test_email ?? ""} className="input" placeholder="you@yourcompany.com" />
          </div>
          <div className="field">
            <label htmlFor="test_phone">Test mobile number</label>
            <input id="test_phone" name="test_phone" type="tel" defaultValue={settings.test_phone ?? ""} className="input" />
          </div>
          <div className="field">
            <label htmlFor="from_name">From name</label>
            <input id="from_name" name="from_name" defaultValue={settings.from_name ?? company.name} className="input" />
          </div>
          <div className="field">
            <label htmlFor="reply_to">Replies go to</label>
            <input id="reply_to" name="reply_to" type="email" defaultValue={settings.reply_to ?? ""} className="input" />
          </div>
          <div className="field">
            <label htmlFor="portal_url">Customer portal web address</label>
            <input id="portal_url" name="portal_url" type="url" defaultValue={settings.portal_url ?? ""} className="input" placeholder="https://" />
          </div>
          <label className={styles.check}><input type="checkbox" name="visit_reminders" defaultChecked={settings.visit_reminders} /> Remind customers the day before a visit</label>
          <label className={styles.check}><input type="checkbox" name="invoice_reminders" defaultChecked={settings.invoice_reminders} /> Remind about overdue invoices</label>
          <div className="field">
            <label htmlFor="invoice_reminder_days">Payment reminder every (days)</label>
            <input id="invoice_reminder_days" name="invoice_reminder_days" type="number" min={1} max={60} defaultValue={settings.invoice_reminder_days} className="input" />
          </div>
          <button className="button" type="submit">Save settings</button>
        </form>
      </details>

      <details className={styles.panel}>
        <summary>Wording</summary>
        <div className={styles.templates}>
          {templates.map((t) => {
            const o = custom.find((c) => c.template_key === t.template_key && c.channel === t.channel && c.active);
            const anyOverride = custom.find((c) => c.template_key === t.template_key && c.channel === t.channel);
            return (
              <div key={`${t.template_key}-${t.channel}`} className={styles.template}>
                <h3>{t.label} <span className={styles.muted}>· {t.channel === "sms" ? "text" : "email"}{o ? " · customized" : ""}</span></h3>
                <form action={saveTemplate} className={styles.templates}>
                  <input type="hidden" name="template_key" value={t.template_key} />
                  <input type="hidden" name="channel" value={t.channel} />
                  {anyOverride && <input type="hidden" name="override_id" value={anyOverride.id} />}
                  {t.channel === "email" && (
                    <div className="field">
                      <label htmlFor={`s-${t.template_key}-${t.channel}`}>Subject</label>
                      <input id={`s-${t.template_key}-${t.channel}`} name="subject" required defaultValue={o?.subject ?? t.subject ?? ""} className="input" />
                    </div>
                  )}
                  <div className="field">
                    <label htmlFor={`b-${t.template_key}-${t.channel}`}>Message</label>
                    <textarea id={`b-${t.template_key}-${t.channel}`} name="body" required maxLength={t.channel === "sms" ? 480 : 5000}
                      defaultValue={o?.body ?? t.body} className="input" />
                  </div>
                  <p className={styles.vars}>Fills in: {t.variables.map((v) => <code key={v}>{`{{${v}}}`}</code>).reduce<React.ReactNode[]>((a, c, i) => (i ? [...a, " ", c] : [c]), [])}</p>
                  <div>
                    <button className="button quiet" type="submit">Save wording</button>
                  </div>
                </form>
                {o && (
                  <form action={resetTemplate}><input type="hidden" name="override_id" value={o.id} /><button className={styles.textButton} type="submit">Use the built-in wording</button></form>
                )}
              </div>
            );
          })}
        </div>
      </details>
    </div>
  );
}
