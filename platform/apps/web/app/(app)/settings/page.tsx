import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../import/import.module.css";

export const metadata: Metadata = { title: "Company settings" };
export const dynamic = "force-dynamic";

const ZONES = ["America/New_York", "America/Chicago", "America/Denver", "America/Phoenix", "America/Los_Angeles", "America/Anchorage", "Pacific/Honolulu"];
const DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

async function save(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const t = (k: string) => String(formData.get(k) ?? "").trim() || null;
  const tax = Number(formData.get("tax_percent") ?? 0);
  const { error, count } = await (await supabaseServer()).from("tenants").update({
    name: t("name") ?? company.name,
    phone: t("phone"), email: t("email"), website: t("website"),
    address_line1: t("address_line1"), city: t("city"), region: t("region")?.toUpperCase() ?? null, postal_code: t("postal_code"),
    default_tax_rate: Number.isFinite(tax) ? tax / 100 : 0,
    payment_terms_days: Number(formData.get("payment_terms_days") ?? 30),
    invoice_note: t("invoice_note"), estimate_note: t("estimate_note"),
    timezone: t("timezone") ?? company.timezone,
    week_start_day: Number(formData.get("week_start_day") ?? 1),
    overtime_weekly_hours: Number(formData.get("overtime_weekly_hours") ?? 40),
  }, { count: "exact" }).eq("id", company.tenant_id);
  if (error) redirect(`/settings?error=${encodeURIComponent(error.message.includes("check") ? "One of the values is out of range." : friendlyError(error))}`);
  if (!count) redirect(`/settings?error=${encodeURIComponent("Only the owner can change company settings.")}`);
  redirect("/settings?saved=1");
}

export default async function SettingsPage({ searchParams }: { searchParams: Promise<{ error?: string; saved?: string }> }) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const { data } = await (await supabaseServer()).from("tenants").select("*").eq("id", company.tenant_id).maybeSingle();
  const c = (data ?? {}) as Record<string, string | number | null>;
  const v = (k: string) => (c[k] == null ? "" : String(c[k]));
  const owner = company.role === "owner";

  return (
    <div className={styles.page}>
      <header>
        <h1>Company settings</h1>
        <p className={styles.lede}>What customers see on invoices and estimates, your billing defaults, and how hours are counted.{!owner && " Only the owner can change these."}</p>
      </header>
      {sp.saved && <p className="notice" role="status">Saved.</p>}
      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}
      <form action={save} className={styles.flow}>
        <fieldset className={styles.panel} disabled={!owner}>
          <legend><h2>Company</h2></legend>
          <div className="field"><label htmlFor="name">Company name</label><input id="name" name="name" required defaultValue={v("name")} className="input" /></div>
          <div className="field"><label htmlFor="a1">Street address</label><input id="a1" name="address_line1" defaultValue={v("address_line1")} className="input" /></div>
          <div className={styles.cols}>
            <div className="field"><label htmlFor="city">City</label><input id="city" name="city" defaultValue={v("city")} className="input" /></div>
            <div className="field"><label htmlFor="region">State</label><input id="region" name="region" maxLength={2} defaultValue={v("region")} className="input" /></div>
            <div className="field"><label htmlFor="zip">ZIP</label><input id="zip" name="postal_code" defaultValue={v("postal_code")} className="input" /></div>
            <div className="field"><label htmlFor="phone">Phone</label><input id="phone" name="phone" type="tel" defaultValue={v("phone")} className="input" /></div>
            <div className="field"><label htmlFor="email">Email</label><input id="email" name="email" type="email" defaultValue={v("email")} className="input" /></div>
            <div className="field"><label htmlFor="web">Website</label><input id="web" name="website" defaultValue={v("website")} className="input" /></div>
          </div>
        </fieldset>
        <fieldset className={styles.panel} disabled={!owner}>
          <legend><h2>Billing</h2></legend>
          <div className={styles.cols}>
            <div className="field"><label htmlFor="tax">Sales tax (%)</label><input id="tax" name="tax_percent" type="number" min={0} max={99} step="0.001" defaultValue={String(Math.round(Number(c.default_tax_rate ?? 0) * 100000) / 1000)} className="input" /></div>
            <div className="field"><label htmlFor="terms">Payment due (days)</label><input id="terms" name="payment_terms_days" type="number" min={0} max={120} defaultValue={v("payment_terms_days") || "30"} className="input" /></div>
          </div>
          <div className="field"><label htmlFor="inote">Note on invoices</label><input id="inote" name="invoice_note" maxLength={1000} defaultValue={v("invoice_note")} className="input" placeholder="e.g. Thank you! Checks payable to CWLC." /></div>
          <div className="field"><label htmlFor="enote">Note on estimates</label><input id="enote" name="estimate_note" maxLength={1000} defaultValue={v("estimate_note")} className="input" placeholder="e.g. Prices good for 30 days." /></div>
        </fieldset>
        <fieldset className={styles.panel} disabled={!owner}>
          <legend><h2>Hours</h2></legend>
          <div className={styles.cols}>
            <div className="field">
              <label htmlFor="tz">Time zone</label>
              <select id="tz" name="timezone" defaultValue={v("timezone")} className="select">
                {ZONES.map((z) => <option key={z} value={z}>{z.replace("America/", "").replace("_", " ")}</option>)}
              </select>
            </div>
            <div className="field">
              <label htmlFor="ws">Work week starts</label>
              <select id="ws" name="week_start_day" defaultValue={v("week_start_day") || "1"} className="select">
                {DAYS.map((d, i) => <option key={d} value={i + 1}>{d}</option>)}
              </select>
            </div>
            <div className="field"><label htmlFor="ot">Overtime after (hours a week)</label><input id="ot" name="overtime_weekly_hours" type="number" min={1} max={168} step="0.5" defaultValue={v("overtime_weekly_hours") || "40"} className="input" /></div>
          </div>
          <p className={styles.muted}>Changing these changes how every timesheet and payroll export is counted, including past weeks.</p>
        </fieldset>
        {owner && <div><button className="button" type="submit">Save settings</button></div>}
      </form>
    </div>
  );
}
