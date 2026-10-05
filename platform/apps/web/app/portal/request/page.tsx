import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { currentPortalAccount, portalDate } from "@/lib/portal";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../portal.module.css";

export const metadata: Metadata = { title: "Request service" };
export const dynamic = "force-dynamic";

const STATUS: Record<string, string> = { new: "Received", acknowledged: "Being reviewed", scheduled: "Scheduled", closed: "Closed" };

async function submit(formData: FormData) {
  "use server";
  const { account } = await currentPortalAccount();
  const property = String(formData.get("property_id") ?? "");
  const date = String(formData.get("preferred_date") ?? "");
  const { error } = await (await supabaseServer()).rpc("portal_submit_request", {
    p_client_id: account.client_id,
    p_details: String(formData.get("details") ?? ""),
    p_property_id: property || (null as unknown as string),
    p_preferred_date: date || (null as unknown as string),
  });
  redirect(`/portal/request${error ? `?error=${encodeURIComponent(friendlyError(error))}` : "?sent=1"}`);
}

export default async function RequestPage({ searchParams }: { searchParams: Promise<{ error?: string; sent?: string }> }) {
  const { account } = await currentPortalAccount();
  const sp = await searchParams;
  const supabase = await supabaseServer();
  const [{ data: props }, { data: requests }] = await Promise.all([
    supabase.rpc("portal_properties", { p_client_id: account.client_id }),
    supabase.rpc("portal_requests", { p_client_id: account.client_id }),
  ]);
  const properties = (props ?? []) as { property_id: string; address: string }[];

  return (
    <>
      <section className={styles.section}>
        <h1>Request service</h1>
        <p className={styles.muted}>Tell {account.company_name} what you need. They'll follow up to confirm timing and price.</p>
        {sp.sent && <p className="notice" role="status">Request sent. {account.company_name} will be in touch.</p>}
        {sp.error && <p className="error-text" role="alert">{sp.error}</p>}
        <form action={submit} className={styles.form}>
          <div className="field">
            <label htmlFor="details">What do you need?</label>
            <textarea id="details" name="details" required minLength={3} maxLength={2000} rows={4} className={`input ${styles.textarea}`}
              placeholder="e.g. Leaf cleanup in the back yard, and trim the hedges by the driveway" />
          </div>
          {properties.length > 1 && (
            <div className="field">
              <label htmlFor="property_id">Where</label>
              <select id="property_id" name="property_id" className="select">
                {properties.map((p) => <option key={p.property_id} value={p.property_id}>{p.address}</option>)}
              </select>
            </div>
          )}
          {properties.length === 1 && <input type="hidden" name="property_id" value={properties[0]!.property_id} />}
          <div className="field">
            <label htmlFor="preferred_date">Preferred date (optional)</label>
            <input id="preferred_date" name="preferred_date" type="date" className="input" />
          </div>
          <button className="button" type="submit">Send request</button>
        </form>
      </section>

      {((requests ?? []) as unknown[]).length > 0 && (
        <section className={styles.section} aria-labelledby="past">
          <h2 id="past">Your requests</h2>
          <ul className={styles.list}>
            {((requests ?? []) as { request_id: string; details: string; status: string; created_at: string }[]).map((r) => (
              <li key={r.request_id}>
                <span>{r.details} <span className={styles.muted}>{portalDate(r.created_at, account.company_timezone, { month: "short", day: "numeric" })}</span></span>
                <span className={`${styles.pill} ${styles[`pill_${r.status}`] ?? ""}`}>{STATUS[r.status] ?? r.status}</span>
              </li>
            ))}
          </ul>
        </section>
      )}
    </>
  );
}
