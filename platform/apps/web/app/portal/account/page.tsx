import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { currentPortalAccount } from "@/lib/portal";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../portal.module.css";

export const metadata: Metadata = { title: "Contact details" };
export const dynamic = "force-dynamic";

async function save(formData: FormData) {
  "use server";
  const { account } = await currentPortalAccount();
  const { error } = await (await supabaseServer()).rpc("portal_update_contact", {
    p_client_id: account.client_id,
    p_phone: String(formData.get("phone") ?? ""),
    p_preferred_contact: String(formData.get("preferred_contact") ?? "") || (null as unknown as string),
    p_mailing_address: String(formData.get("mailing_address") ?? ""),
  });
  redirect(`/portal/account${error ? `?error=${encodeURIComponent(friendlyError(error))}` : "?saved=1"}`);
}

export default async function AccountPage({ searchParams }: { searchParams: Promise<{ error?: string; saved?: string }> }) {
  const { account } = await currentPortalAccount();
  const sp = await searchParams;
  const supabase = await supabaseServer();
  const [{ data: profile }, { data: props }] = await Promise.all([
    supabase.rpc("portal_profile", { p_client_id: account.client_id }),
    supabase.rpc("portal_properties", { p_client_id: account.client_id }),
  ]);
  const p = ((profile ?? []) as { name: string; email: string | null; phone: string | null; preferred_contact: string | null; mailing_address: string | null }[])[0];

  return (
    <section className={styles.section}>
      <h1>Contact details</h1>
      <p className={styles.muted}>Account name: <strong>{p?.name}</strong>{p?.email ? `, ${p.email}` : ""}. To change these, contact {account.company_name}.</p>
      {sp.saved && <p className="notice" role="status">Saved.</p>}
      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}
      <form action={save} className={styles.form}>
        <div className="field">
          <label htmlFor="phone">Phone</label>
          <input id="phone" name="phone" type="tel" defaultValue={p?.phone ?? ""} className="input" />
        </div>
        <div className="field">
          <label htmlFor="preferred_contact">Best way to reach you</label>
          <select id="preferred_contact" name="preferred_contact" defaultValue={p?.preferred_contact ?? ""} className="select">
            <option value="">No preference</option>
            <option value="call">Phone call</option>
            <option value="text">Text message</option>
            <option value="email">Email</option>
          </select>
        </div>
        <div className="field">
          <label htmlFor="mailing_address">Mailing address</label>
          <input id="mailing_address" name="mailing_address" defaultValue={p?.mailing_address ?? ""} className="input" />
        </div>
        <button className="button" type="submit">Save</button>
      </form>
      {((props ?? []) as unknown[]).length > 0 && (
        <>
          <h2>Service addresses</h2>
          <ul className={styles.list}>
            {((props ?? []) as { property_id: string; address: string }[]).map((x) => <li key={x.property_id}>{x.address}</li>)}
          </ul>
        </>
      )}
    </section>
  );
}
