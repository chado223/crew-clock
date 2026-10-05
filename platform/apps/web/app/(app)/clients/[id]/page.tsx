import type { Metadata } from "next";
import Link from "next/link";
import { notFound, redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import { friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "./client.module.css";

export const metadata: Metadata = { title: "Customer" };
export const dynamic = "force-dynamic";

const STATUS_LABEL: Record<string, string> = { lead: "Lead", active: "Customer", inactive: "Inactive", lost: "Lost" };
const NOTE_KINDS = [
  ["note", "Note"],
  ["call", "Phone call"],
  ["sms", "Text message"],
  ["email", "Email"],
  ["meeting", "Visit or meeting"],
  ["complaint", "Complaint"],
] as const;

async function addNote(formData: FormData) {
  "use server";
  const id = String(formData.get("client_id"));
  const { error } = await (await supabaseServer()).rpc("add_client_note", {
    p_client_id: id,
    p_kind: String(formData.get("kind") ?? "note"),
    p_summary: String(formData.get("summary") ?? ""),
  });
  revalidatePath(`/clients/${id}`);
  redirect(`/clients/${id}${error ? `?error=${encodeURIComponent(friendlyError(error))}` : ""}`);
}

async function addProperty(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const id = String(formData.get("client_id"));
  const address = String(formData.get("address_line1") ?? "").trim();
  if (!address) redirect(`/clients/${id}?error=${encodeURIComponent("Enter the street address.")}`);
  const sqft = Number(formData.get("lawn_sqft") ?? "");
  const { error } = await (await supabaseServer()).from("properties").insert({
    tenant_id: company.tenant_id,
    client_id: id,
    address_line1: address,
    city: String(formData.get("city") ?? "").trim() || null,
    postal_code: String(formData.get("postal_code") ?? "").trim() || null,
    access_notes: String(formData.get("access_notes") ?? "").trim() || null,
    lawn_sqft: Number.isFinite(sqft) && sqft > 0 ? Math.round(sqft) : null,
  });
  revalidatePath(`/clients/${id}`);
  redirect(`/clients/${id}${error ? `?error=${encodeURIComponent(friendlyError(error))}` : ""}`);
}

async function setStatus(formData: FormData) {
  "use server";
  const id = String(formData.get("client_id"));
  const { error } = await (await supabaseServer())
    .from("clients")
    .update({ status: String(formData.get("status")) })
    .eq("id", id);
  revalidatePath(`/clients/${id}`);
  redirect(`/clients/${id}${error ? `?error=${encodeURIComponent(friendlyError(error))}` : ""}`);
}

export default async function ClientPage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ error?: string }>;
}) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const { id } = await params;
  const { error } = await searchParams;
  if (!/^[0-9a-f-]{36}$/i.test(id)) notFound();

  const supabase = await supabaseServer();
  const [{ data: client }, { data: properties }, { data: timeline }] = await Promise.all([
    supabase.from("clients").select("*").eq("id", id).maybeSingle(),
    supabase.from("properties").select("*").eq("client_id", id).order("created_at"),
    supabase.from("activity").select("id, kind, summary, occurred_at").eq("client_id", id).order("occurred_at", { ascending: false }).limit(100),
  ]);
  if (!client) notFound();

  const day = (iso: string) =>
    new Intl.DateTimeFormat("en-US", { timeZone: company.timezone, month: "short", day: "numeric", year: "numeric" }).format(new Date(iso));

  return (
    <div className={styles.page}>
      <Link href={`/clients?status=${client.status}`} className={styles.back}>All {client.status === "lead" ? "leads" : "customers"}</Link>

      <header className={styles.header}>
        <div>
          <h1>{client.name}</h1>
          <p className={styles.sub}>
            {STATUS_LABEL[client.status] ?? client.status}, {client.kind === "commercial" ? "commercial" : "residential"}
            {client.lead_source ? `, found you through ${client.lead_source}` : ""}
          </p>
        </div>
        <form action={setStatus} className={styles.statusForm}>
          <input type="hidden" name="client_id" value={client.id} />
          <label htmlFor="status" className={styles.srOnly}>Status</label>
          <select id="status" name="status" defaultValue={client.status} className="select">
            {Object.entries(STATUS_LABEL).map(([v, l]) => (
              <option key={v} value={v}>{l}</option>
            ))}
          </select>
          <button className="button quiet" type="submit">Update status</button>
        </form>
      </header>

      {error && <p className="error-text" role="alert">{error}</p>}

      <div className={styles.columns}>
        <div className={styles.main}>
          <section aria-labelledby="timeline" className={styles.section}>
            <h2 id="timeline">History</h2>
            <form action={addNote} className={styles.noteForm}>
              <input type="hidden" name="client_id" value={client.id} />
              <label htmlFor="kind" className={styles.srOnly}>Type</label>
              <select id="kind" name="kind" className="select" defaultValue="note">
                {NOTE_KINDS.map(([v, l]) => (
                  <option key={v} value={v}>{l}</option>
                ))}
              </select>
              <label htmlFor="summary" className={styles.srOnly}>What happened</label>
              <textarea id="summary" name="summary" required rows={2} maxLength={2000} className={`input ${styles.textarea}`}
                placeholder="What happened? e.g. Called about fall aeration, wants a quote" />
              <button className="button" type="submit">Add to history</button>
            </form>
            {(timeline ?? []).length === 0 ? (
              <p className={styles.empty}>Nothing recorded yet.</p>
            ) : (
              <ol className={styles.timeline}>
                {(timeline ?? []).map((a) => (
                  <li key={a.id} className={styles.event}>
                    <time className={styles.when} dateTime={a.occurred_at}>{day(a.occurred_at)}</time>
                    <p>{a.summary}</p>
                  </li>
                ))}
              </ol>
            )}
          </section>
        </div>

        <aside className={styles.side}>
          <section aria-labelledby="contact" className={styles.section}>
            <h2 id="contact">Contact</h2>
            <dl className={styles.facts}>
              <dt>Phone</dt><dd>{client.phone ? <a href={`tel:${client.phone}`}>{client.phone}</a> : "None"}</dd>
              <dt>Email</dt><dd>{client.email ? <a href={`mailto:${client.email}`}>{client.email}</a> : "None"}</dd>
              {client.company_name && (<><dt>Company</dt><dd>{client.company_name}</dd></>)}
            </dl>
          </section>

          <section aria-labelledby="properties" className={styles.section}>
            <h2 id="properties">Properties</h2>
            {(properties ?? []).length === 0 ? (
              <p className={styles.empty}>No service address yet.</p>
            ) : (
              <ul className={styles.props}>
                {(properties ?? []).map((p) => (
                  <li key={p.id}>
                    <p className={styles.addr}>{p.address_line1}{p.city ? `, ${p.city}` : ""}</p>
                    {p.lawn_sqft && <p className={styles.small}>{p.lawn_sqft.toLocaleString()} sq ft of lawn</p>}
                    {p.access_notes && <p className={styles.small}>{p.access_notes}</p>}
                  </li>
                ))}
              </ul>
            )}
            <details className={styles.addProp}>
              <summary>Add a property</summary>
              <form action={addProperty} className={styles.propForm}>
                <input type="hidden" name="client_id" value={client.id} />
                <div className="field"><label htmlFor="a1">Street address</label><input id="a1" name="address_line1" required className="input" /></div>
                <div className="field"><label htmlFor="city">City</label><input id="city" name="city" className="input" /></div>
                <div className="field"><label htmlFor="zip">ZIP</label><input id="zip" name="postal_code" className="input" inputMode="numeric" /></div>
                <div className="field"><label htmlFor="sqft">Lawn size (sq ft)</label><input id="sqft" name="lawn_sqft" type="number" min={0} className="input" /></div>
                <div className="field"><label htmlFor="access">Gate and access notes</label><input id="access" name="access_notes" className="input" placeholder="e.g. Side gate, dog in back yard" /></div>
                <button className="button" type="submit">Add property</button>
              </form>
            </details>
          </section>
        </aside>
      </div>
    </div>
  );
}
