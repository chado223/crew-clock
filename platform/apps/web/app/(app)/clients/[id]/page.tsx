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

const WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

async function addJob(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const supabase = await supabaseServer();
  const id = String(formData.get("client_id"));
  const kind = String(formData.get("kind")) === "recurring" ? "recurring" : "one_off";
  const date = String(formData.get("date") ?? "");
  const price = Number(formData.get("price") ?? "");
  const minutes = Number(formData.get("est_minutes") ?? "");
  const crew = String(formData.get("crew_id") ?? "");
  const title = String(formData.get("title") ?? "").trim();
  const fail = (msg: string): never => redirect(`/clients/${id}?error=${encodeURIComponent(msg)}`);
  if (!title) fail("Name the job, e.g. Weekly mow & edge.");
  if (!date) fail("Pick a date.");

  const startDow = ((new Date(`${date}T12:00:00Z`).getUTCDay() + 6) % 7) + 1; // ISO weekday of the chosen date
  const { data: job, error } = await supabase
    .from("jobs")
    .insert({
      tenant_id: company.tenant_id,
      client_id: id,
      property_id: String(formData.get("property_id")),
      title,
      kind,
      status: "active",
      price: Number.isFinite(price) && price > 0 ? price : null,
      est_minutes: Number.isFinite(minutes) && minutes > 0 ? Math.round(minutes) : null,
      crew_id: crew || null,
      ...(kind === "recurring"
        ? { starts_on: date, weekday: startDow, interval_weeks: Number(formData.get("interval_weeks") ?? 1) }
        : {}),
    })
    .select("id")
    .single();
  if (error || !job) fail(friendlyError(error));

  if (kind === "one_off") {
    const { error: vErr } = await supabase.from("visits").insert({ tenant_id: company.tenant_id, job_id: job!.id, scheduled_date: date });
    if (vErr) fail(friendlyError(vErr));
  } else {
    const end = new Date(`${date}T12:00:00Z`);
    end.setUTCDate(end.getUTCDate() + 42);
    await supabase.rpc("generate_visits", { p_tenant_id: company.tenant_id, p_from: date, p_to: end.toISOString().slice(0, 10) });
  }
  revalidatePath(`/clients/${id}`);
  redirect(`/clients/${id}`);
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
  const [{ data: jobs }, { data: crews }, { data: upcoming }] = await Promise.all([
    supabase.from("jobs").select("id, title, kind, status, price, interval_weeks, weekday, property_id, crews(name)").eq("client_id", id).order("created_at"),
    supabase.from("crews").select("id, name").eq("tenant_id", company.tenant_id).eq("active", true).order("name"),
    supabase.from("visits").select("id, scheduled_date, status, job_id").eq("client_id", id)
      .gte("scheduled_date", new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date()))
      .eq("status", "scheduled").order("scheduled_date").limit(8),
  ]);
  const nextVisit = new Map<string, string>();
  for (const v of upcoming ?? []) if (!nextVisit.has(v.job_id)) nextVisit.set(v.job_id, v.scheduled_date);
  const shortDate = (ymd: string) =>
    new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short", month: "short", day: "numeric" }).format(new Date(`${ymd}T12:00:00Z`));
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
          <Link href="/estimates" className="button quiet">New estimate</Link>
          <Link href={`/invoices?client=${client.id}`} className="button quiet">Bill completed work</Link>
        </form>
      </header>

      {error && <p className="error-text" role="alert">{error}</p>}

      <div className={styles.columns}>
        <div className={styles.main}>
          <section aria-labelledby="jobs" className={styles.section}>
            <h2 id="jobs">Jobs</h2>
            {(jobs ?? []).length === 0 ? (
              <p className={styles.empty}>No jobs yet. Add the work you do for this customer below.</p>
            ) : (
              <ul className={styles.props}>
                {(jobs ?? []).map((j) => (
                  <li key={j.id}>
                    <p className={styles.addr}>{j.title}</p>
                    <p className={styles.small}>
                      {j.kind === "recurring"
                        ? `${j.interval_weeks === 1 ? "Every" : `Every ${j.interval_weeks} weeks on`} ${WEEKDAYS[(j.weekday ?? 1) - 1]}`
                        : "One-time"}
                      {j.price ? `, $${Number(j.price).toFixed(2)}` : ""}
                      {(j.crews as unknown as { name: string } | null)?.name ? `, ${(j.crews as unknown as { name: string }).name}` : ""}
                      {nextVisit.get(j.id) ? `. Next: ${shortDate(nextVisit.get(j.id)!)}` : ""}
                    </p>
                  </li>
                ))}
              </ul>
            )}
            {(properties ?? []).length > 0 ? (
              <details className={styles.addProp}>
                <summary>Add a job</summary>
                <form action={addJob} className={styles.propForm}>
                  <input type="hidden" name="client_id" value={client.id} />
                  <div className="field"><label htmlFor="jt">What you'll do</label><input id="jt" name="title" required className="input" placeholder="e.g. Mow, edge and blow" /></div>
                  <div className="field">
                    <label htmlFor="jp">Property</label>
                    <select id="jp" name="property_id" className="select">
                      {(properties ?? []).map((p) => <option key={p.id} value={p.id}>{p.address_line1}</option>)}
                    </select>
                  </div>
                  <div className="field">
                    <label htmlFor="jk">How often</label>
                    <select id="jk" name="kind" className="select" defaultValue="recurring">
                      <option value="recurring">Repeats</option>
                      <option value="one_off">One time</option>
                    </select>
                  </div>
                  <div className="field">
                    <label htmlFor="ji">Repeats every</label>
                    <select id="ji" name="interval_weeks" className="select" defaultValue="1">
                      <option value="1">Week</option>
                      <option value="2">2 weeks</option>
                      <option value="3">3 weeks</option>
                      <option value="4">4 weeks</option>
                    </select>
                    <p className="hint">Ignored for one-time jobs.</p>
                  </div>
                  <div className="field"><label htmlFor="jd">First date</label><input id="jd" name="date" type="date" required className="input" /><p className="hint">Repeating jobs stay on this weekday.</p></div>
                  <div className="field"><label htmlFor="jpr">Price per visit ($)</label><input id="jpr" name="price" type="number" min={0} step="0.01" className="input" /></div>
                  <div className="field"><label htmlFor="jm">Time on site (minutes)</label><input id="jm" name="est_minutes" type="number" min={1} className="input" /></div>
                  <div className="field">
                    <label htmlFor="jc">Crew</label>
                    <select id="jc" name="crew_id" className="select" defaultValue="">
                      <option value="">Assign later</option>
                      {(crews ?? []).map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
                    </select>
                  </div>
                  <button className="button" type="submit">Add job</button>
                </form>
              </details>
            ) : (
              <p className={styles.small}>Add a property first, then you can add jobs for it.</p>
            )}
          </section>

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
