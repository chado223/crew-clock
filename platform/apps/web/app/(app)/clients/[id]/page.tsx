import type { Metadata } from "next";
import Link from "next/link";
import { notFound, redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import { friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import { siteOrigin } from "@/lib/origin";
import { readFlash, setFlash } from "@/lib/flash";
import { describeMessage, MESSAGE_COLUMNS, TEMPLATE_LABEL, type MessageRow } from "@/lib/messages";
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
    region: String(formData.get("region") ?? "").trim().toUpperCase() || null,
    postal_code: String(formData.get("postal_code") ?? "").trim() || null,
    access_notes: String(formData.get("access_notes") ?? "").trim() || null,
    lawn_sqft: Number.isFinite(sqft) && sqft > 0 ? Math.round(sqft) : null,
  });
  revalidatePath(`/clients/${id}`);
  redirect(`/clients/${id}${error ? `?error=${encodeURIComponent(friendlyError(error))}` : ""}`);
}

const WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
const JOB_STATUS: Record<string, string> = { paused: "Paused", completed: "Ended", canceled: "Canceled" };

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
    const { error: genError } = await supabase.rpc("generate_visits", { p_tenant_id: company.tenant_id, p_from: date, p_to: end.toISOString().slice(0, 10) });
    if (genError) {
      console.error("[clients] generate_visits failed:", genError.message);
      redirect(`/clients/${id}?error=${encodeURIComponent("The job was saved, but its visits weren't scheduled. Use Fill on the Schedule page.")}`);
    }
  }
  revalidatePath(`/clients/${id}`);
  redirect(`/clients/${id}`);
}

async function invitePortal(formData: FormData) {
  "use server";
  const id = String(formData.get("client_id"));
  const supabase = await supabaseServer();
  const { data, error } = await supabase.rpc("invite_customer", {
    p_client_id: id,
    p_email: String(formData.get("email") ?? ""),
  });
  if (!error) {
    const { company } = await currentCompany();
    const { error: mailError } = await supabase.rpc("send_invite_message", {
      p_tenant_id: company.tenant_id,
      p_kind: "portal_invite",
      p_to: String(formData.get("email") ?? ""),
      p_link: `${await siteOrigin()}/portal/join/${String(data)}`,
      p_client_id: id,
    });
    if (mailError) console.error("[clients] portal invite email not queued:", mailError.message);
  }
  revalidatePath(`/clients/${id}`);
  if (!error) await setFlash("portal_invite", String(data), `/clients/${id}`);
  redirect(`/clients/${id}?${error ? `error=${encodeURIComponent(friendlyError(error))}` : "portal_invite=1"}`);
}

async function savePreferences(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const id = String(formData.get("client_id"));
  const sms = formData.get("sms_ok") === "on";
  const off = ["visit_reminder", "invoice_reminder"].filter((k) => formData.get(`kind_${k}`) !== "on");
  const { error } = await (await supabaseServer()).from("contact_preferences").upsert(
    {
      tenant_id: company.tenant_id,
      client_id: id,
      email_ok: formData.get("email_ok") === "on",
      sms_ok: sms,
      sms_consent_source: sms ? String(formData.get("sms_consent_source") ?? "").trim() || "office" : null,
      kinds_off: off,
      unsubscribed_at: formData.get("unsubscribed") === "on" ? new Date().toISOString() : null,
    },
    { onConflict: "client_id" },
  );
  revalidatePath(`/clients/${id}`);
  redirect(`/clients/${id}${error ? `?error=${encodeURIComponent(friendlyError(error))}` : ""}`);
}

async function photoAction(formData: FormData) {
  "use server";
  const id = String(formData.get("client_id"));
  const photo = String(formData.get("photo_id"));
  const supabase = await supabaseServer();
  const action = String(formData.get("action"));
  const { error } = action === "hide"
    ? await supabase.rpc("hide_visit_photo", { p_photo_id: photo, p_hidden: true })
    : await supabase.rpc("set_photo_visibility", { p_photo_id: photo, p_customer_visible: action === "share" });
  revalidatePath(`/clients/${id}`);
  redirect(`/clients/${id}${error ? `?error=${encodeURIComponent(friendlyError(error))}` : ""}#photos`);
}

async function revokePortal(formData: FormData) {
  "use server";
  const id = String(formData.get("client_id"));
  const { error } = await (await supabaseServer()).rpc("revoke_portal_access", { p_access_id: String(formData.get("access_id")) });
  revalidatePath(`/clients/${id}`);
  redirect(`/clients/${id}${error ? `?error=${encodeURIComponent(friendlyError(error))}` : ""}`);
}

async function updateRequest(formData: FormData) {
  "use server";
  const id = String(formData.get("client_id"));
  const { error } = await (await supabaseServer())
    .from("service_requests")
    .update({ status: String(formData.get("status")) })
    .eq("id", String(formData.get("request_id")));
  revalidatePath(`/clients/${id}`);
  redirect(`/clients/${id}${error ? `?error=${encodeURIComponent(friendlyError(error))}` : ""}`);
}

const back = (id: string, error?: unknown, anchor = ""): never => {
  revalidatePath(`/clients/${id}`);
  redirect(`/clients/${id}${error ? `?error=${encodeURIComponent(friendlyError(error))}` : ""}${anchor}`);
};
const text = (f: FormData, k: string) => String(f.get(k) ?? "").trim() || null;

async function saveClient(formData: FormData) {
  "use server";
  const id = String(formData.get("client_id"));
  const name = text(formData, "name");
  if (!name) redirect(`/clients/${id}?error=${encodeURIComponent("The customer needs a name.")}`);
  const tags = (text(formData, "tags") ?? "").split(",").map((t) => t.trim()).filter(Boolean).slice(0, 20);
  const { error } = await (await supabaseServer()).from("clients").update({
    name,
    kind: formData.get("kind") === "commercial" ? "commercial" : "residential",
    company_name: text(formData, "company_name"),
    email: text(formData, "email")?.toLowerCase() ?? null,
    phone: text(formData, "phone"),
    preferred_contact: text(formData, "preferred_contact"),
    lead_source: text(formData, "lead_source"),
    address: text(formData, "address"),
    tags,
  }).eq("id", id);
  back(id, error);
}

async function saveProperty(formData: FormData) {
  "use server";
  const id = String(formData.get("client_id"));
  const pid = String(formData.get("property_id"));
  const sqft = Number(formData.get("lawn_sqft") ?? "");
  const address = text(formData, "address_line1");
  if (!address) redirect(`/clients/${id}?error=${encodeURIComponent("Enter the street address.")}`);
  const { error } = await (await supabaseServer()).from("properties").update({
    label: text(formData, "label"),
    address_line1: address,
    address_line2: text(formData, "address_line2"),
    city: text(formData, "city"),
    region: text(formData, "region")?.toUpperCase() ?? null,
    postal_code: text(formData, "postal_code"),
    access_notes: text(formData, "access_notes"),
    gate_code: text(formData, "gate_code"),
    notes: text(formData, "notes"),
    lawn_sqft: Number.isFinite(sqft) && sqft > 0 ? Math.round(sqft) : null,
  }).eq("id", pid);
  back(id, error, "#properties");
}

async function setPropertyStatus(formData: FormData) {
  "use server";
  const id = String(formData.get("client_id"));
  const { error } = await (await supabaseServer()).from("properties")
    .update({ status: formData.get("status") === "inactive" ? "inactive" : "active" })
    .eq("id", String(formData.get("property_id")));
  back(id, error, "#properties");
}

async function saveJob(formData: FormData) {
  "use server";
  const id = String(formData.get("client_id"));
  const num = (k: string) => {
    const v = text(formData, k);
    const n = v == null ? NaN : Number(v);
    return Number.isFinite(n) ? n : null;
  };
  const crew = String(formData.get("crew_id") ?? "");
  const endsOn = text(formData, "ends_on");
  const status = text(formData, "status");
  const { error } = await (await supabaseServer()).rpc("update_job", {
    p_job_id: String(formData.get("job_id")),
    p_title: text(formData, "title"),
    p_price: num("price"),
    p_est_minutes: num("est_minutes"),
    p_crew_id: crew && crew !== "none" ? crew : null,
    p_clear_crew: crew === "none",
    p_interval_weeks: num("interval_weeks"),
    p_weekday: num("weekday"),
    p_ends_on: endsOn,
    p_clear_ends_on: formData.get("had_end") === "1" && !endsOn,
    p_status: status,
    p_apply_to_scheduled: formData.get("apply") !== "off",
    p_reason: text(formData, "reason"),
  } as never);
  back(id, error, "#jobs");
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
  searchParams: Promise<{ error?: string; portal_invite?: string }>;
}) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const { id } = await params;
  const { error, portal_invite: portalInviteFlag } = await searchParams;
  const portal_invite = portalInviteFlag ? await readFlash("portal_invite") : null;
  if (!/^[0-9a-f-]{36}$/i.test(id)) notFound();

  const supabase = await supabaseServer();
  const [{ data: client }, { data: properties }, { data: timeline }] = await Promise.all([
    supabase.from("clients").select("*").eq("id", id).maybeSingle(),
    supabase.from("properties").select("*").eq("client_id", id).order("created_at"),
    supabase.from("activity").select("id, kind, summary, occurred_at").eq("client_id", id).order("occurred_at", { ascending: false }).order("seq", { ascending: false }).limit(100),
  ]);
  const [{ data: jobs }, { data: crews }, { data: upcoming }] = await Promise.all([
    supabase.from("jobs").select("id, title, kind, status, price, est_minutes, crew_id, interval_weeks, weekday, ends_on, property_id, crews(name)").eq("client_id", id).order("created_at"),
    supabase.from("crews").select("id, name").eq("tenant_id", company.tenant_id).eq("active", true).order("name"),
    supabase.from("visits").select("id, scheduled_date, status, job_id").eq("client_id", id)
      .gte("scheduled_date", new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date()))
      .eq("status", "scheduled").order("scheduled_date").limit(8),
  ]);
  const [{ data: portalUsers }, { data: requests }] = await Promise.all([
    supabase.from("portal_access").select("id, status, created_at, user_id").eq("client_id", id).eq("status", "active"),
    supabase.from("service_requests").select("id, details, preferred_date, status, created_at").eq("client_id", id).neq("status", "closed").order("created_at", { ascending: false }),
  ]);
  const [{ data: msgs }, { data: prefRow }] = await Promise.all([
    supabase.from("messages").select(MESSAGE_COLUMNS).eq("client_id", id).order("created_at", { ascending: false }).limit(10),
    supabase.from("contact_preferences").select("email_ok, sms_ok, sms_consent_at, sms_consent_source, kinds_off, unsubscribed_at").eq("client_id", id).maybeSingle(),
  ]);
  const messages = (msgs ?? []) as unknown as MessageRow[];
  const { data: photoRows } = await supabase.from("visit_photos")
    .select("id, kind, caption, storage_path, customer_visible, taken_at, visits(scheduled_date)")
    .eq("client_id", id).is("hidden_at", null).order("taken_at", { ascending: false }).limit(24);
  const photos = (photoRows ?? []) as unknown as { id: string; kind: string; caption: string | null; storage_path: string;
    customer_visible: boolean; taken_at: string; visits: { scheduled_date: string } | null }[];
  const { data: signedPhotos } = photos.length
    ? await supabase.storage.from("visit-photos").createSignedUrls(photos.map((p) => p.storage_path), 300)
    : { data: [] as { path: string | null; signedUrl: string }[] };
  const photoUrl = (path: string): string | undefined => signedPhotos?.find((x) => x.path === path)?.signedUrl ?? undefined;
  const pref = (prefRow as { email_ok: boolean; sms_ok: boolean; sms_consent_at: string | null; sms_consent_source: string | null; kinds_off: string[]; unsubscribed_at: string | null } | null)
    ?? { email_ok: true, sms_ok: false, sms_consent_at: null, sms_consent_source: null, kinds_off: [], unsubscribed_at: null };
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
          <section aria-labelledby="jobs" id="jobs-section" className={styles.section}>
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
                      {j.ends_on ? `. Ends ${shortDate(j.ends_on)}` : ""}
                    </p>
                    {j.status !== "active" && j.status !== "scheduled" && <p className={styles.badge}>{JOB_STATUS[j.status] ?? j.status}</p>}
                    <details className={styles.editBox}>
                      <summary>Change</summary>
                      <form action={saveJob} className={styles.propForm}>
                        <input type="hidden" name="client_id" value={client.id} />
                        <input type="hidden" name="job_id" value={j.id} />
                        <input type="hidden" name="had_end" value={j.ends_on ? "1" : "0"} />
                        <div className="field"><label htmlFor={`t-${j.id}`}>Job</label><input id={`t-${j.id}`} name="title" defaultValue={j.title} required className="input" /></div>
                        <div className={styles.twoUp}>
                          <div className="field"><label htmlFor={`p-${j.id}`}>Price per visit ($)</label><input id={`p-${j.id}`} name="price" type="number" min={0} step="0.01" defaultValue={j.price ?? ""} className="input" /></div>
                          <div className="field"><label htmlFor={`m-${j.id}`}>Minutes on site</label><input id={`m-${j.id}`} name="est_minutes" type="number" min={1} defaultValue={j.est_minutes ?? ""} className="input" /></div>
                        </div>
                        <div className="field">
                          <label htmlFor={`c-${j.id}`}>Crew</label>
                          <select id={`c-${j.id}`} name="crew_id" defaultValue={j.crew_id ?? "none"} className="select">
                            <option value="none">No crew</option>
                            {(crews ?? []).map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
                          </select>
                        </div>
                        {j.kind === "recurring" && (
                          <div className={styles.twoUp}>
                            <div className="field">
                              <label htmlFor={`w-${j.id}`}>Day</label>
                              <select id={`w-${j.id}`} name="weekday" defaultValue={String(j.weekday ?? 1)} className="select">
                                {WEEKDAYS.map((d, i) => <option key={d} value={i + 1}>{d}</option>)}
                              </select>
                            </div>
                            <div className="field">
                              <label htmlFor={`i-${j.id}`}>Every</label>
                              <select id={`i-${j.id}`} name="interval_weeks" defaultValue={String(j.interval_weeks ?? 1)} className="select">
                                {[1, 2, 3, 4].map((n) => <option key={n} value={n}>{n === 1 ? "Week" : `${n} weeks`}</option>)}
                              </select>
                            </div>
                          </div>
                        )}
                        {j.kind === "recurring" && (
                          <div className="field"><label htmlFor={`e-${j.id}`}>Last date (optional)</label><input id={`e-${j.id}`} name="ends_on" type="date" defaultValue={j.ends_on ?? ""} className="input" /><p className="hint">For seasonal work. Visits after it are canceled.</p></div>
                        )}
                        <label className={styles.checkRow}><input type="checkbox" name="apply" value="off" /> Keep upcoming visits at their old price, time and crew</label>
                        <div className="field"><label htmlFor={`r-${j.id}`}>Reason (optional)</label><input id={`r-${j.id}`} name="reason" className="input" placeholder="e.g. Price increase for 2027" /></div>
                        <div className={styles.inlineForm}>
                          <button className="button" type="submit">Save changes</button>
                          {j.kind === "recurring" && (j.status === "paused"
                            ? <button className="button quiet" type="submit" name="status" value="active">Resume</button>
                            : <button className="button quiet" type="submit" name="status" value="paused">Pause</button>)}
                          {j.status !== "completed" && j.status !== "canceled" && <button className={styles.textButton} type="submit" name="status" value="completed">End this job</button>}
                        </div>
                      </form>
                    </details>
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
                      {(properties ?? []).filter((p) => p.status !== "inactive").map((p) => <option key={p.id} value={p.id}>{p.address_line1}</option>)}
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
              {client.address && (<><dt>Mailing</dt><dd>{client.address}</dd></>)}
              {(client.tags ?? []).length > 0 && (<><dt>Tags</dt><dd>{(client.tags as string[]).join(", ")}</dd></>)}
            </dl>
            <details className={styles.addProp}>
              <summary>Edit details</summary>
              <form action={saveClient} className={styles.propForm}>
                <input type="hidden" name="client_id" value={client.id} />
                <div className="field"><label htmlFor="c-name">Name</label><input id="c-name" name="name" required defaultValue={client.name} className="input" /></div>
                <div className="field">
                  <label htmlFor="c-kind">Type</label>
                  <select id="c-kind" name="kind" defaultValue={client.kind ?? "residential"} className="select">
                    <option value="residential">Residential</option>
                    <option value="commercial">Commercial</option>
                  </select>
                </div>
                <div className="field"><label htmlFor="c-co">Company (commercial)</label><input id="c-co" name="company_name" defaultValue={client.company_name ?? ""} className="input" /></div>
                <div className="field"><label htmlFor="c-ph">Phone</label><input id="c-ph" name="phone" type="tel" defaultValue={client.phone ?? ""} className="input" /></div>
                <div className="field"><label htmlFor="c-em">Email</label><input id="c-em" name="email" type="email" defaultValue={client.email ?? ""} className="input" /></div>
                <div className="field">
                  <label htmlFor="c-pc">Best way to reach</label>
                  <select id="c-pc" name="preferred_contact" defaultValue={client.preferred_contact ?? ""} className="select">
                    <option value="">No preference</option>
                    <option value="call">Phone call</option>
                    <option value="text">Text</option>
                    <option value="email">Email</option>
                  </select>
                </div>
                <div className="field"><label htmlFor="c-addr">Mailing address</label><input id="c-addr" name="address" defaultValue={client.address ?? ""} className="input" /></div>
                <div className="field"><label htmlFor="c-src">Found you through</label><input id="c-src" name="lead_source" defaultValue={client.lead_source ?? ""} className="input" /></div>
                <div className="field"><label htmlFor="c-tags">Tags</label><input id="c-tags" name="tags" defaultValue={(client.tags ?? []).join(", ")} className="input" placeholder="e.g. corner lot, HOA" /><p className="hint">Separate with commas.</p></div>
                <button className="button" type="submit">Save</button>
              </form>
            </details>
          </section>

          {(requests ?? []).length > 0 && (
            <section aria-labelledby="requests" className={styles.section}>
              <h2 id="requests">Service requests</h2>
              <ul className={styles.props}>
                {(requests ?? []).map((r) => (
                  <li key={r.id}>
                    <p>{r.details}</p>
                    <p className={styles.small}>
                      {new Intl.DateTimeFormat("en-US", { timeZone: company.timezone, month: "short", day: "numeric" }).format(new Date(r.created_at))}
                      {r.preferred_date ? `, wants ${shortDate(r.preferred_date)}` : ""}
                    </p>
                    <form action={updateRequest} className={styles.inlineForm}>
                      <input type="hidden" name="client_id" value={client.id} />
                      <input type="hidden" name="request_id" value={r.id} />
                      <label htmlFor={`rs-${r.id}`} className={styles.srOnly}>Status</label>
                      <select id={`rs-${r.id}`} name="status" defaultValue={r.status} className="select">
                        <option value="new">New</option>
                        <option value="acknowledged">Reviewing</option>
                        <option value="scheduled">Scheduled</option>
                        <option value="closed">Closed</option>
                      </select>
                      <button type="submit" className="button quiet">Update</button>
                    </form>
                  </li>
                ))}
              </ul>
            </section>
          )}

          {photos.length > 0 && (
            <section aria-labelledby="photos" id="photos" className={styles.section}>
              <h2 id="photos-h">Photos</h2>
              <p className={styles.small}>Taken by crews. Customers only see the ones you share.</p>
              <ul className={styles.photoGrid}>
                {photos.map((p) => (
                  <li key={p.id}>
                    {photoUrl(p.storage_path) ? (
                      <a href={photoUrl(p.storage_path)} target="_blank" rel="noreferrer">
                        {/* eslint-disable-next-line @next/next/no-img-element */}
                        <img src={photoUrl(p.storage_path)} alt={p.caption ?? `${p.kind} photo`} loading="lazy" />
                      </a>
                    ) : <span className={styles.photoMissing} />}
                    <span className={styles.small}>
                      {p.kind === "issue" ? "Problem" : p.kind === "before" ? "Before" : p.kind === "after" ? "After" : "Photo"}
                      {p.visits?.scheduled_date ? `, ${shortDate(p.visits.scheduled_date)}` : ""}
                      {p.customer_visible ? " · shared" : ""}
                    </span>
                    <form action={photoAction} className={styles.photoActions}>
                      <input type="hidden" name="client_id" value={client.id} />
                      <input type="hidden" name="photo_id" value={p.id} />
                      <button type="submit" name="action" value={p.customer_visible ? "unshare" : "share"} className={styles.linkButton}>
                        {p.customer_visible ? "Stop sharing" : "Share with customer"}
                      </button>
                      <button type="submit" name="action" value="hide" className={styles.textButton}>Hide</button>
                    </form>
                  </li>
                ))}
              </ul>
            </section>
          )}

          <section aria-labelledby="msgs" className={styles.section}>
            <h2 id="msgs">Messages</h2>
            {messages.length === 0 ? (
              <p className={styles.small}>Nothing sent yet.</p>
            ) : (
              <ul className={styles.props}>
                {messages.map((m) => (
                  <li key={m.id}>
                    <span className={styles.addr}>{TEMPLATE_LABEL[m.template_key] ?? m.template_key}</span>{" "}
                    <span className={styles.small}>{m.channel === "sms" ? "text" : "email"}, {day(m.created_at)}</span>
                    <p className={styles.small}>{describeMessage(m)}</p>
                  </li>
                ))}
              </ul>
            )}
            <details className={styles.addProp}>
              <summary>Contact preferences</summary>
              <form action={savePreferences} className={styles.propForm}>
                <input type="hidden" name="client_id" value={client.id} />
                <label className={styles.checkRow}><input type="checkbox" name="email_ok" defaultChecked={pref.email_ok} /> Email is OK</label>
                <label className={styles.checkRow}><input type="checkbox" name="sms_ok" defaultChecked={pref.sms_ok} /> Customer agreed to texts</label>
                <div className="field">
                  <label htmlFor="sms_src">How they agreed</label>
                  <input id="sms_src" name="sms_consent_source" className="input" defaultValue={pref.sms_consent_source ?? ""} placeholder="e.g. said yes on the phone, signed form" />
                </div>
                <label className={styles.checkRow}><input type="checkbox" name="kind_visit_reminder" defaultChecked={!pref.kinds_off.includes("visit_reminder")} /> Visit reminders</label>
                <label className={styles.checkRow}><input type="checkbox" name="kind_invoice_reminder" defaultChecked={!pref.kinds_off.includes("invoice_reminder")} /> Payment reminders</label>
                <label className={styles.checkRow}><input type="checkbox" name="unsubscribed" defaultChecked={!!pref.unsubscribed_at} /> Stop all messages</label>
                <button type="submit" className="button quiet">Save preferences</button>
              </form>
            </details>
          </section>

          <section aria-labelledby="portal" className={styles.section}>
            <h2 id="portal">Customer portal</h2>
            <p className={styles.small}>
              {(portalUsers ?? []).length > 0
                ? `${(portalUsers ?? []).length} ${(portalUsers ?? []).length === 1 ? "person can" : "people can"} sign in to see visits, estimates and invoices.`
                : "Let this customer see their visits, approve estimates and view invoices online."}
            </p>
            {portal_invite && (
              <div className="notice">
                <p>Invite queued in Messages (test mode sends it to your test address). You can also send this link yourself. It works once, for that email, for 14 days.</p>
                <input readOnly className="input" style={{ width: "100%", marginTop: 8 }} aria-label="Portal invite link"
                  value={`${await siteOrigin()}/portal/join/${portal_invite}`} />
              </div>
            )}
            <form action={invitePortal} className={styles.inlineForm}>
              <input type="hidden" name="client_id" value={client.id} />
              <label htmlFor="portal-email" className={styles.srOnly}>Customer email</label>
              <input id="portal-email" name="email" type="email" required className="input" defaultValue={client.email ?? ""} placeholder="Customer email" />
              <button type="submit" className="button quiet">Create invite</button>
            </form>
            {(portalUsers ?? []).map((u) => (
              <form key={u.id} action={revokePortal} className={styles.inlineForm}>
                <input type="hidden" name="client_id" value={client.id} />
                <input type="hidden" name="access_id" value={u.id} />
                <span className={styles.small}>Portal login since {new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric" }).format(new Date(u.created_at))}</span>
                <button type="submit" className={styles.textButton}>Remove access</button>
              </form>
            ))}
          </section>

          <section aria-labelledby="properties" className={styles.section}>
            <h2 id="properties">Properties</h2>
            {(properties ?? []).length === 0 ? (
              <p className={styles.empty}>No service address yet.</p>
            ) : (
              <ul className={styles.props}>
                {(properties ?? []).map((p) => (
                  <li key={p.id} className={p.status === "inactive" ? styles.inactive : undefined}>
                    <p className={styles.addr}>{p.label ? `${p.label}: ` : ""}{p.address_line1}{p.city ? `, ${p.city}` : ""}{p.region ? ` ${p.region}` : ""}</p>
                    {p.status === "inactive" && <p className={styles.badge}>Archived</p>}
                    {p.lawn_sqft && <p className={styles.small}>{p.lawn_sqft.toLocaleString()} sq ft of lawn</p>}
                    {p.access_notes && <p className={styles.small}>{p.access_notes}</p>}
                    {p.latitude == null && <p className={styles.small}>Not on the map yet (Routes can look it up).</p>}
                    <details className={styles.editBox}>
                      <summary>Edit</summary>
                      <form action={saveProperty} className={styles.propForm}>
                        <input type="hidden" name="client_id" value={client.id} />
                        <input type="hidden" name="property_id" value={p.id} />
                        <div className="field"><label htmlFor={`pl-${p.id}`}>Label (optional)</label><input id={`pl-${p.id}`} name="label" defaultValue={p.label ?? ""} className="input" placeholder="e.g. Rental house" /></div>
                        <div className="field"><label htmlFor={`pa-${p.id}`}>Street address</label><input id={`pa-${p.id}`} name="address_line1" required defaultValue={p.address_line1} className="input" /></div>
                        <div className="field"><label htmlFor={`pa2-${p.id}`}>Unit / line 2</label><input id={`pa2-${p.id}`} name="address_line2" defaultValue={p.address_line2 ?? ""} className="input" /></div>
                        <div className={styles.threeUp}>
                          <div className="field"><label htmlFor={`pc-${p.id}`}>City</label><input id={`pc-${p.id}`} name="city" defaultValue={p.city ?? ""} className="input" /></div>
                          <div className="field"><label htmlFor={`pr-${p.id}`}>State</label><input id={`pr-${p.id}`} name="region" maxLength={2} defaultValue={p.region ?? ""} className="input" /></div>
                          <div className="field"><label htmlFor={`pz-${p.id}`}>ZIP</label><input id={`pz-${p.id}`} name="postal_code" defaultValue={p.postal_code ?? ""} className="input" inputMode="numeric" /></div>
                        </div>
                        <div className="field"><label htmlFor={`ps-${p.id}`}>Lawn size (sq ft)</label><input id={`ps-${p.id}`} name="lawn_sqft" type="number" min={0} defaultValue={p.lawn_sqft ?? ""} className="input" /></div>
                        <div className="field"><label htmlFor={`pn-${p.id}`}>Gate and access notes (crew see these)</label><input id={`pn-${p.id}`} name="access_notes" defaultValue={p.access_notes ?? ""} className="input" /></div>
                        <div className="field"><label htmlFor={`pg-${p.id}`}>Gate code (office only)</label><input id={`pg-${p.id}`} name="gate_code" defaultValue={p.gate_code ?? ""} className="input" /></div>
                        <div className="field"><label htmlFor={`po-${p.id}`}>Office notes</label><input id={`po-${p.id}`} name="notes" defaultValue={p.notes ?? ""} className="input" /></div>
                        <button className="button" type="submit">Save</button>
                      </form>
                      <form action={setPropertyStatus} className={styles.inlineForm}>
                        <input type="hidden" name="client_id" value={client.id} />
                        <input type="hidden" name="property_id" value={p.id} />
                        {p.status === "inactive"
                          ? <button className="button quiet" name="status" value="active" type="submit">Restore</button>
                          : <button className={styles.textButton} name="status" value="inactive" type="submit">Archive this property</button>}
                      </form>
                    </details>
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
                <div className="field"><label htmlFor="st">State</label><input id="st" name="region" maxLength={2} className="input" defaultValue="TN" /></div>
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
