import type { Metadata } from "next";
import Link from "next/link";
import { notFound, redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import { formatMoney, friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import { describeMessage, MESSAGE_COLUMNS, type MessageRow } from "@/lib/messages";
import styles from "../../money.module.css";

export const metadata: Metadata = { title: "Estimate" };
export const dynamic = "force-dynamic";

function back(id: string, error?: unknown): never {
  revalidatePath(`/estimates/${id}`);
  redirect(`/estimates/${id}${error ? `?error=${encodeURIComponent(friendlyError(error))}` : ""}`);
}

async function addLine(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const id = String(formData.get("id"));
  const every = String(formData.get("repeat") ?? "");
  const minutes = Number(formData.get("est_minutes") ?? "");
  const { error } = await (await supabaseServer()).from("estimate_lines").insert({
    tenant_id: company.tenant_id,
    estimate_id: id,
    description: String(formData.get("description") ?? "").trim(),
    quantity: Number(formData.get("quantity") || 1),
    unit_price: Number(formData.get("unit_price")),
    repeat_every_weeks: every ? Number(every) : null,
    est_minutes: Number.isFinite(minutes) && minutes > 0 ? Math.round(minutes) : null,
  });
  back(id, error);
}

async function removeLine(formData: FormData) {
  "use server";
  const id = String(formData.get("id"));
  const { error } = await (await supabaseServer()).from("estimate_lines").delete().eq("id", String(formData.get("line_id")));
  back(id, error);
}

async function setStatus(formData: FormData) {
  "use server";
  const id = String(formData.get("id"));
  const { error } = await (await supabaseServer()).rpc("set_estimate_status", { p_estimate_id: id, p_status: String(formData.get("status")) });
  back(id, error);
}

async function send(formData: FormData) {
  "use server";
  const id = String(formData.get("id"));
  const { error } = await (await supabaseServer()).rpc("send_estimate", { p_estimate_id: id, p_channel: String(formData.get("channel") ?? "email") });
  if (error) back(id, error);
  redirect(`/estimates/${id}?sent=1`);
}

async function convert(formData: FormData) {
  "use server";
  const id = String(formData.get("id"));
  const crew = String(formData.get("crew_id") ?? "");
  const { error } = await (await supabaseServer()).rpc("convert_estimate", {
    p_estimate_id: id,
    p_start_on: String(formData.get("start_on")),
    p_crew_id: crew || (null as unknown as string),
  });
  if (error) back(id, error);
  revalidatePath("/schedule");
  redirect(`/estimates/${id}`);
}

const STATUS_NOTE: Record<string, string> = {
  draft: "Add the work and prices, then mark it sent once the customer has it.",
  sent: "Waiting on the customer. Record their answer when you hear back.",
  approved: "Approved. Turn it into jobs to put the work on the schedule.",
  converted: "This estimate is now on the schedule as jobs.",
  declined: "The customer said no.",
  expired: "This estimate expired.",
};

export default async function EstimatePage({ params, searchParams }: { params: Promise<{ id: string }>; searchParams: Promise<{ error?: string; sent?: string }> }) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const { id } = await params;
  const { error, sent } = await searchParams;
  if (!/^[0-9a-f-]{36}$/i.test(id)) notFound();

  const supabase = await supabaseServer();
  const [{ data: est }, { data: lines }, { data: crews }, { data: jobs }] = await Promise.all([
    supabase.from("estimates").select("*, clients(id, name), properties(address_line1, city)").eq("id", id).maybeSingle(),
    supabase.from("estimate_lines").select("*").eq("estimate_id", id).order("sort_order").order("created_at"),
    supabase.from("crews").select("id, name").eq("tenant_id", company.tenant_id).eq("active", true).order("name"),
    supabase.from("jobs").select("id, title, kind").eq("estimate_id", id),
  ]);
  if (!est) notFound();
  const client = est.clients as unknown as { id: string; name: string } | null;
  const prop = est.properties as unknown as { address_line1: string; city: string | null } | null;
  const draft = est.status === "draft";
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date());
  const recurringTotal = (lines ?? []).filter((l) => l.repeat_every_weeks).reduce((n, l) => n + Number(l.amount), 0);

  const { data: lastMsg } = await (await supabaseServer()).from("messages").select(MESSAGE_COLUMNS)
    .eq("subject_type", "estimate").eq("subject_id", id).order("created_at", { ascending: false }).limit(1).maybeSingle();
  const lastMessage = lastMsg as unknown as MessageRow | null;

  return (
    <div className={styles.page}>
      <Link href="/estimates" className="noPrint">All estimates</Link>
      {error && <p className="error-text" role="alert">{error}</p>}
      {lastMessage && (
        <p className={`notice noPrint`} role="status">
          {sent ? "Sent to the outbox. " : "Last message: "}{describeMessage(lastMessage)}{" "}
          <Link href="/messages">Messages</Link>
        </p>
      )}

      <article className={styles.doc}>
        <div className={styles.docHead}>
          <div>
            <p className={styles.docNumber}>{est.number}</p>
            {est.valid_until && <p className={styles.sub}>Good through {new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "long", day: "numeric", year: "numeric" }).format(new Date(`${est.valid_until}T12:00:00Z`))}</p>}
          </div>
          <div>
            <p><strong>{company.name}</strong></p>
            <p className={styles.sub}>For: {client?.name}</p>
            {prop && <p className={styles.sub}>{prop.address_line1}{prop.city ? `, ${prop.city}` : ""}</p>}
          </div>
        </div>

        {(lines ?? []).length === 0 ? (
          <p className={styles.empty}>No work added yet.</p>
        ) : (
          <table className={styles.lines}>
            <thead>
              <tr><th scope="col">Work</th><th scope="col">How often</th><th scope="col" className={styles.right}>Price</th>{draft && <th className="noPrint" />}</tr>
            </thead>
            <tbody>
              {(lines ?? []).map((l) => (
                <tr key={l.id}>
                  <td>{l.description}{Number(l.quantity) !== 1 ? ` × ${l.quantity}` : ""}</td>
                  <td>{l.repeat_every_weeks ? (l.repeat_every_weeks === 1 ? "Each week" : `Every ${l.repeat_every_weeks} weeks`) : "Once"}</td>
                  <td className={`${styles.right} figure`}>{formatMoney(l.amount)}{l.repeat_every_weeks ? " per visit" : ""}</td>
                  {draft && (
                    <td className={`${styles.right} noPrint`}>
                      <form action={removeLine}>
                        <input type="hidden" name="id" value={est.id} />
                        <input type="hidden" name="line_id" value={l.id} />
                        <button type="submit" className={styles.remove}>Remove</button>
                      </form>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {recurringTotal > 0 && <p className={styles.sub}>Recurring work is billed per visit. One-time work is billed once.</p>}
      </article>

      <p className={`${styles.empty} noPrint`}>{STATUS_NOTE[est.status]}</p>

      {draft && (
        <section className={`${styles.panel} noPrint`} aria-labelledby="add">
          <h2 id="add">Add work</h2>
          <form action={addLine} className={styles.form}>
            <input type="hidden" name="id" value={est.id} />
            <div className="field"><label htmlFor="description">Work</label><input id="description" name="description" required maxLength={500} className="input" placeholder="e.g. Core aeration and overseed" /></div>
            <div className="field"><label htmlFor="unit_price">Price ($)</label><input id="unit_price" name="unit_price" type="number" min={0} step="0.01" required className="input" /></div>
            <div className="field"><label htmlFor="quantity">Quantity</label><input id="quantity" name="quantity" type="number" min={0.01} step="0.01" defaultValue="1" className="input" /></div>
            <div className="field">
              <label htmlFor="repeat">How often</label>
              <select id="repeat" name="repeat" className="select" defaultValue="">
                <option value="">Once</option>
                <option value="1">Each week</option>
                <option value="2">Every 2 weeks</option>
                <option value="4">Every 4 weeks</option>
              </select>
            </div>
            <div className="field"><label htmlFor="est_minutes">Time on site (min)</label><input id="est_minutes" name="est_minutes" type="number" min={1} className="input" /></div>
            <button className="button" type="submit">Add</button>
          </form>
        </section>
      )}

      <div className={`${styles.actions} noPrint`}>
        <span className={`${styles.status} ${styles[`status_${est.status}`] ?? ""}`}>{est.status}</span>
        {draft && (
          <>
            <form action={send}><input type="hidden" name="id" value={est.id} /><button className="button" type="submit" name="channel" value="email">Email to customer</button></form>
            <form action={setStatus}><input type="hidden" name="id" value={est.id} /><input type="hidden" name="status" value="sent" /><button className="button quiet" type="submit">Mark sent (I sent it myself)</button></form>
          </>
        )}
        {est.status === "sent" && (
          <>
            <form action={send}><input type="hidden" name="id" value={est.id} /><button className="button quiet" type="submit" name="channel" value="email">Email again</button></form>
            <form action={setStatus}><input type="hidden" name="id" value={est.id} /><input type="hidden" name="status" value="approved" /><button className="button" type="submit">Customer approved</button></form>
            <form action={setStatus}><input type="hidden" name="id" value={est.id} /><input type="hidden" name="status" value="declined" /><button className="button quiet" type="submit">Customer declined</button></form>
          </>
        )}
        {client && <Link href={`/clients/${client.id}`} className="button quiet">Customer record</Link>}
      </div>

      {est.status === "approved" && (
        <section className={`${styles.panel} noPrint`} aria-labelledby="convert">
          <h2 id="convert">Put it on the schedule</h2>
          <form action={convert} className={styles.form}>
            <input type="hidden" name="id" value={est.id} />
            <div className="field"><label htmlFor="start_on">Start on</label><input id="start_on" name="start_on" type="date" required defaultValue={today} className="input" /><p className="hint">Recurring work stays on this weekday.</p></div>
            <div className="field">
              <label htmlFor="crew_id">Crew</label>
              <select id="crew_id" name="crew_id" className="select" defaultValue="">
                <option value="">Assign later</option>
                {(crews ?? []).map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
              </select>
            </div>
            <button className="button" type="submit">Create jobs</button>
          </form>
        </section>
      )}

      {(jobs ?? []).length > 0 && (
        <p className="noPrint">
          Jobs from this estimate: {(jobs ?? []).map((j) => j.title).join(", ")}. <Link href="/schedule">See the schedule</Link>.
        </p>
      )}
    </div>
  );
}
