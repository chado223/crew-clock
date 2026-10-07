import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import { formatMoney, friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../money.module.css";

export const metadata: Metadata = { title: "Expenses" };
export const dynamic = "force-dynamic";

const CATEGORIES = ["Fuel", "Materials", "Equipment", "Repairs", "Dump fees", "Insurance", "Office", "Other"];

async function add(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const amount = Number(formData.get("amount"));
  const link = String(formData.get("link") ?? "");
  const [kind, ref] = link.split(":");
  if (!Number.isFinite(amount) || amount <= 0) redirect(`/expenses?error=${encodeURIComponent("Enter an amount greater than zero.")}`);
  const { error } = await (await supabaseServer()).from("expenses").insert({
    tenant_id: company.tenant_id,
    category: String(formData.get("category") || "Other"),
    amount: Math.round(amount * 100) / 100,
    spent_at: String(formData.get("spent_at")),
    note: String(formData.get("note") ?? "").trim() || null,
    visit_id: kind === "v" ? ref : null,
    job_id: kind === "j" ? ref : null,
    client_id: kind === "c" ? ref : null,
  });
  revalidatePath("/expenses");
  redirect(`/expenses${error ? `?error=${encodeURIComponent(friendlyError(error))}` : "?saved=1"}`);
}

export default async function ExpensesPage({ searchParams }: { searchParams: Promise<{ error?: string; saved?: string; month?: string }> }) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date());
  const month = sp.month && /^\d{4}-\d{2}$/.test(sp.month) ? sp.month : today.slice(0, 7);
  const start = `${month}-01`;
  const endDate = new Date(`${start}T12:00:00Z`); endDate.setUTCMonth(endDate.getUTCMonth() + 1); endDate.setUTCDate(0);
  const end = endDate.toISOString().slice(0, 10);
  const supabase = await supabaseServer();
  const [{ data: rows }, { data: visits }, { data: jobRows }, { data: clientRows }] = await Promise.all([
    supabase.from("expenses").select("id, category, amount, spent_at, note, visit_id, job_id, client_id, clients(name), jobs(title)").eq("tenant_id", company.tenant_id)
      .gte("spent_at", start).lte("spent_at", end).order("spent_at", { ascending: false }).limit(500),
    supabase.rpc("schedule", { p_tenant_id: company.tenant_id, p_from: start, p_to: end }),
    supabase.from("jobs").select("id, title, clients(name)").eq("tenant_id", company.tenant_id).in("status", ["scheduled", "active"]).order("title").limit(500),
    supabase.from("clients").select("id, name").eq("tenant_id", company.tenant_id).in("status", ["active", "lead"]).order("name").limit(1000),
  ]);
  const list = (rows ?? []) as unknown as { id: string; category: string; amount: number; spent_at: string; note: string | null; visit_id: string | null;
    job_id: string | null; client_id: string | null; clients: { name: string } | null; jobs: { title: string } | null }[];
  const jobs = (jobRows ?? []) as unknown as { id: string; title: string; clients: { name: string } | null }[];
  const customers = (clientRows ?? []) as { id: string; name: string }[];
  const done = ((visits ?? []) as { visit_id: string; scheduled_date: string; client_name: string | null; job_title: string; status: string }[])
    .filter((v) => v.status === "completed");
  const byCat = new Map<string, number>();
  for (const e of list) byCat.set(e.category, (byCat.get(e.category) ?? 0) + Number(e.amount));
  const total = list.reduce((n, e) => n + Number(e.amount), 0);
  const prev = new Date(`${start}T12:00:00Z`); prev.setUTCMonth(prev.getUTCMonth() - 1);
  const next = new Date(`${start}T12:00:00Z`); next.setUTCMonth(next.getUTCMonth() + 1);
  const label = new Date(`${start}T12:00:00Z`).toLocaleDateString("en-US", { month: "long", year: "numeric", timeZone: "UTC" });

  return (
    <div className={styles.page}>
      <header style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", flexWrap: "wrap", gap: 12 }}>
        <div><h1>Expenses</h1><p className={styles.empty}>{label}: {formatMoney(total)}</p></div>
        <nav style={{ display: "flex", gap: 8 }}>
          <a className="button quiet" href={`/expenses?month=${prev.toISOString().slice(0, 7)}`}>Previous month</a>
          <a className="button quiet" href={`/expenses?month=${next.toISOString().slice(0, 7)}`}>Next month</a>
        </nav>
      </header>
      {sp.saved && <p className="notice" role="status">Expense added. It counts in Business health and, if tied to a visit, in that job&apos;s profit.</p>}
      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}

      <section className={styles.panel} aria-labelledby="add">
        <h2 id="add">Add an expense</h2>
        <form action={add} className={styles.form}>
          <div className="field">
            <label htmlFor="cat">Category</label>
            <select id="cat" name="category" className="select">{CATEGORIES.map((c) => <option key={c}>{c}</option>)}</select>
          </div>
          <div className="field"><label htmlFor="amt">Amount ($)</label><input id="amt" name="amount" type="number" min={0.01} step="0.01" required className="input" /></div>
          <div className="field"><label htmlFor="on">Date</label><input id="on" name="spent_at" type="date" required defaultValue={today} className="input" /></div>
          <div className="field">
            <label htmlFor="link">What it was for</label>
            <select id="link" name="link" className="select" defaultValue="">
              <option value="">General business (overhead)</option>
              {done.length > 0 && <optgroup label="A finished visit">{done.map((v) => <option key={v.visit_id} value={`v:${v.visit_id}`}>{v.scheduled_date}: {v.client_name ?? v.job_title}</option>)}</optgroup>}
              {jobs.length > 0 && <optgroup label="A job">{jobs.map((j) => <option key={j.id} value={`j:${j.id}`}>{j.clients?.name ? `${j.clients.name}: ` : ""}{j.title}</option>)}</optgroup>}
              {customers.length > 0 && <optgroup label="A customer">{customers.map((c) => <option key={c.id} value={`c:${c.id}`}>{c.name}</option>)}</optgroup>}
            </select>
            <p className="hint">Tied costs count in that customer&apos;s, job&apos;s and service&apos;s profit. Overhead counts in company totals.</p>
          </div>
          <div className="field"><label htmlFor="note">Note</label><input id="note" name="note" className="input" placeholder="e.g. 3 bags mulch" /></div>
          <button className="button" type="submit">Add expense</button>
        </form>
      </section>

      {byCat.size > 0 && (
        <p className={styles.empty}>{[...byCat.entries()].sort((a, b) => b[1] - a[1]).map(([k, n]) => `${k} ${formatMoney(n)}`).join(" · ")}</p>
      )}
      {list.length === 0 ? <p className={styles.empty}>No expenses this month.</p> : (
        <ul className={styles.list}>
          {list.map((e) => (
            <li key={e.id} className={styles.row}>
              <span>{e.spent_at}</span>
              <span>{e.category}{e.clients?.name ? ` · ${e.clients.name}` : ""}{e.jobs?.title ? ` · ${e.jobs.title}` : ""}{!e.client_id && !e.job_id ? " · overhead" : ""}</span>
              <span>{e.note}</span>
              <span className={styles.num}>{formatMoney(e.amount)}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
