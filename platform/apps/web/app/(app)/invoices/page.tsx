import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";
import { formatMoney, friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../money.module.css";

export const metadata: Metadata = { title: "Invoices" };
export const dynamic = "force-dynamic";

const FILTERS = [
  ["open", "Unpaid"],
  ["draft", "Drafts"],
  ["paid", "Paid"],
  ["all", "All"],
] as const;

async function billVisits(formData: FormData) {
  "use server";
  const tax = Number(formData.get("tax_percent") ?? 0) / 100;
  const { data, error } = await (await supabaseServer()).rpc("invoice_completed_visits", {
    p_client_id: String(formData.get("client_id")),
    p_from: String(formData.get("from")),
    p_to: String(formData.get("to")),
    p_tax_rate: Number.isFinite(tax) ? tax : 0,
  });
  if (error || !data) redirect(`/invoices?error=${encodeURIComponent(friendlyError(error))}`);
  redirect(`/invoices/${(data as { id: string }).id}`);
}

export default async function InvoicesPage({
  searchParams,
}: {
  searchParams: Promise<{ show?: string; error?: string; client?: string }>;
}) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const show = FILTERS.some(([f]) => f === sp.show) ? sp.show! : "open";
  const supabase = await supabaseServer();

  let q = supabase
    .from("invoices")
    .select("id, number, status, total, amount_paid, issued_at, due_at, clients(name)")
    .eq("tenant_id", company.tenant_id)
    .order("issued_at", { ascending: false })
    .limit(200);
  if (show === "open") q = q.in("status", ["sent", "partial", "overdue"]);
  else if (show !== "all") q = q.eq("status", show);
  const [{ data: invoices, error }, { data: clients }] = await Promise.all([
    q,
    supabase.from("clients").select("id, name").eq("tenant_id", company.tenant_id).eq("status", "active").order("name"),
  ]);

  const today = new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date());
  const monthStart = `${today.slice(0, 8)}01`;
  const outstanding = (invoices ?? [])
    .filter((i) => ["sent", "partial", "overdue"].includes(i.status))
    .reduce((n, i) => n + Number(i.total) - Number(i.amount_paid), 0);

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <div>
          <h1>Invoices</h1>
          {show === "open" && (invoices ?? []).length > 0 && (
            <p className={styles.sub}>
              <span className="figure">{formatMoney(outstanding)}</span> waiting to be paid.
            </p>
          )}
        </div>
        <nav className={styles.tabs} aria-label="Filter">
          {FILTERS.map(([f, label]) => (
            <Link key={f} href={`/invoices?show=${f}`} aria-current={f === show ? "page" : undefined} className={styles.tab}>{label}</Link>
          ))}
        </nav>
      </header>

      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}
      {error && <p className="error-text">{friendlyError(error)}</p>}

      <section className={styles.panel} aria-labelledby="bill">
        <h2 id="bill">Bill completed work</h2>
        <p className={styles.empty}>Creates a draft invoice from a customer's completed visits that haven't been billed yet. You review it before marking it sent.</p>
        <form action={billVisits} className={styles.form}>
          <div className="field">
            <label htmlFor="client_id">Customer</label>
            <select id="client_id" name="client_id" required className="select" defaultValue={sp.client ?? ""}>
              <option value="" disabled>Choose a customer</option>
              {(clients ?? []).map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
            </select>
          </div>
          <div className="field"><label htmlFor="from">Visits from</label><input id="from" name="from" type="date" required defaultValue={monthStart} className="input" /></div>
          <div className="field"><label htmlFor="to">Through</label><input id="to" name="to" type="date" required defaultValue={today} className="input" /></div>
          <div className="field"><label htmlFor="tax">Sales tax (%)</label><input id="tax" name="tax_percent" type="number" min={0} max={99} step="0.001" defaultValue="0" className="input" /></div>
          <button className="button" type="submit">Create draft invoice</button>
        </form>
      </section>

      {(invoices ?? []).length === 0 ? (
        <p className={styles.empty}>{show === "open" ? "Nothing waiting to be paid." : "No invoices here."}</p>
      ) : (
        <ul className={styles.list}>
          {(invoices ?? []).map((i) => {
            const balance = Number(i.total) - Number(i.amount_paid);
            return (
              <li key={i.id}>
                <Link href={`/invoices/${i.id}`} className={styles.row}>
                  <span className="figure">{i.number ?? "Earlier invoice"}</span>
                  <span>{(i.clients as unknown as { name: string } | null)?.name}</span>
                  <span className={`${styles.status} ${styles[`status_${i.status}`] ?? ""}`}>{i.status}</span>
                  <span className={styles.num}>{formatMoney(i.status === "paid" ? i.total : balance)}</span>
                </Link>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
