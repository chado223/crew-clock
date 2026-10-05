import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "./profit.module.css";

export const metadata: Metadata = { title: "Profit" };
export const dynamic = "force-dynamic";

type Row = {
  visit_id: string;
  scheduled_date: string;
  job_title: string;
  client_id: string | null;
  client_name: string | null;
  revenue: number;
  workers: number;
  onsite_minutes: number;
  overhead_minutes: number;
  labor_cost: number;
  margin: number;
  margin_pct: number | null;
  missing_rates: number;
};

const money = (n: number) =>
  new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(n);
const pct = (n: number | null) => (n == null ? "–" : `${n.toFixed(0)}%`);
const hours = (m: number) => `${Math.floor(m / 60)}:${String(m % 60).padStart(2, "0")}`;

function addDays(ymd: string, n: number) {
  const d = new Date(`${ymd}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

export default async function ProfitPage({ searchParams }: { searchParams: Promise<{ from?: string; to?: string }> }) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date());
  const valid = (s?: string) => (s && /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : undefined);
  const to = valid(sp.to) ?? today;
  const from = valid(sp.from) ?? addDays(to, -27);

  const { data, error } = await (await supabaseServer()).rpc("visit_costing", {
    p_tenant_id: company.tenant_id,
    p_from: from,
    p_to: to,
  });
  const rows = (data ?? []) as Row[];

  const revenue = rows.reduce((n, r) => n + Number(r.revenue), 0);
  const labor = rows.reduce((n, r) => n + Number(r.labor_cost), 0);
  const margin = revenue - labor;
  const missing = rows.reduce((n, r) => n + r.missing_rates, 0);

  const byClient = new Map<string, { id: string | null; name: string; visits: number; revenue: number; labor: number }>();
  for (const r of rows) {
    const key = r.client_id ?? "none";
    const c = byClient.get(key) ?? { id: r.client_id, name: r.client_name ?? "No customer", visits: 0, revenue: 0, labor: 0 };
    c.visits += 1;
    c.revenue += Number(r.revenue);
    c.labor += Number(r.labor_cost);
    byClient.set(key, c);
  }
  const clients = [...byClient.values()].sort((a, b) => (a.revenue - a.labor) - (b.revenue - b.labor));

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <h1>Profit</h1>
        <form className={styles.range}>
          <div className="field">
            <label htmlFor="from">From</label>
            <input id="from" name="from" type="date" defaultValue={from} className="input" />
          </div>
          <div className="field">
            <label htmlFor="to">To</label>
            <input id="to" name="to" type="date" defaultValue={to} className="input" />
          </div>
          <button className="button quiet" type="submit">Show</button>
        </form>
      </header>

      {error && <p className="error-text">{friendlyError(error)}</p>}

      <p className={styles.headline}>
        {rows.length === 0 ? (
          "No completed visits in this period."
        ) : (
          <>
            <span className="figure">{money(margin)}</span> kept from <span className="figure">{money(revenue)}</span> of
            completed work after <span className="figure">{money(labor)}</span> in labor
            {revenue > 0 ? <>, a <span className="figure">{pct((margin / revenue) * 100)}</span> margin</> : null}.
          </>
        )}
      </p>
      <p className={styles.method}>
        Labor includes time on site plus each person's drive and prep time that day, at their pay rate. Materials, fuel and
        equipment are not included yet.
      </p>
      {missing > 0 && (
        <p className="notice">
          {missing} {missing === 1 ? "person has" : "people have"} no pay rate, so their time counts as $0.{" "}
          <Link href="/team">Add pay rates</Link>.
        </p>
      )}

      {clients.length > 0 && (
        <section aria-labelledby="by-customer">
          <h2 id="by-customer" className={styles.h2}>By customer, least profitable first</h2>
          <table className={styles.table}>
            <thead>
              <tr>
                <th scope="col">Customer</th>
                <th scope="col" className={styles.num}>Visits</th>
                <th scope="col" className={styles.num}>Billed</th>
                <th scope="col" className={styles.num}>Labor</th>
                <th scope="col" className={styles.num}>Margin</th>
              </tr>
            </thead>
            <tbody>
              {clients.map((c) => {
                const m = c.revenue - c.labor;
                const p = c.revenue > 0 ? (m / c.revenue) * 100 : null;
                return (
                  <tr key={c.id ?? "none"} className={p != null && p < 20 ? styles.low : undefined}>
                    <td>{c.id ? <Link href={`/clients/${c.id}`}>{c.name}</Link> : c.name}</td>
                    <td className={`${styles.num} figure`}>{c.visits}</td>
                    <td className={`${styles.num} figure`}>{money(c.revenue)}</td>
                    <td className={`${styles.num} figure`}>{money(c.labor)}</td>
                    <td className={`${styles.num} figure`}>{money(m)} <span className={styles.p}>{pct(p)}</span></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </section>
      )}

      {rows.length > 0 && (
        <section aria-labelledby="visits">
          <h2 id="visits" className={styles.h2}>Every visit</h2>
          <table className={styles.table}>
            <thead>
              <tr>
                <th scope="col">Date</th>
                <th scope="col">Job</th>
                <th scope="col" className={styles.num}>On site</th>
                <th scope="col" className={styles.num}>Drive and prep</th>
                <th scope="col" className={styles.num}>Billed</th>
                <th scope="col" className={styles.num}>Labor</th>
                <th scope="col" className={styles.num}>Margin</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.visit_id} className={r.margin_pct != null && r.margin_pct < 20 ? styles.low : undefined}>
                  <td>{new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", day: "numeric" }).format(new Date(`${r.scheduled_date}T12:00:00Z`))}</td>
                  <td>
                    {r.job_title}
                    <span className={styles.sub}>{r.client_name}</span>
                  </td>
                  <td className={`${styles.num} figure`}>{hours(r.onsite_minutes)}</td>
                  <td className={`${styles.num} figure`}>{hours(r.overhead_minutes)}</td>
                  <td className={`${styles.num} figure`}>{money(Number(r.revenue))}</td>
                  <td className={`${styles.num} figure`}>{money(Number(r.labor_cost))}</td>
                  <td className={`${styles.num} figure`}>{pct(r.margin_pct)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
    </div>
  );
}
