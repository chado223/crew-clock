import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "./insights.module.css";

export const metadata: Metadata = { title: "Business health" };
export const dynamic = "force-dynamic";

type Health = {
  from: string; to: string; today: string;
  money: {
    work_done: number; completed_visits: number; billed: number; collected: number; job_labor: number; payroll: number;
    paid_hours: number; unallocated_labor: number; expenses: number; gross_profit: number; labor_pct: number | null;
    gross_margin_pct: number | null; revenue_per_paid_hour: number | null; utilization_pct: number | null; missing_rates: number;
  };
  receivables: { total: number; count: number; current: number; d1_30: number; d31_60: number; d61_90: number; d90_plus: number };
  pipeline: { created: number; sent: number; open: number; open_value: number; won: number; won_value: number; lost: number;
    win_rate_pct: number | null; avg_days_to_decision: number | null };
  recurring: { jobs: number; customers: number; monthly_value: number };
  customers: { active: number; served_in_period: number; new_in_period: number; lost_in_period: number; at_risk: number };
  workload: { date: string; visits: number; minutes: number; value: number; unassigned: number }[];
  crews: { crew_id: string; crew: string; visits: number; revenue: number; labor_cost: number; margin: number;
    onsite_hours: number; paid_hours: number; utilization_pct: number | null }[];
};
type Profit = { key: string | null; label: string; sub: string | null; visits: number; revenue: number; labor_cost: number;
  expenses: number; margin: number; margin_pct: number | null; missing_rates: number };
type Receivable = { invoice_id: string; number: string | null; client_id: string | null; client_name: string | null;
  due_on: string | null; balance: number; days_overdue: number; bucket: string };
type AtRisk = { client_id: string; name: string; last_visit: string; visits_180d: number; revenue_180d: number; open_balance: number };

const money = (n: number | null | undefined) =>
  new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(Number(n ?? 0));
const pct = (n: number | null | undefined) => (n == null ? "–" : `${Number(n).toFixed(0)}%`);
const BY: Record<string, string> = { customer: "Customers", service: "Services", property: "Properties", job: "Jobs", crew: "Crews" };

function ymd(d: Date) { return d.toISOString().slice(0, 10); }
function rangeFor(key: string, today: string): { from: string; to: string; label: string } {
  const t = new Date(`${today}T12:00:00Z`);
  const y = t.getUTCFullYear(), m = t.getUTCMonth();
  switch (key) {
    case "last_month": return { from: ymd(new Date(Date.UTC(y, m - 1, 1))), to: ymd(new Date(Date.UTC(y, m, 0))), label: "Last month" };
    case "30d": return { from: ymd(new Date(t.getTime() - 29 * 86_400_000)), to: today, label: "Last 30 days" };
    case "ytd": return { from: `${y}-01-01`, to: today, label: "This year" };
    default: return { from: ymd(new Date(Date.UTC(y, m, 1))), to: today, label: "This month" };
  }
}
const short = (d: string) => new Date(`${d}T12:00:00Z`).toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });

export default async function InsightsPage({ searchParams }: { searchParams: Promise<{ range?: string; by?: string }> }) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date());
  const rangeKey = ["month", "last_month", "30d", "ytd"].includes(sp.range ?? "") ? sp.range! : "month";
  const by = BY[sp.by ?? ""] ? sp.by! : "customer";
  const { from, to, label } = rangeFor(rangeKey, today);
  const q = (extra: Record<string, string>) => `/insights?${new URLSearchParams({ range: rangeKey, by, ...extra }).toString()}`;

  const supabase = await supabaseServer();
  const [{ data: h, error }, { data: prof }, { data: recv }, { data: risk }] = await Promise.all([
    supabase.rpc("business_health", { p_tenant_id: company.tenant_id, p_from: from, p_to: to }),
    supabase.rpc("profitability", { p_tenant_id: company.tenant_id, p_from: from, p_to: to, p_by: by }),
    supabase.rpc("receivables", { p_tenant_id: company.tenant_id }),
    supabase.rpc("at_risk_customers", { p_tenant_id: company.tenant_id }),
  ]);
  if (error || !h) return <p className="error-text" role="alert">{friendlyError(error)}</p>;
  const H = h as Health;
  const M = H.money;
  const profits = (prof ?? []) as Profit[];
  const top = profits.slice(0, 8);
  const bottom = profits.length > 8 ? profits.slice(-5).reverse() : [];
  const receivables = (recv ?? []) as Receivable[];
  const atRisk = (risk ?? []) as AtRisk[];
  const maxWork = Math.max(1, ...H.workload.map((w) => w.visits));
  const aging = [
    { label: "Not due yet", v: H.receivables.current },
    { label: "1–30 days late", v: H.receivables.d1_30 },
    { label: "31–60", v: H.receivables.d31_60 },
    { label: "61–90", v: H.receivables.d61_90 },
    { label: "90+", v: H.receivables.d90_plus },
  ];
  const maxAging = Math.max(1, ...aging.map((a) => Number(a.v)));
  const href = (p: Profit) => (p.key ? (by === "customer" ? `/clients/${p.key}` : by === "crew" ? `/routes` : null) : null);

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <div>
          <h1>Business health</h1>
          <p className={styles.muted}>{label}: {short(from)} – {short(to)}</p>
        </div>
        <nav className={styles.ranges} aria-label="Period">
          {[["month", "This month"], ["last_month", "Last month"], ["30d", "30 days"], ["ytd", "This year"]].map(([k, l]) => (
            <Link key={k} href={q({ range: k! })} className={k === rangeKey ? styles.on : undefined} aria-current={k === rangeKey ? "page" : undefined}>{l}</Link>
          ))}
        </nav>
      </header>

      {M.missing_rates > 0 && (
        <p className="notice">Labor is understated: {M.missing_rates} shift{M.missing_rates === 1 ? "" : "s"} or visit worker{M.missing_rates === 1 ? "" : "s"} have no pay rate. <Link href="/team">Add pay rates</Link>.</p>
      )}

      <section aria-labelledby="money" className={styles.section}>
        <h2 id="money">Money</h2>
        <div className={styles.kpis}>
          <Kpi label="Work done" value={money(M.work_done)} note={`${M.completed_visits} completed visits`} href={`/profit?from=${from}&to=${to}`} />
          <Kpi label="Gross profit" value={money(M.gross_profit)} note={`${pct(M.gross_margin_pct)} of work done`} tone={M.gross_profit < 0 ? "bad" : undefined} />
          <Kpi label="Labor" value={pct(M.labor_pct)} note={`${money(M.payroll)} payroll, ${M.paid_hours} paid hours`} href="/time" />
          <Kpi label="Collected" value={money(M.collected)} note={`${money(M.billed)} billed`} href="/invoices" />
          <Kpi label="Per paid hour" value={money(M.revenue_per_paid_hour)} note={`${pct(M.utilization_pct)} of paid time on site`} />
        </div>
        <table className={styles.table}>
          <caption className={styles.caption}>How gross profit is counted</caption>
          <tbody>
            <tr><td>Work done (completed visits, at their price)</td><td className={styles.num}>{money(M.work_done)}</td></tr>
            <tr><td>− Payroll (every paid hour × that day&apos;s pay rate)</td><td className={styles.num}>{money(M.payroll)}</td></tr>
            <tr className={styles.subRow}><td>of which on jobs (job costing: on-site + share of drive time)</td><td className={styles.num}>{money(M.job_labor)}</td></tr>
            <tr className={styles.subRow}><td>of which not tied to a visit (shop days, extra time)</td><td className={styles.num}>{money(M.unallocated_labor)}</td></tr>
            <tr><td>− Expenses recorded in the period</td><td className={styles.num}>{money(M.expenses)}</td></tr>
            <tr className={styles.total}><td>Gross profit</td><td className={styles.num}>{money(M.gross_profit)}</td></tr>
          </tbody>
        </table>
      </section>

      <section aria-labelledby="profit" className={styles.section}>
        <div className={styles.sectionHead}>
          <h2 id="profit">Profitability</h2>
          <nav className={styles.ranges} aria-label="Group by">
            {Object.entries(BY).map(([k, l]) => (
              <Link key={k} href={q({ by: k })} className={k === by ? styles.on : undefined} aria-current={k === by ? "page" : undefined}>{l}</Link>
            ))}
          </nav>
        </div>
        {profits.length === 0 ? (
          <p className={styles.muted}>No completed visits in this period.</p>
        ) : (
          <>
            <ProfitTable rows={top} title="Most profitable" href={href} />
            {bottom.length > 0 && <ProfitTable rows={bottom} title="Least profitable" href={href} />}
            <p className={styles.muted}>Margin = visit price − job labor − expenses tied to the visit. Same numbers as the <Link href={`/profit?from=${from}&to=${to}`}>Profit</Link> page.</p>
          </>
        )}
      </section>

      <section aria-labelledby="receivables" className={styles.section}>
        <h2 id="receivables">Owed to you</h2>
        <p className={styles.lead}><span className="figure">{money(H.receivables.total)}</span> on {H.receivables.count} open invoice{H.receivables.count === 1 ? "" : "s"}.</p>
        <ul className={styles.bars}>
          {aging.map((a) => (
            <li key={a.label}>
              <span>{a.label}</span>
              <span className={styles.barTrack} title={`${a.label}: ${money(a.v)}`}>
                <span className={a.label === "Not due yet" ? styles.barCalm : styles.barLate} style={{ width: `${(100 * Number(a.v)) / maxAging}%` }} />
              </span>
              <span className={styles.num}>{money(a.v)}</span>
            </li>
          ))}
        </ul>
        {receivables.length > 0 && (
          <details>
            <summary className={styles.summary}>Every open invoice</summary>
            <table className={styles.table}>
              <thead><tr><th>Invoice</th><th>Customer</th><th>Due</th><th className={styles.num}>Balance</th></tr></thead>
              <tbody>
                {receivables.map((r) => (
                  <tr key={r.invoice_id}>
                    <td><Link href={`/invoices/${r.invoice_id}`}>{r.number ?? "Invoice"}</Link></td>
                    <td>{r.client_id ? <Link href={`/clients/${r.client_id}`}>{r.client_name}</Link> : "–"}</td>
                    <td className={r.days_overdue > 0 ? styles.late : undefined}>{r.due_on ? short(r.due_on) : "On receipt"}{r.days_overdue > 0 ? ` (${r.days_overdue} days late)` : ""}</td>
                    <td className={styles.num}>{money(r.balance)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </details>
        )}
      </section>

      <section aria-labelledby="pipeline" className={styles.section}>
        <h2 id="pipeline">Estimates</h2>
        <div className={styles.kpis}>
          <Kpi label="Waiting on customers" value={money(H.pipeline.open_value)} note={`${H.pipeline.open} estimates`} href="/estimates" />
          <Kpi label="Won" value={money(H.pipeline.won_value)} note={`${H.pipeline.won} of ${H.pipeline.sent} sent`} />
          <Kpi label="Win rate" value={pct(H.pipeline.win_rate_pct)} note={`${H.pipeline.lost} declined or expired`} />
          <Kpi label="Days to answer" value={H.pipeline.avg_days_to_decision == null ? "–" : String(H.pipeline.avg_days_to_decision)} note="average, sent → decided" />
        </div>
        <p className={styles.muted}>Estimates created in this period. Win rate counts only estimates that got an answer.</p>
      </section>

      <section aria-labelledby="customers" className={styles.section}>
        <h2 id="customers">Customers and recurring work</h2>
        <div className={styles.kpis}>
          <Kpi label="Recurring revenue" value={`${money(H.recurring.monthly_value)}/mo`} note={`${H.recurring.jobs} recurring jobs, ${H.recurring.customers} customers`} />
          <Kpi label="Served" value={String(H.customers.served_in_period)} note={`${H.customers.active} active customers`} href="/clients" />
          <Kpi label="New" value={String(H.customers.new_in_period)} note="first completed visit this period" />
          <Kpi label="Lost" value={String(H.customers.lost_in_period)} note="marked lost or inactive" tone={H.customers.lost_in_period > 0 ? "bad" : undefined} />
        </div>
        {atRisk.length > 0 && (
          <>
            <h3>Drifting away ({atRisk.length})</h3>
            <p className={styles.muted}>Served in the last 6 months, nothing in 45 days, nothing scheduled, no recurring plan.</p>
            <table className={styles.table}>
              <thead><tr><th>Customer</th><th>Last visit</th><th className={styles.num}>Last 6 months</th><th className={styles.num}>Owes</th></tr></thead>
              <tbody>
                {atRisk.map((c) => (
                  <tr key={c.client_id}>
                    <td><Link href={`/clients/${c.client_id}`}>{c.name}</Link></td>
                    <td>{short(c.last_visit)}</td>
                    <td className={styles.num}>{money(c.revenue_180d)} · {c.visits_180d} visits</td>
                    <td className={styles.num}>{Number(c.open_balance) > 0 ? money(c.open_balance) : "–"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )}
      </section>

      {H.crews.length > 0 && (
        <section aria-labelledby="crews" className={styles.section}>
          <h2 id="crews">Crews</h2>
          <table className={styles.table}>
            <thead><tr><th>Crew</th><th className={styles.num}>Visits</th><th className={styles.num}>Work done</th><th className={styles.num}>Margin</th>
              <th className={styles.num}>Paid hours</th><th className={styles.num}>On site</th></tr></thead>
            <tbody>
              {H.crews.map((c) => (
                <tr key={c.crew_id}>
                  <td>{c.crew}</td>
                  <td className={styles.num}>{c.visits}</td>
                  <td className={styles.num}>{money(c.revenue)}</td>
                  <td className={styles.num}>{money(c.margin)}</td>
                  <td className={styles.num}>{c.paid_hours}</td>
                  <td className={styles.num}>{pct(c.utilization_pct)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className={styles.muted}>On site = time clocked in during visits ÷ all paid hours of current crew members. The rest is driving, loading and breaks.</p>
        </section>
      )}

      <section aria-labelledby="workload" className={styles.section}>
        <h2 id="workload">Next two weeks</h2>
        <ul className={styles.bars}>
          {H.workload.map((w) => (
            <li key={w.date}>
              <Link href={`/routes?date=${w.date}`}>{new Date(`${w.date}T12:00:00Z`).toLocaleDateString("en-US", { weekday: "short", month: "numeric", day: "numeric", timeZone: "UTC" })}</Link>
              <span className={styles.barTrack} title={`${w.visits} visits, ${Math.round(w.minutes / 60)} h planned, ${money(w.value)}`}>
                <span className={styles.barCalm} style={{ width: `${(100 * w.visits) / maxWork}%` }} />
              </span>
              <span className={styles.num}>
                {w.visits} · {money(w.value)}
                {w.unassigned > 0 && <Link href={`/schedule?week=${w.date}`} className={styles.late}> · {w.unassigned} no crew</Link>}
              </span>
            </li>
          ))}
        </ul>
        <p className={styles.muted}>Only visits already on the schedule. Recurring visits appear once the schedule is filled for that week.</p>
      </section>
    </div>
  );
}

function Kpi({ label, value, note, href, tone }: { label: string; value: string; note?: string; href?: string; tone?: "bad" }) {
  const body = (
    <>
      <span className={styles.kpiLabel}>{label}</span>
      <span className={`${styles.kpiValue} figure ${tone === "bad" ? styles.late : ""}`}>{value}</span>
      {note && <span className={styles.kpiNote}>{note}</span>}
    </>
  );
  return href ? <Link href={href} className={styles.kpi}>{body}</Link> : <div className={styles.kpi}>{body}</div>;
}

function ProfitTable({ rows, title, href }: { rows: Profit[]; title: string; href: (p: Profit) => string | null }) {
  return (
    <table className={styles.table}>
      <caption className={styles.caption}>{title}</caption>
      <thead><tr><th>Name</th><th className={styles.num}>Visits</th><th className={styles.num}>Work done</th><th className={styles.num}>Labor</th>
        <th className={styles.num}>Margin</th></tr></thead>
      <tbody>
        {rows.map((p) => {
          const h = href(p);
          return (
            <tr key={p.key ?? p.label}>
              <td>{h ? <Link href={h}>{p.label}</Link> : p.label}{p.sub && <span className={styles.sub}>{p.sub}</span>}</td>
              <td className={styles.num}>{p.visits}</td>
              <td className={styles.num}>{money(p.revenue)}</td>
              <td className={styles.num}>{money(p.labor_cost)}{p.missing_rates > 0 ? "*" : ""}</td>
              <td className={`${styles.num} ${Number(p.margin) < 0 ? styles.late : ""}`}>{money(p.margin)} <span className={styles.sub}>{pct(p.margin_pct)}</span></td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
