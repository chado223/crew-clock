import type { Metadata } from "next";
import Link from "next/link";
import {
  formatClockTime,
  formatDuration,
  weekStart,
  type TimesheetRow,
  type WeeklyHoursRow,
} from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import { recordHref } from "@/lib/links";
import { Elapsed } from "./elapsed";
import styles from "./today.module.css";

export const metadata: Metadata = { title: "Today" };
export const dynamic = "force-dynamic";

type Overview = {
  visits_today: { total: number; done: number; working: number; left: number; skipped: number; value_done: number };
  receivables: { open: number; open_count: number; overdue: number; overdue_count: number };
  estimates_waiting: { count: number; value: number; expiring_soon: number };
  collected: { week: number; month: number };
  work_done: { week: number; month: number };
};
type Attention = {
  kind: string; severity: number; title: string; detail: string | null;
  ref_type: string; ref_id: string | null; ref_date: string | null; client_id: string | null; amount: number | null;
};
type Stop = {
  visit_id: string; status: string; job_title: string; client_name: string | null; client_id: string | null;
  address: string | null; crew_id: string | null; crew_name: string | null; assignees: string[]; price: number | null;
};

const money = (n: number | null | undefined) =>
  new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(Number(n ?? 0));
const STATUS_LABEL: Record<string, string> = { scheduled: "To do", in_progress: "Working", completed: "Done", skipped: "Skipped" };
const GROUPS: { severity: number; title: string }[] = [
  { severity: 1, title: "Act today" },
  { severity: 2, title: "This week" },
  { severity: 3, title: "When you can" },
];

function localDate(date: Date, timeZone: string) {
  return new Intl.DateTimeFormat("en-CA", { timeZone }).format(date);
}

export default async function TodayPage() {
  const { company } = await currentCompany();
  const supabase = await supabaseServer();
  const manager = isManager(company);
  const now = new Date();
  const today = localDate(now, company.timezone);
  const yesterday = localDate(new Date(now.getTime() - 86_400_000), company.timezone);
  const week = weekStart(now, company.timezone);
  const none = Promise.resolve({ data: null, error: null });
  if (manager) {
    // Keep the next three weeks filled from recurring jobs (idempotent; the background job does this too once hosted).
    const until = localDate(new Date(now.getTime() + 21 * 86_400_000), company.timezone);
    const { error: fillError } = await supabase.rpc("generate_visits", { p_tenant_id: company.tenant_id, p_from: today, p_to: until });
    if (fillError) console.error("[today] schedule fill failed:", fillError.message);
  }

  const [{ data: shifts, error: shiftsError }, { data: weekly, error: weeklyError }, { data: ov }, { data: att }, { data: sched }] = await Promise.all([
    supabase.rpc("timesheet", { p_tenant_id: company.tenant_id, p_from: yesterday, p_to: today }),
    supabase.rpc("weekly_hours", { p_tenant_id: company.tenant_id, p_week_start: week }),
    manager ? supabase.rpc("owner_overview", { p_tenant_id: company.tenant_id }) : none,
    manager ? supabase.rpc("owner_attention", { p_tenant_id: company.tenant_id }) : none,
    manager ? supabase.rpc("schedule", { p_tenant_id: company.tenant_id, p_from: today, p_to: today }) : none,
  ]);
  if (shiftsError) throw shiftsError;

  const rows = (shifts ?? []) as TimesheetRow[];
  const onClock = rows.filter((r) => r.status === "open");
  const finishedToday = rows.filter((r) => r.work_date === today && r.status === "closed");
  const weekRows = (weekly ?? []) as WeeklyHoursRow[];
  const needsReview = weekRows.reduce((n, r) => n + r.needs_review_shifts, 0);
  const o = ov as Overview | null;
  const items = [...((att ?? []) as Attention[])];
  if (manager && needsReview > 0) {
    items.push({ kind: "review", severity: 2, title: `${needsReview} ${needsReview === 1 ? "shift needs" : "shifts need"} review before payroll`,
      detail: null, ref_type: "time_entry", ref_id: null, ref_date: null, client_id: null, amount: null });
  }
  const stops = ((sched ?? []) as Stop[]).filter((s) => s.status !== "canceled");
  const scheduledValue = stops.reduce((n, s) => n + Number(s.price ?? 0), 0);
  const crews = new Map<string, { name: string; stops: Stop[] }>();
  for (const s of stops) {
    const key = s.crew_id ?? "none";
    const c = crews.get(key) ?? { name: s.crew_name ?? "No crew yet", stops: [] };
    c.stops.push(s);
    crews.set(key, c);
  }

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <h1>Today</h1>
        <p className={styles.date}>
          {new Intl.DateTimeFormat("en-US", { timeZone: company.timezone, weekday: "long", month: "long", day: "numeric" }).format(now)}
        </p>
      </header>

      {o && (
        <section aria-labelledby="day" className={styles.overview}>
          <h2 id="day" className={styles.srOnly}>The day so far</h2>
          <p className={styles.headline}>
            {o.visits_today.total === 0 ? (
              "No visits scheduled today."
            ) : (
              <>
                <span className="figure">{o.visits_today.done}</span> of {o.visits_today.total} visits done
                {o.visits_today.working > 0 && <>, {o.visits_today.working} in progress</>}
                {o.visits_today.skipped > 0 && <>, {o.visits_today.skipped} skipped</>}.{" "}
                <span className="figure">{money(o.visits_today.value_done)}</span> of {money(scheduledValue)} scheduled work finished.
              </>
            )}
          </p>
          <p className={styles.money}>
            This week: {money(o.work_done.week)} of work done, {money(o.collected.week)} collected.{" "}
            Customers owe {money(o.receivables.open)}
            {o.receivables.overdue > 0 && <>, <Link href="/insights#receivables">{money(o.receivables.overdue)} overdue</Link></>}.
            {o.estimates_waiting.count > 0 && <> <Link href="/estimates">{money(o.estimates_waiting.value)} in estimates</Link> waiting on customers.</>}
            {" "}<Link href="/insights">Business health →</Link>
          </p>
        </section>
      )}

      {manager && (
        <section aria-labelledby="todo" className={styles.block}>
          <h2 id="todo">Needs attention {items.length > 0 && <span className={styles.badge}>{items.length}</span>}</h2>
          {items.length === 0 ? (
            <p className={styles.empty}>Nothing waiting on you.</p>
          ) : (
            <div className={styles.groups}>
              {GROUPS.map((g) => {
                const list = items.filter((i) => i.severity === g.severity);
                if (!list.length) return null;
                return (
                  <div key={g.severity} className={styles.group}>
                    <h3 className={g.severity === 1 ? styles.urgentTitle : undefined}>{g.title}</h3>
                    <ul className={styles.todos}>
                      {list.map((i, n) => (
                        <li key={`${i.kind}-${i.ref_id ?? n}`} className={g.severity === 1 ? styles.urgent : undefined}>
                          <Link href={recordHref(i.ref_type, i.ref_id, i.ref_date, i.kind)}>
                            <span className={styles.todoTitle}>{i.title}</span>
                            {i.detail && <span className={styles.todoDetail}>{i.detail}</span>}
                            {i.amount != null && Number(i.amount) > 0 && <span className={`${styles.todoAmount} figure`}>{money(i.amount)}</span>}
                          </Link>
                        </li>
                      ))}
                    </ul>
                  </div>
                );
              })}
            </div>
          )}
        </section>
      )}

      {manager && crews.size > 0 && (
        <section aria-labelledby="crews" className={styles.block}>
          <h2 id="crews">Crews today</h2>
          <div className={styles.crewGrid}>
            {[...crews.entries()].map(([key, c]) => {
              const done = c.stops.filter((s) => s.status === "completed" || s.status === "skipped").length;
              return (
                <article key={key} className={styles.crewCard}>
                  <header className={styles.crewHead}>
                    <h3>{c.name}</h3>
                    <span className="figure">{done}/{c.stops.length}</span>
                  </header>
                  <span className={styles.bar} role="img" aria-label={`${done} of ${c.stops.length} stops finished`}>
                    <span style={{ width: `${c.stops.length ? (100 * done) / c.stops.length : 0}%` }} />
                  </span>
                  <ol className={styles.crewStops}>
                    {c.stops.map((s) => (
                      <li key={s.visit_id}>
                        <span className={`${styles.chip} ${styles[`chip_${s.status}`] ?? ""}`}>{STATUS_LABEL[s.status] ?? s.status}</span>
                        {s.client_id ? <Link href={`/clients/${s.client_id}`}>{s.client_name}</Link> : <span>{s.job_title}</span>}
                      </li>
                    ))}
                  </ol>
                  <Link href={`/routes?date=${today}`} className={styles.small}>Open route</Link>
                </article>
              );
            })}
          </div>
        </section>
      )}

      <section aria-labelledby="on-clock" className={styles.onClock}>
        <h2 id="on-clock">
          On the clock <span className={`${styles.count} figure`}>{onClock.length}</span>
        </h2>
        {onClock.length === 0 ? (
          <p>Nobody is clocked in right now.</p>
        ) : (
          <ul className={styles.roster}>
            {onClock.map((r) => (
              <li key={r.entry_id} className={styles.person}>
                <span className={styles.name}>{r.employee_name}</span>
                <span className={styles.since}>since {formatClockTime(r.clock_in, company.timezone)}</span>
                <Elapsed since={r.clock_in} className={`${styles.elapsed} figure`} />
              </li>
            ))}
          </ul>
        )}
      </section>

      <section aria-labelledby="finished" className={styles.block}>
        <h2 id="finished">Finished shifts today</h2>
        {finishedToday.length === 0 ? (
          <p className={styles.empty}>No completed shifts yet today.</p>
        ) : (
          <table className={styles.table}>
            <thead>
              <tr>
                <th scope="col">Name</th>
                <th scope="col">In</th>
                <th scope="col">Out</th>
                <th scope="col" className={styles.num}>Hours</th>
              </tr>
            </thead>
            <tbody>
              {finishedToday.map((r) => (
                <tr key={r.entry_id}>
                  <td>{r.employee_name}</td>
                  <td>{formatClockTime(r.clock_in, company.timezone)}</td>
                  <td>{r.clock_out ? formatClockTime(r.clock_out, company.timezone) : ""}</td>
                  <td className={`${styles.num} figure`}>{formatDuration(r.worked_seconds)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      {!weeklyError && (
        <section aria-labelledby="week" className={styles.block}>
          <h2 id="week">Hours this week</h2>
          {weekRows.length === 0 ? (
            <p className={styles.empty}>No hours recorded this week.</p>
          ) : (
            <table className={styles.table}>
              <thead>
                <tr>
                  <th scope="col">Name</th>
                  <th scope="col" className={styles.num}>Regular</th>
                  <th scope="col" className={styles.num}>Overtime</th>
                  <th scope="col" className={styles.num}>Total</th>
                </tr>
              </thead>
              <tbody>
                {weekRows.map((r) => (
                  <tr key={r.employee_id}>
                    <td>{r.employee_name}</td>
                    <td className={`${styles.num} figure`}>{formatDuration(r.regular_seconds)}</td>
                    <td className={`${styles.num} figure`}>{r.overtime_seconds > 0 ? formatDuration(r.overtime_seconds) : "–"}</td>
                    <td className={`${styles.num} figure`}>{formatDuration(r.total_seconds)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      )}
    </div>
  );
}
