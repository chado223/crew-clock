import type { Metadata } from "next";
import {
  formatClockTime,
  formatDuration,
  weekStart,
  type TimesheetRow,
  type WeeklyHoursRow,
} from "@crew/shared";
import Link from "next/link";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import { Elapsed } from "./elapsed";
import styles from "./today.module.css";

export const metadata: Metadata = { title: "Today" };
export const dynamic = "force-dynamic";

type Overview = {
  visits_today: { total: number; done: number; working: number; left: number; skipped: number; value_done: number };
  crews_today: { crew_id: string | null; crew: string; done: number; total: number }[];
  tomorrow_unassigned: number;
  weather_alerts: number;
  new_requests: number;
  leads: number;
  estimates_waiting: { count: number; value: number; expiring_soon: number };
  estimates_approved_not_scheduled: number;
  receivables: { open: number; open_count: number; overdue: number; overdue_count: number };
  unbilled_visits: { count: number; value: number };
  collected: { week: number; month: number };
  work_done: { week: number; month: number };
  messages_problems: number;
  open_shifts_over_12h: number;
};

const money = (n: number) =>
  new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(Number(n));
const plural = (n: number, one: string, many: string) => `${n} ${n === 1 ? one : many}`;

/** Things the owner should act on, most urgent first. Only what's actually true shows. */
function todos(o: Overview, needsReview: number) {
  const t: { href: string; text: string; urgent?: boolean }[] = [];
  if (o.open_shifts_over_12h) t.push({ href: "/time", text: `${plural(o.open_shifts_over_12h, "person has", "people have")} been clocked in over 12 hours. Forgot to clock out?`, urgent: true });
  if (o.weather_alerts) t.push({ href: "/weather", text: `${plural(o.weather_alerts, "visit has", "visits have")} bad weather coming`, urgent: true });
  if (o.tomorrow_unassigned) t.push({ href: "/schedule", text: `${plural(o.tomorrow_unassigned, "visit", "visits")} tomorrow with no crew`, urgent: true });
  if (o.new_requests) t.push({ href: "/clients", text: `${plural(o.new_requests, "new service request", "new service requests")} from customers` });
  if (o.estimates_approved_not_scheduled) t.push({ href: "/estimates", text: `${plural(o.estimates_approved_not_scheduled, "approved estimate", "approved estimates")} to turn into jobs` });
  if (o.estimates_waiting.expiring_soon) t.push({ href: "/estimates", text: `${plural(o.estimates_waiting.expiring_soon, "estimate expires", "estimates expire")} this week without an answer` });
  if (o.receivables.overdue_count) t.push({ href: "/invoices", text: `${money(o.receivables.overdue)} overdue on ${plural(o.receivables.overdue_count, "invoice", "invoices")}` });
  if (o.unbilled_visits.count) t.push({ href: "/invoices", text: `${plural(o.unbilled_visits.count, "finished visit", "finished visits")} not invoiced yet (${money(o.unbilled_visits.value)})` });
  if (needsReview) t.push({ href: "/time", text: `${plural(needsReview, "shift needs", "shifts need")} review before payroll` });
  if (o.messages_problems) t.push({ href: "/messages", text: `${plural(o.messages_problems, "message", "messages")} didn't go out this week` });
  if (o.leads) t.push({ href: "/clients?status=lead", text: `${plural(o.leads, "lead", "leads")} to follow up` });
  return t;
}

function localDate(date: Date, timeZone: string) {
  return new Intl.DateTimeFormat("en-CA", { timeZone }).format(date);
}

export default async function TodayPage() {
  const { company } = await currentCompany();
  const supabase = await supabaseServer();
  const now = new Date();
  const today = localDate(now, company.timezone);
  const yesterday = localDate(new Date(now.getTime() - 86_400_000), company.timezone);
  const week = weekStart(now, company.timezone);

  const manager = isManager(company);
  const [{ data: shifts, error: shiftsError }, { data: weekly, error: weeklyError }, { data: ov }] = await Promise.all([
    supabase.rpc("timesheet", { p_tenant_id: company.tenant_id, p_from: yesterday, p_to: today }),
    supabase.rpc("weekly_hours", { p_tenant_id: company.tenant_id, p_week_start: week }),
    manager ? supabase.rpc("owner_overview", { p_tenant_id: company.tenant_id }) : Promise.resolve({ data: null }),
  ]);
  const o = ov as Overview | null;
  if (shiftsError) throw shiftsError;

  const rows = (shifts ?? []) as TimesheetRow[];
  const onClock = rows.filter((r) => r.status === "open");
  const finishedToday = rows.filter((r) => r.work_date === today && r.status === "closed");
  const weekRows = (weekly ?? []) as WeeklyHoursRow[];
  const needsReview = weekRows.reduce((n, r) => n + r.needs_review_shifts, 0);

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
                {Number(o.visits_today.value_done) > 0 && <>, <span className="figure">{money(o.visits_today.value_done)}</span> of work</>}.
              </>
            )}
          </p>
          {o.crews_today.length > 0 && (
            <ul className={styles.crewBars}>
              {o.crews_today.map((c) => (
                <li key={c.crew_id ?? "none"}>
                  <span>{c.crew}</span>
                  <span className={styles.bar} aria-hidden="true"><span style={{ width: `${c.total ? (100 * c.done) / c.total : 0}%` }} /></span>
                  <span className="figure">{c.done}/{c.total}</span>
                </li>
              ))}
            </ul>
          )}
          <p className={styles.money}>
            This week: {money(o.work_done.week)} of work done, {money(o.collected.week)} collected.
            {" "}Owed to you: {money(o.receivables.open)}{o.receivables.overdue > 0 ? `, ${money(o.receivables.overdue)} overdue` : ""}.
            {o.estimates_waiting.count > 0 && ` ${money(o.estimates_waiting.value)} in estimates waiting on customers.`}
          </p>
        </section>
      )}

      {o && (() => {
        const list = todos(o, needsReview);
        return (
          <section aria-labelledby="todo" className={styles.block}>
            <h2 id="todo">Needs attention</h2>
            {list.length === 0 ? (
              <p className={styles.empty}>Nothing waiting on you.</p>
            ) : (
              <ul className={styles.todos}>
                {list.map((t) => (
                  <li key={t.text} className={t.urgent ? styles.urgent : undefined}>
                    <Link href={t.href}>{t.text}</Link>
                  </li>
                ))}
              </ul>
            )}
          </section>
        );
      })()}

      <section aria-labelledby="on-clock" className={styles.onClock}>
        <h2 id="on-clock">
          On the clock <span className={`${styles.count} figure`}>{onClock.length}</span>
        </h2>
        {onClock.length === 0 ? (
          <p className={styles.empty}>Nobody is clocked in right now.</p>
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
        <h2 id="finished">Finished today</h2>
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
          <h2 id="week">This week</h2>
          {needsReview > 0 && (
            <p className="notice">
              {needsReview} {needsReview === 1 ? "shift needs" : "shifts need"} review before payroll.
            </p>
          )}
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
