import type { Metadata } from "next";
import {
  formatClockTime,
  formatDuration,
  weekStart,
  type TimesheetRow,
  type WeeklyHoursRow,
} from "@crew/shared";
import { currentCompany } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import { Elapsed } from "./elapsed";
import styles from "./today.module.css";

export const metadata: Metadata = { title: "Today" };
export const dynamic = "force-dynamic";

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

  const [{ data: shifts, error: shiftsError }, { data: weekly, error: weeklyError }] = await Promise.all([
    supabase.rpc("timesheet", { p_tenant_id: company.tenant_id, p_from: yesterday, p_to: today }),
    supabase.rpc("weekly_hours", { p_tenant_id: company.tenant_id, p_week_start: week }),
  ]);
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
