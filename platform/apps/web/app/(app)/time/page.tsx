import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import {
  formatClockTime,
  formatDuration,
  friendlyError,
  isoToZonedLocal,
  weekStart,
  zonedLocalToIso,
  type TimesheetRow,
  type WeeklyHoursRow,
} from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "./time.module.css";

export const metadata: Metadata = { title: "Time" };
export const dynamic = "force-dynamic";

function addDays(ymd: string, n: number) {
  const d = new Date(`${ymd}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

function back(week: string, params: Record<string, string>) {
  const q = new URLSearchParams({ week, ...params });
  redirect(`/time?${q.toString()}`);
}

async function correct(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const week = String(formData.get("week"));
  const tz = company.timezone;
  const outLocal = String(formData.get("clock_out") ?? "");
  let error;
  try {
    ({ error } = await (await supabaseServer()).rpc("correct_time_entry", {
      p_entry_id: String(formData.get("entry_id")),
      p_clock_in: zonedLocalToIso(String(formData.get("clock_in")), tz),
      p_clock_out: outLocal ? zonedLocalToIso(outLocal, tz) : (null as unknown as string),
      p_reason: String(formData.get("reason") ?? ""),
    }));
  } catch (e) {
    error = e;
  }
  revalidatePath("/time");
  back(week, error ? { error: friendlyError(error) } : { saved: "Shift updated." });
}

async function voidShift(formData: FormData) {
  "use server";
  const week = String(formData.get("week"));
  const { error } = await (await supabaseServer()).rpc("void_time_entry", {
    p_entry_id: String(formData.get("entry_id")),
    p_reason: String(formData.get("reason") ?? ""),
  });
  revalidatePath("/time");
  back(week, error ? { error: friendlyError(error) } : { saved: "Shift removed from payroll. It stays in the record." });
}

async function addShift(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const week = String(formData.get("week"));
  let error;
  try {
    ({ error } = await (await supabaseServer()).rpc("add_time_entry", {
      p_tenant_id: company.tenant_id,
      p_employee_id: String(formData.get("employee_id")),
      p_clock_in: zonedLocalToIso(String(formData.get("clock_in")), company.timezone),
      p_clock_out: zonedLocalToIso(String(formData.get("clock_out")), company.timezone),
      p_reason: String(formData.get("reason") ?? ""),
    }));
  } catch (e) {
    error = e;
  }
  revalidatePath("/time");
  back(week, error ? { error: friendlyError(error) } : { saved: "Shift added." });
}

async function fixBreak(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const week = String(formData.get("week"));
  const endLocal = String(formData.get("ended_at") ?? "");
  let error;
  try {
    ({ error } = await (await supabaseServer()).rpc("correct_break", {
      p_break_id: String(formData.get("break_id")),
      p_started_at: zonedLocalToIso(String(formData.get("started_at")), company.timezone),
      p_ended_at: endLocal ? zonedLocalToIso(endLocal, company.timezone) : (null as unknown as string),
      p_paid: formData.get("paid") === "on",
      p_reason: String(formData.get("reason") ?? ""),
    }));
  } catch (e) {
    error = e;
  }
  revalidatePath("/time");
  back(week, error ? { error: friendlyError(error) } : { saved: "Break updated." });
}

async function resolveProblem(formData: FormData) {
  "use server";
  const week = String(formData.get("week"));
  const { error } = await (await supabaseServer()).rpc("resolve_sync_problem", {
    p_id: String(formData.get("id")),
    p_resolution: String(formData.get("resolution") ?? ""),
  });
  revalidatePath("/time");
  back(week, error ? { error: friendlyError(error) } : { saved: "Marked as handled." });
}

const STATUS_LABEL: Record<string, string> = {
  open: "On the clock",
  closed: "",
  needs_review: "Needs review",
  voided: "Removed",
};

export default async function TimePage({
  searchParams,
}: {
  searchParams: Promise<{ week?: string; error?: string; saved?: string }>;
}) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const tz = company.timezone;
  const current = weekStart(new Date(), tz);
  const week = sp.week && /^\d{4}-\d{2}-\d{2}$/.test(sp.week) ? weekStart(new Date(`${sp.week}T12:00:00Z`), "UTC") : current;
  const weekEnd = addDays(week, 6);

  const supabase = await supabaseServer();
  const [{ data: summary, error: sErr }, { data: shifts, error: tErr }, { data: employees }, { data: breakRows }, { data: problemRows }] = await Promise.all([
    supabase.rpc("weekly_hours", { p_tenant_id: company.tenant_id, p_week_start: week }),
    supabase.rpc("timesheet", { p_tenant_id: company.tenant_id, p_from: week, p_to: weekEnd, p_include_voided: true }),
    supabase.from("employees").select("id, display_name").eq("tenant_id", company.tenant_id).eq("status", "active").order("display_name"),
    supabase.from("time_entry_breaks").select("id, time_entry_id, started_at, ended_at, paid").eq("tenant_id", company.tenant_id)
      .gte("started_at", zonedLocalToIso(`${addDays(week, -1)}T00:00`, tz)).lte("started_at", zonedLocalToIso(`${addDays(week, 8)}T00:00`, tz))
      .order("started_at"),
    supabase.from("sync_problems").select("id, action_label, happened_at, error, employees(display_name)").eq("tenant_id", company.tenant_id)
      .is("resolved_at", null).order("happened_at"),
  ]);
  const breaksByShift = new Map<string, { id: string; started_at: string; ended_at: string | null; paid: boolean }[]>();
  for (const b of (breakRows ?? []) as { id: string; time_entry_id: string; started_at: string; ended_at: string | null; paid: boolean }[]) {
    breaksByShift.set(b.time_entry_id, [...(breaksByShift.get(b.time_entry_id) ?? []), b]);
  }
  const problems = (problemRows ?? []) as unknown as { id: string; action_label: string; happened_at: string; error: string; employees: { display_name: string } | null }[];
  const loadError = sErr ?? tErr;
  const rows = (shifts ?? []) as TimesheetRow[];
  const totals = (summary ?? []) as WeeklyHoursRow[];
  const fmtDay = (ymd: string) =>
    new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short", month: "short", day: "numeric" }).format(new Date(`${ymd}T12:00:00Z`));

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <h1>Time</h1>
        <nav className={styles.weekNav} aria-label="Week">
          <Link href={`/time?week=${addDays(week, -7)}`} className="button quiet">
            Previous week
          </Link>
          <p className={styles.range}>
            {fmtDay(week)} to {fmtDay(weekEnd)}
          </p>
          {week !== current && (
            <Link href={`/time?week=${addDays(week, 7)}`} className="button quiet">
              Next week
            </Link>
          )}
        </nav>
      </header>

      {sp.saved && <p className="notice" role="status">{sp.saved}</p>}
      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}
      {loadError && <p className="error-text">{friendlyError(loadError)}</p>}

      {problems.length > 0 && (
        <section aria-labelledby="phone-problems" id="phone-problems">
          <h2 id="phone-problems-h" className={styles.h2}>Phone problems to fix</h2>
          <p className={styles.empty}>A crew phone saved these with no signal, and the server couldn&apos;t accept them. Fix the shift below or add it, then mark handled.</p>
          <ul className={styles.shifts}>
            {problems.map((p) => (
              <li key={p.id} className={styles.shift}>
                <p><strong>{p.employees?.display_name ?? "Someone"}</strong>: {p.action_label} at {fmtDay(new Intl.DateTimeFormat("en-CA", { timeZone: tz }).format(new Date(p.happened_at)))}, {formatClockTime(p.happened_at, tz)}. {friendlyError(p.error)}</p>
                <form action={resolveProblem} className={styles.voidForm}>
                  <input type="hidden" name="id" value={p.id} />
                  <input type="hidden" name="week" value={week} />
                  <label htmlFor={`res-${p.id}`} className={styles.voidLabel}>What you did</label>
                  <input id={`res-${p.id}`} name="resolution" required minLength={3} className="input" placeholder="e.g. Added the clock-out by hand" />
                  <button className="button quiet" type="submit">Mark handled</button>
                </form>
              </li>
            ))}
          </ul>
        </section>
      )}

      <section aria-labelledby="totals">
        <h2 id="totals" className={styles.h2}>Weekly totals</h2>
        {totals.length === 0 ? (
          <p className={styles.empty}>No hours this week.</p>
        ) : (
          <table className={styles.table}>
            <thead>
              <tr>
                <th scope="col">Name</th>
                <th scope="col" className={styles.num}>Regular</th>
                <th scope="col" className={styles.num}>Overtime</th>
                <th scope="col" className={styles.num}>Total</th>
                <th scope="col">Open</th>
              </tr>
            </thead>
            <tbody>
              {totals.map((t) => (
                <tr key={t.employee_id}>
                  <td>{t.employee_name}</td>
                  <td className={`${styles.num} figure`}>{formatDuration(t.regular_seconds)}</td>
                  <td className={`${styles.num} figure`}>{t.overtime_seconds > 0 ? formatDuration(t.overtime_seconds) : "–"}</td>
                  <td className={`${styles.num} figure`}>{formatDuration(t.total_seconds)}</td>
                  <td>{t.open_shifts + t.needs_review_shifts > 0 ? `${t.open_shifts + t.needs_review_shifts} to fix` : ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section aria-labelledby="shifts">
        <h2 id="shifts" className={styles.h2}>Shifts</h2>
        {rows.length === 0 ? (
          <p className={styles.empty}>No shifts recorded this week.</p>
        ) : (
          <ul className={styles.shifts}>
            {rows.map((r) => (
              <li key={r.entry_id} className={`${styles.shift} ${r.status === "voided" ? styles.voided : ""}`}>
                <details>
                  <summary className={styles.summary}>
                    <span className={styles.who}>{r.employee_name}</span>
                    <span className={styles.when}>
                      {fmtDay(r.work_date)}, {formatClockTime(r.clock_in, tz)}
                      {r.clock_out ? ` to ${formatClockTime(r.clock_out, tz)}` : ""}
                    </span>
                    <span className={`${styles.hours} figure`}>{formatDuration(r.worked_seconds)}</span>
                    {STATUS_LABEL[r.status] && <span className={styles[`status_${r.status}`]}>{STATUS_LABEL[r.status]}</span>}
                  </summary>
                  {r.status !== "voided" && (
                    <div className={styles.edit}>
                      <form action={correct} className={styles.form}>
                        <input type="hidden" name="entry_id" value={r.entry_id} />
                        <input type="hidden" name="week" value={week} />
                        <div className="field">
                          <label htmlFor={`in-${r.entry_id}`}>Clock in</label>
                          <input id={`in-${r.entry_id}`} name="clock_in" type="datetime-local" required className="input"
                            defaultValue={isoToZonedLocal(r.clock_in, tz)} />
                        </div>
                        <div className="field">
                          <label htmlFor={`out-${r.entry_id}`}>Clock out</label>
                          <input id={`out-${r.entry_id}`} name="clock_out" type="datetime-local" className="input"
                            defaultValue={r.clock_out ? isoToZonedLocal(r.clock_out, tz) : ""} />
                        </div>
                        <div className={`field ${styles.wide}`}>
                          <label htmlFor={`why-${r.entry_id}`}>Reason for the change</label>
                          <input id={`why-${r.entry_id}`} name="reason" required minLength={3} className="input"
                            placeholder="e.g. Forgot to clock out, confirmed with crew lead" />
                          <p className="hint">Saved with your name in the change history. The employee's original punch is kept.</p>
                        </div>
                        <button className="button" type="submit">Save change</button>
                      </form>
                      {(breaksByShift.get(r.entry_id) ?? []).map((b) => (
                        <form key={b.id} action={fixBreak} className={styles.form}>
                          <input type="hidden" name="break_id" value={b.id} />
                          <input type="hidden" name="week" value={week} />
                          <div className="field">
                            <label htmlFor={`bs-${b.id}`}>Break start</label>
                            <input id={`bs-${b.id}`} name="started_at" type="datetime-local" required className="input" defaultValue={isoToZonedLocal(b.started_at, tz)} />
                          </div>
                          <div className="field">
                            <label htmlFor={`be-${b.id}`}>Break end</label>
                            <input id={`be-${b.id}`} name="ended_at" type="datetime-local" className="input" defaultValue={b.ended_at ? isoToZonedLocal(b.ended_at, tz) : ""} />
                          </div>
                          <label className="field"><span><input type="checkbox" name="paid" defaultChecked={b.paid} /> Paid break</span></label>
                          <div className={`field ${styles.wide}`}>
                            <label htmlFor={`br-${b.id}`}>Reason</label>
                            <input id={`br-${b.id}`} name="reason" required minLength={3} className="input" placeholder="e.g. Forgot to end lunch, took 30 min" />
                          </div>
                          <button className="button quiet" type="submit">Save break</button>
                        </form>
                      ))}
                      <form action={voidShift} className={styles.voidForm}>
                        <input type="hidden" name="entry_id" value={r.entry_id} />
                        <input type="hidden" name="week" value={week} />
                        <label htmlFor={`void-${r.entry_id}`} className={styles.voidLabel}>Remove from payroll</label>
                        <input id={`void-${r.entry_id}`} name="reason" required minLength={3} className="input" placeholder="Reason" />
                        <button className="button quiet" type="submit">Remove shift</button>
                      </form>
                    </div>
                  )}
                </details>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section aria-labelledby="add" className={styles.add}>
        <h2 id="add" className={styles.h2}>Add a missed shift</h2>
        <form action={addShift} className={styles.form}>
          <input type="hidden" name="week" value={week} />
          <div className={`field ${styles.wide}`}>
            <label htmlFor="add-employee">Employee</label>
            <select id="add-employee" name="employee_id" required className="select">
              {(employees ?? []).map((e) => (
                <option key={e.id} value={e.id}>{e.display_name}</option>
              ))}
            </select>
          </div>
          <div className="field">
            <label htmlFor="add-in">Clock in</label>
            <input id="add-in" name="clock_in" type="datetime-local" required className="input" />
          </div>
          <div className="field">
            <label htmlFor="add-out">Clock out</label>
            <input id="add-out" name="clock_out" type="datetime-local" required className="input" />
          </div>
          <div className={`field ${styles.wide}`}>
            <label htmlFor="add-why">Reason</label>
            <input id="add-why" name="reason" required minLength={3} className="input" placeholder="e.g. Phone died, paper timesheet" />
          </div>
          <button className="button" type="submit">Add shift</button>
        </form>
      </section>
    </div>
  );
}
