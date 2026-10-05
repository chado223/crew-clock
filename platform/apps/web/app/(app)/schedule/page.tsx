import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import { friendlyError, weekStart } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "./schedule.module.css";

export const metadata: Metadata = { title: "Schedule" };
export const dynamic = "force-dynamic";

type Visit = {
  visit_id: string;
  scheduled_date: string;
  status: "scheduled" | "in_progress" | "completed" | "skipped" | "canceled";
  status_reason: string | null;
  job_title: string;
  job_kind: string;
  client_id: string | null;
  client_name: string | null;
  address: string | null;
  crew_id: string | null;
  crew_name: string | null;
  assignees: string[];
  est_minutes: number | null;
  price: number | null;
};

function addDays(ymd: string, n: number) {
  const d = new Date(`${ymd}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

function done(week: string, params: Record<string, string>): never {
  redirect(`/schedule?${new URLSearchParams({ week, ...params }).toString()}`);
}

async function fillWeek(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const week = String(formData.get("week"));
  const { data, error } = await (await supabaseServer()).rpc("generate_visits", {
    p_tenant_id: company.tenant_id,
    p_from: week,
    p_to: addDays(week, 13),
  });
  revalidatePath("/schedule");
  if (error) done(week, { error: friendlyError(error) });
  done(week, { saved: data ? `Added ${data} visit${data === 1 ? "" : "s"} from recurring jobs.` : "Already up to date." });
}

async function moveVisit(formData: FormData) {
  "use server";
  const week = String(formData.get("week"));
  const crew = String(formData.get("crew_id") ?? "");
  const { error } = await (await supabaseServer()).rpc("reschedule_visit", {
    p_visit_id: String(formData.get("visit_id")),
    p_date: String(formData.get("date")),
    p_crew_id: crew || (null as unknown as string),
    p_reason: String(formData.get("reason") ?? "") || (null as unknown as string),
  });
  revalidatePath("/schedule");
  done(week, error ? { error: friendlyError(error) } : { saved: "Visit moved." });
}

async function skipVisit(formData: FormData) {
  "use server";
  const week = String(formData.get("week"));
  const { error } = await (await supabaseServer()).rpc("skip_visit", {
    p_visit_id: String(formData.get("visit_id")),
    p_reason: String(formData.get("reason") ?? ""),
  });
  revalidatePath("/schedule");
  done(week, error ? { error: friendlyError(error) } : { saved: "Visit skipped. It's noted in the customer's history." });
}

const STATUS_TEXT: Record<Visit["status"], string> = {
  scheduled: "",
  in_progress: "In progress",
  completed: "Done",
  skipped: "Skipped",
  canceled: "Canceled",
};

export default async function SchedulePage({
  searchParams,
}: {
  searchParams: Promise<{ week?: string; error?: string; saved?: string }>;
}) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const thisWeek = weekStart(new Date(), company.timezone);
  const week = sp.week && /^\d{4}-\d{2}-\d{2}$/.test(sp.week) ? weekStart(new Date(`${sp.week}T12:00:00Z`), "UTC") : thisWeek;
  const days = Array.from({ length: 7 }, (_, i) => addDays(week, i));
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date());

  const supabase = await supabaseServer();
  const [{ data, error }, { data: crews }] = await Promise.all([
    supabase.rpc("schedule", { p_tenant_id: company.tenant_id, p_from: days[0]!, p_to: days[6]! }),
    supabase.from("crews").select("id, name").eq("tenant_id", company.tenant_id).eq("active", true).order("name"),
  ]);
  const visits = (data ?? []) as Visit[];
  const rows: { id: string | null; name: string }[] = [
    ...(crews ?? []).map((c) => ({ id: c.id as string, name: c.name as string })),
    { id: null, name: "No crew yet" },
  ].filter((r) => r.id !== null || visits.some((v) => v.crew_id === null));

  const dayLabel = (ymd: string) =>
    new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short", day: "numeric" }).format(new Date(`${ymd}T12:00:00Z`));
  const minutes = (list: Visit[]) => list.filter((v) => v.status !== "skipped" && v.status !== "canceled").reduce((n, v) => n + (v.est_minutes ?? 0), 0);

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <h1>Schedule</h1>
        <div className={styles.tools}>
          <Link href={`/schedule?week=${addDays(week, -7)}`} className="button quiet">Previous week</Link>
          {week !== thisWeek && <Link href="/schedule" className="button quiet">This week</Link>}
          <Link href={`/schedule?week=${addDays(week, 7)}`} className="button quiet">Next week</Link>
          <form action={fillWeek}>
            <input type="hidden" name="week" value={week} />
            <button className="button" type="submit">Fill from recurring jobs</button>
          </form>
        </div>
      </header>

      {sp.saved && <p className="notice" role="status">{sp.saved}</p>}
      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}
      {error && <p className="error-text">{friendlyError(error)}</p>}

      {visits.length === 0 && (
        <p className={styles.empty}>
          Nothing scheduled this week. Add recurring jobs on a customer's page, then use <strong>Fill from recurring jobs</strong>.
        </p>
      )}

      <div className={styles.board} role="table" aria-label={`Schedule for the week of ${dayLabel(week)}`}>
        <div className={styles.row} role="row">
          <div className={styles.corner} role="columnheader">Crew</div>
          {days.map((d) => (
            <div key={d} role="columnheader" className={`${styles.dayHead} ${d === today ? styles.today : ""}`}>
              {dayLabel(d)}
            </div>
          ))}
        </div>

        {rows.map((row) => (
          <div key={row.id ?? "none"} className={styles.row} role="row">
            <div className={styles.crewName} role="rowheader">{row.name}</div>
            {days.map((d) => {
              const cell = visits.filter((v) => v.crew_id === row.id && v.scheduled_date === d);
              const load = minutes(cell);
              return (
                <div key={d} role="cell" className={`${styles.cell} ${d === today ? styles.todayCell : ""}`}>
                  {load > 0 && <p className={`${styles.load} figure`}>{Math.floor(load / 60)}h {load % 60}m</p>}
                  {cell.map((v) => (
                    <details key={v.visit_id} className={`${styles.card} ${styles[v.status]}`}>
                      <summary>
                        <span className={styles.title}>{v.job_title}</span>
                        <span className={styles.client}>{v.client_name}</span>
                        {STATUS_TEXT[v.status] && <span className={styles.badge}>{STATUS_TEXT[v.status]}</span>}
                      </summary>
                      <div className={styles.cardBody}>
                        {v.address && <p className={styles.address}>{v.address}</p>}
                        {v.assignees.length > 0 && <p className={styles.small}>Also on it: {v.assignees.join(", ")}</p>}
                        {v.status_reason && <p className={styles.small}>{v.status_reason}</p>}
                        {v.client_id && <Link href={`/clients/${v.client_id}`} className={styles.small}>Customer record</Link>}
                        {v.status === "scheduled" && (
                          <>
                            <form action={moveVisit} className={styles.cardForm}>
                              <input type="hidden" name="visit_id" value={v.visit_id} />
                              <input type="hidden" name="week" value={week} />
                              <label htmlFor={`d-${v.visit_id}`}>Move to</label>
                              <input id={`d-${v.visit_id}`} type="date" name="date" defaultValue={v.scheduled_date} required className="input" />
                              <label htmlFor={`c-${v.visit_id}`}>Crew</label>
                              <select id={`c-${v.visit_id}`} name="crew_id" defaultValue={v.crew_id ?? ""} className="select">
                                <option value="">Keep current</option>
                                {(crews ?? []).map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
                              </select>
                              <label htmlFor={`r-${v.visit_id}`}>Reason (optional)</label>
                              <input id={`r-${v.visit_id}`} name="reason" className="input" placeholder="e.g. Rain" />
                              <button className="button" type="submit">Move visit</button>
                            </form>
                            <form action={skipVisit} className={styles.cardForm}>
                              <input type="hidden" name="visit_id" value={v.visit_id} />
                              <input type="hidden" name="week" value={week} />
                              <label htmlFor={`s-${v.visit_id}`}>Skip this visit</label>
                              <input id={`s-${v.visit_id}`} name="reason" required minLength={3} className="input" placeholder="Reason" />
                              <button className="button quiet" type="submit">Skip visit</button>
                            </form>
                          </>
                        )}
                      </div>
                    </details>
                  ))}
                </div>
              );
            })}
          </div>
        ))}
      </div>
    </div>
  );
}
