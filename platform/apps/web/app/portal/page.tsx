import type { Metadata } from "next";
import Link from "next/link";
import { formatMoney } from "@crew/shared";
import { currentPortalAccount, portalDate } from "@/lib/portal";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "./portal.module.css";

export const metadata: Metadata = { title: "Overview" };
export const dynamic = "force-dynamic";

const WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
const VISIT_STATUS: Record<string, string> = { scheduled: "Scheduled", in_progress: "Crew on site", completed: "Done", not_serviced: "Not serviced" };

function addDays(ymd: string, n: number) {
  const d = new Date(`${ymd}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

export default async function PortalHome() {
  const { account } = await currentPortalAccount();
  const supabase = await supabaseServer();
  const tz = account.company_timezone;
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: tz }).format(new Date());
  const id = account.client_id;

  const [{ data: visits }, { data: services }, { data: invoices }, { data: estimates }] = await Promise.all([
    supabase.rpc("portal_visits", { p_client_id: id, p_from: addDays(today, -90), p_to: addDays(today, 60) }),
    supabase.rpc("portal_services", { p_client_id: id }),
    supabase.rpc("portal_invoices", { p_client_id: id }),
    supabase.rpc("portal_estimates", { p_client_id: id }),
  ]);

  type V = { visit_id: string; visit_date: string; service: string; address: string | null; status: string };
  const all = (visits ?? []) as V[];
  const upcoming = all.filter((v) => v.visit_date >= today && v.status !== "completed" && v.status !== "not_serviced").reverse();
  const recent = all.filter((v) => v.status === "completed" || v.status === "not_serviced").slice(0, 8);
  const due = ((invoices ?? []) as { balance: number; status: string }[]).filter((i) => ["sent", "partial", "overdue"].includes(i.status));
  const balance = due.reduce((n, i) => n + Number(i.balance), 0);
  const awaiting = ((estimates ?? []) as { estimate_id: string; number: string; total: number; status: string }[]).filter((e) => e.status === "sent");
  const next = upcoming[0];

  return (
    <>
      <p className={styles.lead}>
        {next ? (
          <>Your next visit is <strong>{portalDate(next.visit_date, tz, { weekday: "long", month: "long", day: "numeric" })}</strong>.</>
        ) : (
          "No visits are scheduled right now."
        )}
        {balance > 0 ? <> You have <strong>{formatMoney(balance)}</strong> due.</> : null}
      </p>

      {awaiting.length > 0 && (
        <section className={styles.section} aria-labelledby="awaiting">
          <h2 id="awaiting">Waiting for your answer</h2>
          <ul className={styles.list}>
            {awaiting.map((e) => (
              <li key={e.estimate_id}>
                <Link href={`/portal/estimates/${e.estimate_id}`}>Estimate {e.number}</Link>
                <span className="figure">{formatMoney(e.total)}</span>
              </li>
            ))}
          </ul>
        </section>
      )}

      {upcoming.length > 0 && (
        <section className={styles.section} aria-labelledby="upcoming">
          <h2 id="upcoming">Coming up</h2>
          <ul className={styles.list}>
            {upcoming.slice(0, 6).map((v) => (
              <li key={v.visit_id}>
                <span><strong>{portalDate(v.visit_date, tz)}</strong> {v.service}</span>
                <span className={styles.muted}>{v.address}</span>
              </li>
            ))}
          </ul>
        </section>
      )}

      {((services ?? []) as unknown[]).length > 0 && (
        <section className={styles.section} aria-labelledby="services">
          <h2 id="services">Your services</h2>
          <ul className={styles.list}>
            {((services ?? []) as { job_id: string; title: string; kind: string; every_weeks: number | null; weekday: number | null; price: number | null; address: string | null }[]).map((s) => (
              <li key={s.job_id}>
                <span>
                  <strong>{s.title}</strong>{" "}
                  <span className={styles.muted}>
                    {s.kind === "recurring" ? `${s.every_weeks === 1 ? "Every" : `Every ${s.every_weeks} weeks on`} ${WEEKDAYS[(s.weekday ?? 1) - 1]}` : "One time"}
                    {s.address ? `, ${s.address}` : ""}
                  </span>
                </span>
                {s.price != null && <span className="figure">{formatMoney(s.price)}{s.kind === "recurring" ? " per visit" : ""}</span>}
              </li>
            ))}
          </ul>
        </section>
      )}

      <section className={styles.section} aria-labelledby="history">
        <h2 id="history">Recent visits</h2>
        {recent.length === 0 ? (
          <p className={styles.empty}>No completed visits yet.</p>
        ) : (
          <ul className={styles.list}>
            {recent.map((v) => (
              <li key={v.visit_id}>
                <span><strong>{portalDate(v.visit_date, tz)}</strong> {v.service}</span>
                <span className={`${styles.pill} ${styles[`pill_${v.status}`] ?? ""}`}>{VISIT_STATUS[v.status] ?? v.status}</span>
              </li>
            ))}
          </ul>
        )}
      </section>

      <p className={styles.muted}>
        <Link href="/portal/history">Full service history and photos</Link>. Need something else done? <Link href="/portal/request">Request service</Link>.
      </p>
    </>
  );
}
