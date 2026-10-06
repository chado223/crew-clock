import type { Metadata } from "next";
import { currentPortalAccount, portalDate } from "@/lib/portal";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../portal.module.css";

export const metadata: Metadata = { title: "Service history" };
export const dynamic = "force-dynamic";

type V = { visit_id: string; visit_date: string; service: string; address: string | null; status: string };
type P = { photo_id: string; visit_id: string; kind: string; caption: string | null; storage_path: string };

function addDays(ymd: string, n: number) {
  const d = new Date(`${ymd}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

export default async function History() {
  const { account } = await currentPortalAccount();
  const tz = account.company_timezone;
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: tz }).format(new Date());
  const supabase = await supabaseServer();
  const [{ data: visits }, { data: photos }] = await Promise.all([
    supabase.rpc("portal_visits", { p_client_id: account.client_id, p_from: addDays(today, -365), p_to: today }),
    supabase.rpc("portal_visit_photos", { p_client_id: account.client_id }),
  ]);
  const done = ((visits ?? []) as V[]).filter((v) => v.status === "completed" || v.status === "not_serviced");
  const pics = (photos ?? []) as P[];
  // Short-lived links, issued with the customer's own session: storage re-checks access for each file.
  const { data: signed } = pics.length
    ? await supabase.storage.from("visit-photos").createSignedUrls(pics.map((p) => p.storage_path), 3600)
    : { data: [] as { path: string | null; signedUrl: string }[] };
  const url = (path: string): string | undefined => signed?.find((s) => s.path === path)?.signedUrl ?? undefined;

  return (
    <section className={styles.section}>
      <h1>Service history</h1>
      {done.length === 0 ? (
        <p className={styles.empty}>No visits in the last year yet.</p>
      ) : (
        <ul className={styles.list}>
          {done.map((v) => {
            const mine = pics.filter((p) => p.visit_id === v.visit_id);
            return (
              <li key={v.visit_id} className={styles.historyItem}>
                <div className={styles.cardRow}>
                  <span><strong>{portalDate(v.visit_date, tz, { weekday: "short", month: "short", day: "numeric", year: "numeric" })}</strong> {v.service}</span>
                  <span className={`${styles.pill} ${styles[`pill_${v.status}`] ?? ""}`}>{v.status === "completed" ? "Done" : "Not serviced"}</span>
                </div>
                {mine.length > 0 && (
                  <div className={styles.photos}>
                    {mine.map((p) => {
                      const u = url(p.storage_path);
                      return u ? (
                        <a key={p.photo_id} href={u} target="_blank" rel="noreferrer" className={styles.photo}>
                          {/* eslint-disable-next-line @next/next/no-img-element */}
                          <img src={u} alt={p.caption ?? `${p.kind} photo`} loading="lazy" />
                          <span>{p.kind === "before" ? "Before" : p.kind === "after" ? "After" : ""}{p.caption ? ` · ${p.caption}` : ""}</span>
                        </a>
                      ) : null;
                    })}
                  </div>
                )}
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
