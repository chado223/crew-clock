import type { Metadata } from "next";
import Link from "next/link";
import { formatMoney } from "@crew/shared";
import { currentPortalAccount, portalDate } from "@/lib/portal";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../portal.module.css";

export const metadata: Metadata = { title: "Estimates" };
export const dynamic = "force-dynamic";

const LABEL: Record<string, string> = { sent: "Waiting for you", approved: "Approved", converted: "Approved", declined: "Declined", expired: "Expired" };

export default async function PortalEstimates() {
  const { account } = await currentPortalAccount();
  const { data } = await (await supabaseServer()).rpc("portal_estimates", { p_client_id: account.client_id });
  const estimates = (data ?? []) as { estimate_id: string; number: string; status: string; total: number; sent_at: string | null; address: string | null }[];

  return (
    <section className={styles.section}>
      <h1>Estimates</h1>
      {estimates.length === 0 ? (
        <p className={styles.empty}>No estimates yet.</p>
      ) : (
        <ul className={styles.list}>
          {estimates.map((e) => (
            <li key={e.estimate_id}>
              <span>
                <Link href={`/portal/estimates/${e.estimate_id}`}><strong>{e.number}</strong></Link>{" "}
                <span className={styles.muted}>{e.address}{e.sent_at ? `, ${portalDate(e.sent_at, account.company_timezone, { month: "short", day: "numeric" })}` : ""}</span>
              </span>
              <span>
                <span className={`${styles.pill} ${styles[`pill_${e.status}`] ?? ""}`}>{LABEL[e.status] ?? e.status}</span>{" "}
                <span className="figure">{formatMoney(e.total)}</span>
              </span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
