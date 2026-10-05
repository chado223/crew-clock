import type { Metadata } from "next";
import Link from "next/link";
import { notFound, redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import { formatMoney, friendlyError } from "@crew/shared";
import { currentPortalAccount, portalDate } from "@/lib/portal";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../../portal.module.css";

export const metadata: Metadata = { title: "Estimate" };
export const dynamic = "force-dynamic";

async function respond(formData: FormData) {
  "use server";
  const id = String(formData.get("estimate_id"));
  const { error } = await (await supabaseServer()).rpc("portal_respond_estimate", {
    p_estimate_id: id,
    p_decision: String(formData.get("decision")),
    p_note: String(formData.get("note") ?? "") || (null as unknown as string),
  });
  revalidatePath(`/portal/estimates/${id}`);
  redirect(`/portal/estimates/${id}${error ? `?error=${encodeURIComponent(friendlyError(error))}` : "?done=1"}`);
}

type Line = { description: string; quantity: number; unit_price: number; amount: number; repeat_every_weeks: number | null };

export default async function PortalEstimate({ params, searchParams }: { params: Promise<{ id: string }>; searchParams: Promise<{ error?: string; done?: string }> }) {
  const { account } = await currentPortalAccount();
  const { id } = await params;
  const sp = await searchParams;
  const { data } = await (await supabaseServer()).rpc("portal_estimates", { p_client_id: account.client_id });
  const est = ((data ?? []) as { estimate_id: string; number: string; status: string; total: number; valid_until: string | null; address: string | null; lines: Line[] }[])
    .find((e) => e.estimate_id === id);
  if (!est) notFound();

  return (
    <>
      <Link href="/portal/estimates">All estimates</Link>
      <article className={styles.card}>
        <div className={styles.cardRow}>
          <h1 className={styles.big}>Estimate {est.number}</h1>
          <span className="figure">{formatMoney(est.total)}</span>
        </div>
        {est.address && <p className={styles.muted}>For {est.address}</p>}
        {est.valid_until && <p className={styles.muted}>Good through {portalDate(est.valid_until, account.company_timezone, { month: "long", day: "numeric", year: "numeric" })}</p>}
        <table className={styles.lines}>
          <thead><tr><th scope="col">Work</th><th scope="col" className={styles.right}>Price</th></tr></thead>
          <tbody>
            {est.lines.map((l, i) => (
              <tr key={i}>
                <td>
                  {l.description}
                  <span className={styles.muted}> {l.repeat_every_weeks ? (l.repeat_every_weeks === 1 ? "each week" : `every ${l.repeat_every_weeks} weeks`) : "one time"}</span>
                </td>
                <td className={`${styles.right} figure`}>{formatMoney(l.amount)}{l.repeat_every_weeks ? " per visit" : ""}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </article>

      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}
      {sp.done && <p className="notice" role="status">Thanks. {account.company_name} has your answer.</p>}

      {est.status === "sent" ? (
        <form action={respond} className={styles.form}>
          <input type="hidden" name="estimate_id" value={est.estimate_id} />
          <div className="field">
            <label htmlFor="note">Anything we should know? (optional)</label>
            <textarea id="note" name="note" maxLength={2000} rows={3} className={`input ${styles.textarea}`} placeholder="e.g. Please start after the 15th" />
          </div>
          <div className={styles.choices}>
            <button className="button" type="submit" name="decision" value="approved">Approve estimate</button>
            <button className="button quiet" type="submit" name="decision" value="declined">Decline</button>
          </div>
          <p className={styles.muted}>Approving tells {account.company_name} to schedule this work at the prices shown.</p>
        </form>
      ) : (
        <p className={styles.muted}>
          {est.status === "approved" || est.status === "converted" ? "You approved this estimate." : est.status === "declined" ? "You declined this estimate." : "This estimate is no longer open."}
        </p>
      )}
    </>
  );
}
