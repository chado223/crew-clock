import type { Metadata } from "next";
import { formatMoney } from "@crew/shared";
import { currentPortalAccount, portalDate } from "@/lib/portal";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../portal.module.css";

export const metadata: Metadata = { title: "Invoices" };
export const dynamic = "force-dynamic";

const LABEL: Record<string, string> = { sent: "Due", partial: "Partly paid", overdue: "Past due", paid: "Paid" };
const METHOD: Record<string, string> = { card: "Card", cash: "Cash", check: "Check", ach: "Bank transfer", other: "Payment" };

type Inv = {
  invoice_id: string;
  number: string | null;
  status: string;
  issued_at: string;
  due_at: string | null;
  subtotal: number | null;
  tax_amount: number;
  total: number;
  amount_paid: number;
  balance: number;
  lines: { description: string; quantity: number; amount: number }[];
  payments: { amount: number; method: string; received_on: string }[];
};

export default async function PortalInvoices() {
  const { account } = await currentPortalAccount();
  const tz = account.company_timezone;
  const { data } = await (await supabaseServer()).rpc("portal_invoices", { p_client_id: account.client_id });
  const invoices = (data ?? []) as Inv[];
  const due = invoices.filter((i) => Number(i.balance) > 0).reduce((n, i) => n + Number(i.balance), 0);

  return (
    <section className={styles.section}>
      <h1>Invoices</h1>
      {due > 0 && <p className={styles.lead}>Balance due: <strong>{formatMoney(due)}</strong></p>}
      {due > 0 && (
        <p className={styles.muted}>
          Online payment is coming soon. For now, pay {account.company_name} the way you usually do; payments show here once they're recorded.
        </p>
      )}
      {invoices.length === 0 ? (
        <p className={styles.empty}>No invoices yet.</p>
      ) : (
        invoices.map((i) => (
          <details key={i.invoice_id} className={styles.card} open={Number(i.balance) > 0}>
            <summary className={styles.cardRow}>
              <span>
                <strong>{i.number ?? "Invoice"}</strong>{" "}
                <span className={styles.muted}>{portalDate(i.issued_at, tz, { month: "short", day: "numeric", year: "numeric" })}</span>
              </span>
              <span>
                <span className={`${styles.pill} ${styles[`pill_${i.status}`] ?? ""}`}>{LABEL[i.status] ?? i.status}</span>{" "}
                <span className="figure">{formatMoney(i.status === "paid" ? i.total : i.balance)}</span>
              </span>
            </summary>
            {i.lines.length > 0 && (
              <table className={styles.lines}>
                <tbody>
                  {i.lines.map((l, n) => (
                    <tr key={n}><td>{l.description}</td><td className={`${styles.right} figure`}>{formatMoney(l.amount)}</td></tr>
                  ))}
                  {Number(i.tax_amount) > 0 && <tr><td>Tax</td><td className={`${styles.right} figure`}>{formatMoney(i.tax_amount)}</td></tr>}
                  <tr><td><strong>Total</strong></td><td className={`${styles.right} figure`}><strong>{formatMoney(i.total)}</strong></td></tr>
                </tbody>
              </table>
            )}
            {i.payments.map((p, n) => (
              <p key={n} className={styles.muted}>
                {METHOD[p.method] ?? "Payment"} of {formatMoney(p.amount)} received {portalDate(p.received_on, tz, { month: "short", day: "numeric" })}
              </p>
            ))}
            {i.due_at && Number(i.balance) > 0 && <p className={styles.muted}>Due {portalDate(i.due_at, tz, { month: "long", day: "numeric" })}</p>}
          </details>
        ))
      )}
    </section>
  );
}
