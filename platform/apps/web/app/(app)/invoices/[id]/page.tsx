import type { Metadata } from "next";
import Link from "next/link";
import { notFound, redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import { formatMoney, friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import { describeMessage, MESSAGE_COLUMNS, type MessageRow } from "@/lib/messages";
import { companyProfile, Letterhead } from "@/components/letterhead";
import { PrintButton } from "@/components/print-button";
import styles from "../../money.module.css";

export const metadata: Metadata = { title: "Invoice" };
export const dynamic = "force-dynamic";

function back(id: string, error?: unknown): never {
  revalidatePath(`/invoices/${id}`);
  redirect(`/invoices/${id}${error ? `?error=${encodeURIComponent(friendlyError(error))}` : ""}`);
}

async function markSent(formData: FormData) {
  "use server";
  const id = String(formData.get("id"));
  const { error } = await (await supabaseServer()).rpc("mark_invoice_sent", { p_invoice_id: id });
  back(id, error);
}

async function send(formData: FormData) {
  "use server";
  const id = String(formData.get("id"));
  const { error } = await (await supabaseServer()).rpc("send_invoice", { p_invoice_id: id, p_channel: String(formData.get("channel") ?? "email") });
  if (error) back(id, error);
  redirect(`/invoices/${id}?sent=1`);
}

async function recordPayment(formData: FormData) {
  "use server";
  const id = String(formData.get("id"));
  const { error } = await (await supabaseServer()).rpc("record_payment", {
    p_invoice_id: id,
    p_amount: Number(formData.get("amount")),
    p_method: String(formData.get("method")),
    p_received_on: String(formData.get("received_on")),
    p_reference: String(formData.get("reference") ?? "") || (null as unknown as string),
  });
  back(id, error);
}

async function removeLine(formData: FormData) {
  "use server";
  const id = String(formData.get("id"));
  const { error } = await (await supabaseServer()).from("invoice_lines").delete().eq("id", String(formData.get("line_id")));
  back(id, error);
}

async function addCharge(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const id = String(formData.get("id"));
  const qty = Number(formData.get("quantity") || 1);
  const price = Number(formData.get("unit_price"));
  const description = String(formData.get("description") ?? "").trim();
  if (!description || !Number.isFinite(price) || !Number.isFinite(qty) || qty <= 0) back(id, "Enter a description, quantity and price.");
  const { error } = await (await supabaseServer()).from("invoice_lines").insert({
    tenant_id: company.tenant_id, invoice_id: id, description, quantity: qty, unit_price: price, sort_order: 1000,
  });
  back(id, error);
}

async function voidInvoice(formData: FormData) {
  "use server";
  const id = String(formData.get("id"));
  const { error } = await (await supabaseServer()).rpc("void_invoice", { p_invoice_id: id, p_reason: String(formData.get("reason") ?? "") } as never);
  back(id, error);
}

async function voidPayment(formData: FormData) {
  "use server";
  const id = String(formData.get("id"));
  const { error } = await (await supabaseServer()).rpc("void_payment", { p_payment_id: String(formData.get("payment_id")), p_reason: String(formData.get("reason") ?? "") } as never);
  back(id, error);
}

async function setTax(formData: FormData) {
  "use server";
  const id = String(formData.get("id"));
  const { error } = await (await supabaseServer())
    .from("invoices")
    .update({ tax_rate: Number(formData.get("tax_percent")) / 100 })
    .eq("id", id);
  back(id, error);
}

export default async function InvoicePage({ params, searchParams }: { params: Promise<{ id: string }>; searchParams: Promise<{ error?: string; sent?: string }> }) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const { id } = await params;
  const { error, sent } = await searchParams;
  if (!/^[0-9a-f-]{36}$/i.test(id)) notFound();

  const supabase = await supabaseServer();
  const [{ data: inv }, { data: lines }, { data: payments }] = await Promise.all([
    supabase.from("invoices").select("*, clients(id, name, email, phone, address)").eq("id", id).maybeSingle(),
    supabase.from("invoice_lines").select("id, description, quantity, unit_price, amount").eq("invoice_id", id).order("sort_order"),
    supabase.from("payments").select("id, amount, method, reference, received_on").eq("invoice_id", id).is("voided_at", null).order("received_on"),
  ]);
  if (!inv) notFound();
  const client = inv.clients as unknown as { id: string; name: string; email: string | null; phone: string | null; address: string | null } | null;
  const balance = Number(inv.total) - Number(inv.amount_paid);
  const date = (iso: string | null) =>
    iso ? new Intl.DateTimeFormat("en-US", { timeZone: company.timezone, month: "long", day: "numeric", year: "numeric" }).format(new Date(iso)) : "";
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date());

  const { data: lastMsg } = await (await supabaseServer()).from("messages").select(MESSAGE_COLUMNS)
    .eq("subject_type", "invoice").eq("subject_id", id).order("created_at", { ascending: false }).limit(1).maybeSingle();
  const lastMessage = lastMsg as unknown as MessageRow | null;
  const profile = await companyProfile(company.tenant_id);

  return (
    <div className={styles.page}>
      <Link href="/invoices" className="noPrint">All invoices</Link>
      {error && <p className="error-text" role="alert">{error}</p>}
      {lastMessage && (
        <p className={`notice noPrint`} role="status">
          {sent ? "Sent to the outbox. " : "Last message: "}{describeMessage(lastMessage)}{" "}
          <Link href="/messages">Messages</Link>
        </p>
      )}

      <div className="noPrint" style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}><PrintButton /></div>
      <article className={styles.doc}>
        <Letterhead p={profile} />
        {inv.status === "void" && <p className="error-text">VOID{inv.void_reason ? `: ${inv.void_reason}` : ""}</p>}
        <div className={styles.docHead}>
          <div>
            <p className={styles.docNumber}>{inv.number ?? "Invoice"}</p>
            <p className={styles.sub}>Issued {date(inv.issued_at)}{inv.due_at ? `, due ${date(inv.due_at)}` : ""}</p>
          </div>
          <div>
            <p><strong>{company.name}</strong></p>
            <p className={styles.sub}>Bill to: {client?.name}</p>
            {client?.address && <p className={styles.sub}>{client.address}</p>}
          </div>
        </div>

        {(lines ?? []).length === 0 ? (
          <p className={styles.empty}>This invoice was created before itemized billing. Total: {formatMoney(inv.total)}.</p>
        ) : (
          <table className={styles.lines}>
            <thead>
              <tr><th scope="col">Description</th><th scope="col" className={styles.right}>Amount</th>{inv.status === "draft" && <th className="noPrint" />}</tr>
            </thead>
            <tbody>
              {(lines ?? []).map((l) => (
                <tr key={l.id}>
                  <td>{l.description}{Number(l.quantity) !== 1 ? ` × ${l.quantity}` : ""}</td>
                  <td className={`${styles.right} figure`}>{formatMoney(l.amount)}</td>
                  {inv.status === "draft" && (
                    <td className={`${styles.right} noPrint`}>
                      <form action={removeLine}>
                        <input type="hidden" name="id" value={inv.id} />
                        <input type="hidden" name="line_id" value={l.id} />
                        <button type="submit" className={styles.remove}>Remove</button>
                      </form>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        )}

        <dl className={styles.totals}>
          {inv.subtotal != null && (<><dt>Subtotal</dt><dd>{formatMoney(inv.subtotal)}</dd></>)}
          {Number(inv.tax_amount) > 0 && (<><dt>Tax ({(Number(inv.tax_rate) * 100).toFixed(3).replace(/\.?0+$/, "")}%)</dt><dd>{formatMoney(inv.tax_amount)}</dd></>)}
          <dt className={styles.grand}>Total</dt><dd className={styles.grand}>{formatMoney(inv.total)}</dd>
          {Number(inv.amount_paid) > 0 && (<><dt>Paid</dt><dd>{formatMoney(inv.amount_paid)}</dd><dt className={styles.grand}>Balance due</dt><dd className={styles.grand}>{formatMoney(balance)}</dd></>)}
        </dl>
      </article>

      <div className={`${styles.actions} noPrint`}>
        <span className={`${styles.status} ${styles[`status_${inv.status}`] ?? ""}`}>{inv.status}</span>
        {inv.status === "draft" && (
          <>
            <form action={setTax} className={styles.inline}>
              <input type="hidden" name="id" value={inv.id} />
              <div className="field"><label htmlFor="tax">Sales tax (%)</label><input id="tax" name="tax_percent" type="number" min={0} max={99} step="0.001" defaultValue={(Number(inv.tax_rate) * 100).toString()} className="input" /></div>
              <button className="button quiet" type="submit">Update tax</button>
            </form>
            <form action={send}>
              <input type="hidden" name="id" value={inv.id} />
              <button className="button" type="submit" name="channel" value="email">Email to customer</button>
            </form>
            <form action={markSent}>
              <input type="hidden" name="id" value={inv.id} />
              <button className="button quiet" type="submit">Mark sent (I sent it myself)</button>
            </form>
          </>
        )}
        {["sent", "partial", "overdue"].includes(inv.status) && (
          <form action={send}>
            <input type="hidden" name="id" value={inv.id} />
            <button className="button quiet" type="submit" name="channel" value="email">Email again</button>
          </form>
        )}
        {client && <Link href={`/clients/${client.id}`} className="button quiet">Customer record</Link>}
      </div>
      {inv.status === "draft" && (
        <p className={`${styles.empty} noPrint`}>Sending locks the lines. Emails follow your message settings (test mode sends only to your test address). You can also print or save as PDF.</p>
      )}

      {["sent", "partial", "overdue"].includes(inv.status) && (
        <section className={`${styles.panel} noPrint`} aria-labelledby="pay">
          <h2 id="pay">Record a payment</h2>
          <form action={recordPayment} className={styles.form}>
            <input type="hidden" name="id" value={inv.id} />
            <div className="field"><label htmlFor="amount">Amount ($)</label><input id="amount" name="amount" type="number" min={0.01} step="0.01" max={balance} defaultValue={balance.toFixed(2)} required className="input" /></div>
            <div className="field">
              <label htmlFor="method">Method</label>
              <select id="method" name="method" className="select" defaultValue="check">
                <option value="check">Check</option>
                <option value="cash">Cash</option>
                <option value="card">Card</option>
                <option value="ach">Bank transfer</option>
                <option value="other">Other</option>
              </select>
            </div>
            <div className="field"><label htmlFor="received_on">Received</label><input id="received_on" name="received_on" type="date" defaultValue={today} required className="input" /></div>
            <div className="field"><label htmlFor="reference">Check or reference no.</label><input id="reference" name="reference" className="input" /></div>
            <button className="button" type="submit">Record payment</button>
          </form>
        </section>
      )}

      {inv.status === "draft" && (
        <section className={`${styles.panel} noPrint`} aria-labelledby="charge">
          <h2 id="charge">Add a charge</h2>
          <form action={addCharge} className={styles.form}>
            <input type="hidden" name="id" value={inv.id} />
            <div className="field"><label htmlFor="desc">Description</label><input id="desc" name="description" required className="input" placeholder="e.g. Mulch, 3 bags" /></div>
            <div className="field"><label htmlFor="qty">Quantity</label><input id="qty" name="quantity" type="number" min={0.01} step="0.01" defaultValue={1} className="input" /></div>
            <div className="field"><label htmlFor="up">Price each ($)</label><input id="up" name="unit_price" type="number" step="0.01" required className="input" /></div>
            <button className="button quiet" type="submit">Add charge</button>
          </form>
        </section>
      )}

      {(payments ?? []).length > 0 && (
        <section className="noPrint" aria-labelledby="payments">
          <h2 id="payments">Payments</h2>
          <ul className={styles.list}>
            {(payments ?? []).map((p) => (
              <li key={p.id} className={styles.row}>
                <span>{date(`${p.received_on}T12:00:00Z`)}</span>
                <span style={{ textTransform: "capitalize" }}>{p.method}{p.reference ? `, ${p.reference}` : ""}</span>
                <span />
                <span className={styles.num}>{formatMoney(p.amount)}</span>
                <form action={voidPayment} className={styles.inline}>
                  <input type="hidden" name="id" value={inv.id} />
                  <input type="hidden" name="payment_id" value={p.id} />
                  <label className="srOnly" htmlFor={`vr-${p.id}`}>Reason</label>
                  <input id={`vr-${p.id}`} name="reason" required className="input" placeholder="Reason to void" />
                  <button className={styles.remove} type="submit">Void payment</button>
                </form>
              </li>
            ))}
          </ul>
        </section>
      )}

      {inv.status !== "void" && (
        <details className="noPrint">
          <summary style={{ cursor: "pointer", color: "var(--error)", fontWeight: 600 }}>Void this invoice</summary>
          <form action={voidInvoice} className={styles.form}>
            <input type="hidden" name="id" value={inv.id} />
            <div className="field"><label htmlFor="void-r">Reason</label><input id="void-r" name="reason" required className="input" placeholder="e.g. Billed the wrong customer" /></div>
            <button className="button quiet" type="submit">Void invoice</button>
          </form>
          <p className="hint">The invoice is kept and marked void. Its visits can be billed again. Void any payments on it first.</p>
        </details>
      )}
    </div>
  );
}
