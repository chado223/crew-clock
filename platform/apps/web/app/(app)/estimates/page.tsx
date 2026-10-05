import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";
import { formatMoney, friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../money.module.css";

export const metadata: Metadata = { title: "Estimates" };
export const dynamic = "force-dynamic";

const FILTERS = [
  ["open", "Open"],
  ["approved", "Approved"],
  ["closed", "Won and lost"],
] as const;

async function newEstimate(formData: FormData) {
  "use server";
  const [clientId, propertyId] = String(formData.get("target") ?? "").split("|");
  const { data, error } = await (await supabaseServer()).rpc("create_estimate", {
    p_client_id: clientId!,
    p_property_id: propertyId || (null as unknown as string),
  });
  if (error || !data) redirect(`/estimates?error=${encodeURIComponent(friendlyError(error))}`);
  redirect(`/estimates/${(data as { id: string }).id}`);
}

export default async function EstimatesPage({ searchParams }: { searchParams: Promise<{ show?: string; error?: string }> }) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const show = FILTERS.some(([f]) => f === sp.show) ? sp.show! : "open";
  const supabase = await supabaseServer();

  let q = supabase
    .from("estimates")
    .select("id, number, status, subtotal, valid_until, created_at, clients(name), properties(address_line1)")
    .eq("tenant_id", company.tenant_id)
    .order("created_at", { ascending: false })
    .limit(200);
  q = show === "open" ? q.in("status", ["draft", "sent"]) : show === "approved" ? q.eq("status", "approved") : q.in("status", ["converted", "declined", "expired"]);
  const [{ data: estimates, error }, { data: targets }] = await Promise.all([
    q,
    supabase.from("properties").select("id, address_line1, clients!inner(id, name, status)").eq("tenant_id", company.tenant_id).order("address_line1"),
  ]);

  const pipeline = (estimates ?? []).reduce((n, e) => n + Number(e.subtotal), 0);

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <div>
          <h1>Estimates</h1>
          {show === "open" && pipeline > 0 && <p className={styles.sub}><span className="figure">{formatMoney(pipeline, false)}</span> in open quotes.</p>}
        </div>
        <nav className={styles.tabs} aria-label="Filter">
          {FILTERS.map(([f, label]) => (
            <Link key={f} href={`/estimates?show=${f}`} aria-current={f === show ? "page" : undefined} className={styles.tab}>{label}</Link>
          ))}
        </nav>
      </header>

      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}
      {error && <p className="error-text">{friendlyError(error)}</p>}

      <section className={styles.panel} aria-labelledby="new">
        <h2 id="new">New estimate</h2>
        {(targets ?? []).length === 0 ? (
          <p className={styles.empty}>Add a customer or lead with a service address first. <Link href="/clients?status=lead">Add a lead</Link>.</p>
        ) : (
          <form action={newEstimate} className={styles.form}>
            <div className="field">
              <label htmlFor="target">For</label>
              <select id="target" name="target" required className="select" defaultValue="">
                <option value="" disabled>Choose a customer and property</option>
                {(targets ?? []).map((p) => {
                  const c = p.clients as unknown as { id: string; name: string; status: string };
                  return (
                    <option key={p.id} value={`${c.id}|${p.id}`}>
                      {c.name}, {p.address_line1}{c.status === "lead" ? " (lead)" : ""}
                    </option>
                  );
                })}
              </select>
            </div>
            <button className="button" type="submit">Start estimate</button>
          </form>
        )}
      </section>

      {(estimates ?? []).length === 0 ? (
        <p className={styles.empty}>No estimates here.</p>
      ) : (
        <ul className={styles.list}>
          {(estimates ?? []).map((e) => (
            <li key={e.id}>
              <Link href={`/estimates/${e.id}`} className={styles.row}>
                <span className="figure">{e.number}</span>
                <span>
                  {(e.clients as unknown as { name: string } | null)?.name}
                  <span className={styles.sub}> {(e.properties as unknown as { address_line1: string } | null)?.address_line1}</span>
                </span>
                <span className={`${styles.status} ${styles[`status_${e.status}`] ?? ""}`}>{e.status}</span>
                <span className={styles.num}>{formatMoney(e.subtotal)}</span>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
