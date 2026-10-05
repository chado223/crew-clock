import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "./clients.module.css";

export const metadata: Metadata = { title: "Customers" };
export const dynamic = "force-dynamic";

const STATUS_FILTERS = [
  ["active", "Customers"],
  ["lead", "Leads"],
  ["inactive", "Inactive"],
  ["lost", "Lost"],
] as const;

async function createClient(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const supabase = await supabaseServer();
  const name = String(formData.get("name") ?? "").trim();
  const status = String(formData.get("status") ?? "active");
  const address = String(formData.get("address") ?? "").trim();
  if (!name) redirect(`/clients?status=${status}&error=${encodeURIComponent("Enter the customer's name.")}`);

  const { data, error } = await supabase
    .from("clients")
    .insert({
      tenant_id: company.tenant_id,
      name,
      status,
      kind: String(formData.get("kind") ?? "residential"),
      email: String(formData.get("email") ?? "").trim() || null,
      phone: String(formData.get("phone") ?? "").trim() || null,
      lead_source: String(formData.get("lead_source") ?? "").trim() || null,
    })
    .select("id")
    .single();
  if (error || !data) redirect(`/clients?status=${status}&error=${encodeURIComponent(friendlyError(error))}`);

  if (address) {
    await supabase.from("properties").insert({ tenant_id: company.tenant_id, client_id: data.id, address_line1: address });
  }
  redirect(`/clients/${data.id}`);
}

export default async function ClientsPage({
  searchParams,
}: {
  searchParams: Promise<{ status?: string; q?: string; error?: string }>;
}) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const status = STATUS_FILTERS.some(([s]) => s === sp.status) ? sp.status! : "active";
  const q = (sp.q ?? "").trim();

  const supabase = await supabaseServer();
  let query = supabase
    .from("clients")
    .select("id, name, company_name, kind, phone, email, tags, properties(address_line1)")
    .eq("tenant_id", company.tenant_id)
    .eq("status", status)
    .order("name")
    .limit(200);
  if (q) query = query.ilike("name", `%${q.replace(/[%_\\]/g, (c) => `\\${c}`)}%`);
  const { data: clients, error } = await query;

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <h1>Customers</h1>
        <nav className={styles.tabs} aria-label="Filter">
          {STATUS_FILTERS.map(([s, label]) => (
            <Link key={s} href={`/clients?status=${s}`} aria-current={s === status ? "page" : undefined} className={styles.tab}>
              {label}
            </Link>
          ))}
        </nav>
        <form className={styles.search} role="search">
          <input type="hidden" name="status" value={status} />
          <label htmlFor="q" className={styles.srOnly}>Search by name</label>
          <input id="q" name="q" defaultValue={q} placeholder="Search by name" className="input" />
        </form>
      </header>

      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}
      {error && <p className="error-text">{friendlyError(error)}</p>}

      {(clients ?? []).length === 0 ? (
        <p className={styles.empty}>
          {q ? `No ${status === "lead" ? "leads" : "customers"} match "${q}".` : status === "lead" ? "No open leads. Add one below when someone asks for a quote." : "No customers here yet. Add your first one below."}
        </p>
      ) : (
        <ul className={styles.list}>
          {(clients ?? []).map((c) => {
            const props = (c.properties ?? []) as { address_line1: string }[];
            return (
              <li key={c.id}>
                <Link href={`/clients/${c.id}`} className={styles.row}>
                  <span className={styles.name}>{c.name}</span>
                  <span className={styles.meta}>
                    {props[0]?.address_line1 ?? "No property yet"}
                    {props.length > 1 ? ` and ${props.length - 1} more` : ""}
                  </span>
                  <span className={styles.meta}>{c.phone ?? c.email ?? ""}</span>
                </Link>
              </li>
            );
          })}
        </ul>
      )}

      <section aria-labelledby="new" className={styles.newClient}>
        <h2 id="new">Add a customer or lead</h2>
        <form action={createClient} className={styles.form}>
          <div className="field">
            <label htmlFor="name">Name</label>
            <input id="name" name="name" required className="input" maxLength={200} />
          </div>
          <div className="field">
            <label htmlFor="status">Add as</label>
            <select id="status" name="status" className="select" defaultValue={status === "lead" ? "lead" : "active"}>
              <option value="active">Customer</option>
              <option value="lead">Lead (not a customer yet)</option>
            </select>
          </div>
          <div className="field">
            <label htmlFor="phone">Phone</label>
            <input id="phone" name="phone" type="tel" className="input" />
          </div>
          <div className="field">
            <label htmlFor="email">Email</label>
            <input id="email" name="email" type="email" className="input" />
          </div>
          <div className={`field ${styles.wide}`}>
            <label htmlFor="address">Service address</label>
            <input id="address" name="address" className="input" placeholder="Street address" />
            <p className="hint">You can add more properties on the customer's page.</p>
          </div>
          <div className="field">
            <label htmlFor="kind">Type</label>
            <select id="kind" name="kind" className="select" defaultValue="residential">
              <option value="residential">Residential</option>
              <option value="commercial">Commercial</option>
            </select>
          </div>
          <div className="field">
            <label htmlFor="lead_source">How they found you</label>
            <input id="lead_source" name="lead_source" className="input" placeholder="e.g. Referral, yard sign" />
          </div>
          <button className="button" type="submit">Add</button>
        </form>
      </section>
    </div>
  );
}
