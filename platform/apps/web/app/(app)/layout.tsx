import Link from "next/link";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { currentCompany, COMPANY_COOKIE, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "./shell.module.css";

async function switchCompany(formData: FormData) {
  "use server";
  (await cookies()).set(COMPANY_COOKIE, String(formData.get("tenant") ?? ""), {
    httpOnly: true,
    sameSite: "lax",
    secure: true,
    path: "/",
  });
  redirect("/");
}

async function signOut() {
  "use server";
  const supabase = await supabaseServer();
  await supabase.auth.signOut();
  redirect("/login");
}

export default async function AppLayout({ children }: { children: React.ReactNode }) {
  const { companies, company } = await currentCompany();
  const manager = isManager(company);

  return (
    <div className={styles.shell}>
      <nav className={styles.rail} aria-label="Main">
        <div className={styles.company}>
          {companies.length > 1 ? (
            <form action={switchCompany}>
              <label className={styles.srOnly} htmlFor="tenant">
                Company
              </label>
              <select id="tenant" name="tenant" defaultValue={company.tenant_id} className={styles.companySelect}>
                {companies.map((c) => (
                  <option key={c.tenant_id} value={c.tenant_id}>
                    {c.name}
                  </option>
                ))}
              </select>
              <button type="submit" className={styles.switch}>
                Switch
              </button>
            </form>
          ) : (
            <p className={styles.companyName}>{company.name}</p>
          )}
        </div>
        <ul className={styles.links}>
          <li>
            <Link href="/">Today</Link>
          </li>
          {manager && (
            <>
              <li>
                <Link href="/schedule">Schedule</Link>
              </li>
              <li>
                <Link href="/weather">Weather</Link>
              </li>
              <li>
                <Link href="/time">Time</Link>
              </li>
              <li>
                <Link href="/clients">Customers</Link>
              </li>
              <li>
                <Link href="/estimates">Estimates</Link>
              </li>
              <li>
                <Link href="/invoices">Invoices</Link>
              </li>
              <li>
                <Link href="/profit">Profit</Link>
              </li>
              <li>
                <Link href="/team">Team</Link>
              </li>
            </>
          )}
        </ul>
        <form action={signOut} className={styles.signOut}>
          <button type="submit">Sign out</button>
        </form>
      </nav>
      <main className={styles.main}>{children}</main>
    </div>
  );
}
