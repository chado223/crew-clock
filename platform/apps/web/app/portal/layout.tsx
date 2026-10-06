import type { Metadata } from "next";
import Link from "next/link";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { supabaseServer } from "@/lib/supabase/server";
import { PORTAL_COOKIE } from "@/lib/portal";
import styles from "./portal.module.css";

export const metadata: Metadata = { title: { default: "Your account", template: "%s" } };

async function switchAccount(formData: FormData) {
  "use server";
  (await cookies()).set(PORTAL_COOKIE, String(formData.get("client_id") ?? ""), { httpOnly: true, sameSite: "lax", secure: true, path: "/" });
  redirect("/portal");
}

async function signOut() {
  "use server";
  await (await supabaseServer()).auth.signOut();
  redirect("/login");
}

/** Customer-facing shell. Shows the lawn-care company's name, not ours. */
export default async function PortalLayout({ children }: { children: React.ReactNode }) {
  const supabase = await supabaseServer();
  const { data } = await supabase.rpc("portal_accounts");
  const accounts = (data ?? []) as { client_id: string; client_name: string; company_name: string }[];
  const chosen = (await cookies()).get(PORTAL_COOKIE)?.value;
  const account = accounts.find((a) => a.client_id === chosen) ?? accounts[0];

  return (
    <div className={styles.shell}>
      <header className={styles.top}>
        <div className={styles.brand}>
          <p className={styles.company}>{account?.company_name ?? "Your service account"}</p>
          {account && <p className={styles.for}>{account.client_name}</p>}
        </div>
        {accounts.length > 1 && (
          <form action={switchAccount} className={styles.switcher}>
            <label htmlFor="acct" className={styles.srOnly}>Account</label>
            <select id="acct" name="client_id" defaultValue={account?.client_id} className="select">
              {accounts.map((a) => <option key={a.client_id} value={a.client_id}>{a.company_name}: {a.client_name}</option>)}
            </select>
            <button type="submit" className="button quiet">Switch</button>
          </form>
        )}
        <form action={signOut}><button type="submit" className={styles.signOut}>Sign out</button></form>
      </header>
      {account && (
        <nav className={styles.nav} aria-label="Account">
          <Link href="/portal">Overview</Link>
          <Link href="/portal/history">History</Link>
          <Link href="/portal/estimates">Estimates</Link>
          <Link href="/portal/invoices">Invoices</Link>
          <Link href="/portal/request">Request service</Link>
          <Link href="/portal/messages">Messages</Link>
          <Link href="/portal/account">Contact details</Link>
        </nav>
      )}
      <main className={styles.main}>{children}</main>
    </div>
  );
}
