import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../auth.module.css";

export const metadata: Metadata = { title: "Your account" };
export const dynamic = "force-dynamic";

async function deleteAccount(formData: FormData) {
  "use server";
  const confirm = String(formData.get("confirm") ?? "").trim();
  if (confirm !== "DELETE") redirect("/account?error=confirm_required");
  const supabase = await supabaseServer();
  const { error } = await supabase.rpc("delete_my_account", { p_confirm: confirm });
  if (error) redirect(`/account?error=${encodeURIComponent(error.message)}`);
  await supabase.auth.signOut({ scope: "local" });
  redirect("/login?deleted=1");
}

export default async function AccountPage({ searchParams }: { searchParams: Promise<{ error?: string }> }) {
  const { error } = await searchParams;
  const supabase = await supabaseServer();
  const { data: { user } } = await supabase.auth.getUser();
  if (!user) redirect("/login?next=/account");

  return (
    <main className={styles.page}>
      <div className={styles.panel}>
        <p className={styles.brand}>Crew</p>
        <h1>Your account</h1>
        <p className={styles.lede}>Signed in as <strong>{user.email}</strong>.</p>
        <Link className="button quiet" href="/">Back</Link>

        <h2>Delete my account</h2>
        <p className={styles.lede}>
          This removes your sign-in and your access to every company and customer portal. Hours, visits, invoices and
          payments you were part of stay with the company, because they are its business records.
        </p>
        <p className={styles.lede}>
          If you're a company's only owner, make someone else an owner first, or contact support to close the company.
        </p>
        <form action={deleteAccount} className={styles.form}>
          <div className="field">
            <label htmlFor="confirm">Type DELETE to confirm</label>
            <input id="confirm" name="confirm" className="input" autoComplete="off" required />
          </div>
          {error && <p className="error-text">{friendlyError(error)}</p>}
          <button className="button danger" type="submit">Delete my account</button>
        </form>
        <p className={styles.lede}><Link href="/privacy">Privacy</Link> · <Link href="/terms">Terms</Link></p>
      </div>
    </main>
  );
}
