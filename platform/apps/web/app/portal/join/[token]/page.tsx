import type { Metadata } from "next";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { supabaseServer } from "@/lib/supabase/server";
import { PORTAL_COOKIE } from "@/lib/portal";
import styles from "../../portal.module.css";

export const metadata: Metadata = { title: "Set up your account" };

async function accept(formData: FormData) {
  "use server";
  const token = String(formData.get("token") ?? "");
  const { data, error } = await (await supabaseServer()).rpc("accept_portal_invitation", { p_token: token });
  if (error) redirect(`/portal/join/${encodeURIComponent(token)}?error=${encodeURIComponent(friendlyError(error))}`);
  (await cookies()).set(PORTAL_COOKIE, String(data), { httpOnly: true, sameSite: "lax", secure: true, path: "/" });
  redirect("/portal");
}

export default async function JoinPortal({ params, searchParams }: { params: Promise<{ token: string }>; searchParams: Promise<{ error?: string }> }) {
  const { token } = await params;
  const { error } = await searchParams;
  const { data: { user } } = await (await supabaseServer()).auth.getUser();
  return (
    <section className={styles.section}>
      <h1>See your visits and invoices online</h1>
      <p>Signed in as <strong>{user?.email}</strong>. Continue to connect your service account.</p>
      {error && <p className="error-text" role="alert">{error}</p>}
      <form action={accept} className={styles.form}>
        <input type="hidden" name="token" value={token} />
        <button className="button" type="submit">Continue</button>
      </form>
    </section>
  );
}
