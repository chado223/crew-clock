import type { Metadata } from "next";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { supabaseServer } from "@/lib/supabase/server";
import { COMPANY_COOKIE } from "@/lib/company";
import styles from "../../auth.module.css";

export const metadata: Metadata = { title: "Join your team" };

async function accept(formData: FormData) {
  "use server";
  const token = String(formData.get("token") ?? "");
  const supabase = await supabaseServer();
  const { data, error } = await supabase.rpc("accept_invitation", { p_token: token });
  if (error) redirect(`/invite/${encodeURIComponent(token)}?error=${encodeURIComponent(friendlyError(error))}`);
  (await cookies()).set(COMPANY_COOKIE, String(data), { httpOnly: true, sameSite: "lax", secure: true, path: "/" });
  redirect("/");
}

export default async function InvitePage({
  params,
  searchParams,
}: {
  params: Promise<{ token: string }>;
  searchParams: Promise<{ error?: string }>;
}) {
  const { token } = await params;
  const { error } = await searchParams;
  const supabase = await supabaseServer();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  return (
    <main className={styles.page}>
      <div className={styles.panel}>
        <p className={styles.brand}>Crew</p>
        <h1>Join your team</h1>
        <p className={styles.lede}>
          You're signed in as <strong>{user?.email}</strong>. Accept to join the company that invited you.
        </p>
        <form action={accept} className={styles.form}>
          <input type="hidden" name="token" value={token} />
          {error && <p className="error-text">{error}</p>}
          <button className="button" type="submit">
            Accept invite
          </button>
        </form>
      </div>
    </main>
  );
}
