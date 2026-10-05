import type { Metadata } from "next";
import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { supabaseServer } from "@/lib/supabase/server";
import { safeNext } from "@/lib/company";
import styles from "../auth.module.css";

export const metadata: Metadata = { title: "Sign in" };

async function sendLink(formData: FormData) {
  "use server";
  const email = String(formData.get("email") ?? "").trim().toLowerCase();
  const next = safeNext(String(formData.get("next") ?? ""));
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) {
    redirect(`/login?error=email&next=${encodeURIComponent(next)}`);
  }
  const origin = process.env.NEXT_PUBLIC_SITE_URL ?? `https://${(await headers()).get("host")}`;
  const supabase = await supabaseServer();
  const { error } = await supabase.auth.signInWithOtp({
    email,
    options: { emailRedirectTo: `${origin}/auth/callback?next=${encodeURIComponent(next)}` },
  });
  redirect(error ? `/login?error=send&next=${encodeURIComponent(next)}` : `/login?sent=${encodeURIComponent(email)}`);
}

export default async function LoginPage({
  searchParams,
}: {
  searchParams: Promise<{ next?: string; sent?: string; error?: string }>;
}) {
  const { next, sent, error } = await searchParams;

  return (
    <main className={styles.page}>
      <div className={styles.panel}>
        <p className={styles.brand}>Crew</p>
        {sent ? (
          <>
            <h1>Check your email</h1>
            <p>
              We sent a sign-in link to <strong>{sent}</strong>. Open it on this device to continue.
            </p>
            <a className="button quiet" href="/login">
              Use a different email
            </a>
          </>
        ) : (
          <>
            <h1>Sign in</h1>
            <p className={styles.lede}>Enter your work email and we'll send you a sign-in link. No password needed.</p>
            <form action={sendLink} className={styles.form}>
              <input type="hidden" name="next" value={safeNext(next)} />
              <div className="field">
                <label htmlFor="email">Work email</label>
                <input id="email" name="email" type="email" autoComplete="email" required className="input" />
              </div>
              {error === "email" && <p className="error-text">Enter a valid email address.</p>}
              {error === "send" && <p className="error-text">We couldn't send the link. Wait a minute and try again.</p>}
              {error === "link" && <p className="error-text">That sign-in link has expired. Send a new one.</p>}
              <button className="button" type="submit">
                Email me a sign-in link
              </button>
            </form>
          </>
        )}
      </div>
    </main>
  );
}
