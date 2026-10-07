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
  redirect(error ? `/login?error=send&next=${encodeURIComponent(next)}`
                 : `/login?sent=${encodeURIComponent(email)}&next=${encodeURIComponent(next)}`);
}

/** Typing the 6-digit code works on any device (the emailed link only works in the browser that asked for it). */
async function verifyCode(formData: FormData) {
  "use server";
  const email = String(formData.get("email") ?? "").trim().toLowerCase();
  const token = String(formData.get("code") ?? "").replace(/\s/g, "");
  const next = safeNext(String(formData.get("next") ?? ""));
  const back = `/login?sent=${encodeURIComponent(email)}&next=${encodeURIComponent(next)}`;
  if (!/^\d{6,10}$/.test(token)) redirect(`${back}&error=code`);
  const supabase = await supabaseServer();
  const { error } = await supabase.auth.verifyOtp({ email, token, type: "email" });
  redirect(error ? `${back}&error=code` : next);
}

export default async function LoginPage({
  searchParams,
}: {
  searchParams: Promise<{ next?: string; sent?: string; error?: string; deleted?: string }>;
}) {
  const { next, sent, error, deleted } = await searchParams;

  return (
    <main className={styles.page}>
      <div className={styles.panel}>
        <p className={styles.brand}>Crew</p>
        {sent ? (
          <>
            <h1>Check your email</h1>
            <p>
              We sent a sign-in code to <strong>{sent}</strong>. Type it below, or open the link in the email on this device.
            </p>
            <form action={verifyCode} className={styles.form}>
              <input type="hidden" name="email" value={sent} />
              <input type="hidden" name="next" value={safeNext(next)} />
              <div className="field">
                <label htmlFor="code">Code from the email</label>
                <input id="code" name="code" inputMode="numeric" autoComplete="one-time-code" pattern="[0-9 ]{6,12}"
                       required className="input" autoFocus />
              </div>
              {error === "code" && <p className="error-text">That code didn't work. Check it, or send a new one.</p>}
              <button className="button" type="submit">Sign in</button>
            </form>
            <a className="button quiet" href="/login">
              Use a different email
            </a>
          </>
        ) : (
          <>
            {deleted && <p className={styles.lede}>Your account was deleted. Thanks for using Crew.</p>}
            <h1>Sign in</h1>
            <p className={styles.lede}>Enter your work email and we'll email you a sign-in code. No password needed.</p>
            <form action={sendLink} className={styles.form}>
              <input type="hidden" name="next" value={safeNext(next)} />
              <div className="field">
                <label htmlFor="email">Work email</label>
                <input id="email" name="email" type="email" autoComplete="email" required className="input" />
              </div>
              {error === "email" && <p className="error-text">Enter a valid email address.</p>}
              {error === "send" && <p className="error-text">We couldn't send the code. Wait a minute and try again.</p>}
              {error === "link" && <p className="error-text">That sign-in link has expired. Send a new one.</p>}
              <button className="button" type="submit">
                Email me a code
              </button>
            </form>
          </>
        )}
      </div>
    </main>
  );
}
