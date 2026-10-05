import type { Metadata } from "next";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { supabaseServer } from "@/lib/supabase/server";
import { COMPANY_COOKIE } from "@/lib/company";
import styles from "../auth.module.css";

export const metadata: Metadata = { title: "Set up your company" };

const ZONES = [
  ["America/New_York", "Eastern"],
  ["America/Chicago", "Central"],
  ["America/Denver", "Mountain"],
  ["America/Phoenix", "Arizona"],
  ["America/Los_Angeles", "Pacific"],
  ["America/Anchorage", "Alaska"],
  ["Pacific/Honolulu", "Hawaii"],
] as const;

async function createCompany(formData: FormData) {
  "use server";
  const supabase = await supabaseServer();
  const { data, error } = await supabase.rpc("create_tenant", {
    p_name: String(formData.get("name") ?? ""),
    p_timezone: String(formData.get("timezone") ?? "America/New_York"),
  });
  if (error) redirect(`/onboarding?error=${encodeURIComponent(friendlyError(error))}`);
  (await cookies()).set(COMPANY_COOKIE, String(data), { httpOnly: true, sameSite: "lax", secure: true, path: "/" });
  redirect("/");
}

export default async function OnboardingPage({ searchParams }: { searchParams: Promise<{ error?: string }> }) {
  const { error } = await searchParams;
  return (
    <main className={styles.page}>
      <div className={styles.panel}>
        <p className={styles.brand}>Crew</p>
        <h1>Set up your company</h1>
        <p className={styles.lede}>
          You'll be the owner. After this you can invite your crew and start tracking time.
        </p>
        <form action={createCompany} className={styles.form}>
          <div className="field">
            <label htmlFor="name">Company name</label>
            <input id="name" name="name" required minLength={2} maxLength={120} className="input" />
          </div>
          <div className="field">
            <label htmlFor="timezone">Time zone</label>
            <select id="timezone" name="timezone" className="select" defaultValue="America/New_York">
              {ZONES.map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
            <p className="hint">Used for work days, weekly hours and overtime.</p>
          </div>
          {error && <p className="error-text">{error}</p>}
          <button className="button" type="submit">
            Create company
          </button>
        </form>
      </div>
    </main>
  );
}
