import "server-only";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import type { Company } from "@crew/shared";
import { supabaseServer } from "./supabase/server";

export const COMPANY_COOKIE = "crew_company";

/** Companies the signed-in user belongs to, plus the one they're working in. */
export async function currentCompany(): Promise<{ companies: Company[]; company: Company }> {
  const supabase = await supabaseServer();
  const { data, error } = await supabase.rpc("my_companies");
  if (error) throw error;
  const companies = (data ?? []) as Company[];
  if (companies.length === 0) {
    // Customers (portal users) are not company members; send them to their portal.
    const { data: accounts } = await supabase.rpc("portal_accounts");
    redirect((accounts ?? []).length > 0 ? "/portal" : "/onboarding");
  }

  const chosen = (await cookies()).get(COMPANY_COOKIE)?.value;
  const company = companies.find((c) => c.tenant_id === chosen) ?? companies[0]!;
  return { companies, company };
}

export function isManager(c: Company) {
  return c.role === "owner" || c.role === "admin";
}

/** Only allow redirects to paths on this site. */
export function safeNext(next: string | null | undefined, fallback = "/") {
  return next && next.startsWith("/") && !next.startsWith("//") && !next.includes("\\") ? next : fallback;
}
