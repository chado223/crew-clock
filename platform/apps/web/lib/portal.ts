import "server-only";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { supabaseServer } from "./supabase/server";

export const PORTAL_COOKIE = "crew_portal_account";

export type PortalAccount = {
  client_id: string;
  client_name: string;
  tenant_id: string;
  company_name: string;
  company_timezone: string;
};

/**
 * The customer account being viewed. Every portal read/write still goes
 * through portal_* database functions that re-check access; this only picks
 * which of the customer's own accounts to show.
 */
export async function currentPortalAccount(): Promise<{ accounts: PortalAccount[]; account: PortalAccount }> {
  const supabase = await supabaseServer();
  const { data, error } = await supabase.rpc("portal_accounts");
  if (error) throw error;
  const accounts = (data ?? []) as PortalAccount[];
  if (accounts.length === 0) redirect("/portal/none");
  const chosen = (await cookies()).get(PORTAL_COOKIE)?.value;
  return { accounts, account: accounts.find((a) => a.client_id === chosen) ?? accounts[0]! };
}

export function portalDate(ymdOrIso: string, timeZone: string, opts: Intl.DateTimeFormatOptions = { weekday: "short", month: "short", day: "numeric" }) {
  const d = /^\d{4}-\d{2}-\d{2}$/.test(ymdOrIso) ? new Date(`${ymdOrIso}T12:00:00Z`) : new Date(ymdOrIso);
  return new Intl.DateTimeFormat("en-US", { ...opts, timeZone: /^\d{4}-\d{2}-\d{2}$/.test(ymdOrIso) ? "UTC" : timeZone }).format(d);
}
