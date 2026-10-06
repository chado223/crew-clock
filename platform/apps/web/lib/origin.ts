import "server-only";
import { headers } from "next/headers";

/** Public https origin for links in messages and invites. */
export async function siteOrigin(): Promise<string> {
  if (process.env.NEXT_PUBLIC_SITE_URL) return process.env.NEXT_PUBLIC_SITE_URL.replace(/\/$/, "");
  return `https://${(await headers()).get("host")}`;
}
