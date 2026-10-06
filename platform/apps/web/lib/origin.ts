import "server-only";
import { headers } from "next/headers";

/**
 * Public https origin for links in messages and invites. In production this
 * must be configured; falling back to the request's Host header would let a
 * forged Host end up inside invite links.
 */
export async function siteOrigin(): Promise<string> {
  const configured = process.env.NEXT_PUBLIC_SITE_URL;
  if (configured) return configured.replace(/\/$/, "");
  if (process.env.NODE_ENV === "production" && process.env.VERCEL_ENV !== "preview") {
    throw new Error("NEXT_PUBLIC_SITE_URL must be set in production");
  }
  return `https://${(await headers()).get("host")}`;
}
