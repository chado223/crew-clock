import "server-only";
import { cookies } from "next/headers";

/**
 * One-time secrets (fresh invite links) are handed to the next page in a
 * short-lived, path-scoped httpOnly cookie instead of the URL, so they don't
 * land in browser history, server logs or Referer headers.
 */
export async function setFlash(name: string, value: string, path: string) {
  (await cookies()).set(`flash_${name}`, value, { httpOnly: true, secure: true, sameSite: "lax", path, maxAge: 300 });
}

export async function readFlash(name: string): Promise<string | null> {
  return (await cookies()).get(`flash_${name}`)?.value ?? null;
}
