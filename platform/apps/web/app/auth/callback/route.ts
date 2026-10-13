import { NextResponse, type NextRequest } from "next/server";
import { publicUrl } from "@crew/shared";
import { supabaseServer } from "@/lib/supabase/server";
import { safeNext } from "@/lib/company";

/**
 * Finishes an emailed sign-in link. Redirects are built from the public site
 * address (NEXT_PUBLIC_SITE_URL): behind Render's proxy the request's own
 * origin is the internal https://localhost:10000 (issue T-3). Only local
 * development falls back to the request origin.
 */
export async function GET(request: NextRequest) {
  const url = request.nextUrl;
  const code = url.searchParams.get("code");
  const next = safeNext(url.searchParams.get("next"));
  const fallback = process.env.NODE_ENV === "production" ? undefined : url.origin;
  const to = (path: string) => NextResponse.redirect(publicUrl(path, process.env.NEXT_PUBLIC_SITE_URL, fallback));

  if (code) {
    const supabase = await supabaseServer();
    const { error } = await supabase.auth.exchangeCodeForSession(code);
    if (!error) return to(next);
  }
  return to(`/login?error=link&next=${encodeURIComponent(next)}`);
}
