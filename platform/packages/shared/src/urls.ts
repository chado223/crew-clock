/**
 * Only same-site paths are allowed after sign-in. Browsers drop tabs/newlines
 * and treat backslashes as slashes, so "/\t/evil.com" or "/\\evil.com" would
 * leave the site: resolve against a dummy origin and require it to stay there.
 */
// URL exists in browsers, Node and React Native; declared here because this package is lib-only.
declare const URL: new (url: string, base: string) => { origin: string; pathname: string; search: string; hash: string };

export function safeNext(next: string | null | undefined, fallback = "/"): string {
  if (!next || typeof next !== "string" || !next.startsWith("/")) return fallback;
  if (/[\u0000-\u001f\u007f\\]/.test(next)) return fallback;
  try {
    const u = new URL(next, "http://same.invalid");
    if (u.origin !== "http://same.invalid") return fallback;
    return u.pathname + u.search + u.hash;
  } catch {
    return fallback;
  }
}

/**
 * Absolute public URL for a same-site redirect. Behind a proxy (Render), the
 * request's own address is the internal one (https://localhost:10000), so
 * redirects must be built from the configured public site URL
 * (NEXT_PUBLIC_SITE_URL). The request origin is only accepted as a fallback
 * when the caller passes it (development). The path always goes through
 * safeNext, so it can never point at another site.
 */
export function publicUrl(path: string | null | undefined, siteUrl: string | null | undefined, fallbackOrigin?: string): string {
  const base = originOf(siteUrl) ?? originOf(fallbackOrigin);
  if (!base) throw new Error("NEXT_PUBLIC_SITE_URL must be set to the public https address");
  return base + safeNext(path);
}

function originOf(url: string | null | undefined): string | null {
  if (!url || typeof url !== "string") return null;
  try {
    const u = new URL(url.trim(), "http://same.invalid");
    if (u.origin === "http://same.invalid" || !/^https?:\/\//.test(url.trim())) return null;
    return u.origin;
  } catch {
    return null;
  }
}
