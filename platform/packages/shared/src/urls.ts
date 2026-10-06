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
