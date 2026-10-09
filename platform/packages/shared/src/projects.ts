/**
 * Which Supabase project each environment must use. The apps call
 * assertProjectForEnv at start-up, so a production build pointed at staging
 * (or the reverse, or the old shared project) refuses to run instead of
 * quietly signing people into the wrong database.
 */
export const PROJECTS = {
  production: "kymnehbnmqvpzwtxtizm", // crew-clock-prod, CWLC org, us-east-2
  staging: "newivmnolbzhjypusmob", // crew-clock-staging
} as const;

/** Never used by the new app: the old shared project (frozen archive). */
export const RETIRED_PROJECTS = ["iwowjrnrbjiydckhjsfi"] as const;

export type AppEnv = keyof typeof PROJECTS | "development";

export function projectRef(url: string): string | null {
  const m = /^https:\/\/([a-z0-9]{20})\.supabase\.co\/?$/.exec(url.trim());
  return m ? m[1]! : null;
}

/** Throws if the URL is not the project this environment must use. */
export function assertProjectForEnv(url: string | undefined, env: string | undefined): string {
  if (!url) throw new Error("Supabase URL is not set for this build");
  const ref = projectRef(url);
  if (!ref) throw new Error(`Not a Supabase project URL: ${url}`);
  if ((RETIRED_PROJECTS as readonly string[]).includes(ref)) throw new Error("This build points at the retired shared project");
  const e = (env ?? "development") as AppEnv;
  if (e === "production" && ref !== PROJECTS.production) throw new Error(`Production build must use ${PROJECTS.production}, not ${ref}`);
  if (e === "staging" && ref !== PROJECTS.staging) throw new Error(`Staging build must use ${PROJECTS.staging}, not ${ref}`);
  if (e === "development" && ref === PROJECTS.production) throw new Error("Development builds may not use the production project");
  return ref;
}
