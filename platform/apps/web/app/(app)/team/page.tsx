import type { Metadata } from "next";
import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "./team.module.css";

export const metadata: Metadata = { title: "Team" };
export const dynamic = "force-dynamic";

async function invite(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const supabase = await supabaseServer();
  const { data: token, error } = await supabase.rpc("invite_member", {
    p_tenant_id: company.tenant_id,
    p_email: String(formData.get("email") ?? ""),
    p_role: String(formData.get("role") ?? "crew"),
    p_display_name: String(formData.get("name") ?? "") || undefined,
  });
  if (error) redirect(`/team?error=${encodeURIComponent(friendlyError(error))}`);
  redirect(`/team?invited=${encodeURIComponent(String(token))}`);
}

export default async function TeamPage({
  searchParams,
}: {
  searchParams: Promise<{ invited?: string; error?: string }>;
}) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const { invited, error } = await searchParams;
  const supabase = await supabaseServer();

  const [{ data: employees }, { data: members }, { data: pending }] = await Promise.all([
    supabase.from("employees").select("id, display_name, email, status, user_id").eq("tenant_id", company.tenant_id).order("display_name"),
    supabase.from("memberships").select("user_id, role").eq("tenant_id", company.tenant_id),
    supabase
      .from("invitations")
      .select("id, email, role, expires_at")
      .eq("tenant_id", company.tenant_id)
      .is("accepted_at", null)
      .is("revoked_at", null)
      .gt("expires_at", new Date().toISOString()),
  ]);
  const roleOf = new Map((members ?? []).map((m) => [m.user_id as string, m.role as string]));

  const host = (await headers()).get("host");
  const origin = process.env.NEXT_PUBLIC_SITE_URL ?? `https://${host}`;
  const inviteLink = invited ? `${origin}/invite/${invited}` : null;

  return (
    <div className={styles.page}>
      <h1>Team</h1>

      <section aria-labelledby="invite" className={styles.invite}>
        <h2 id="invite">Invite someone</h2>
        {inviteLink && (
          <div className="notice">
            <p>Send this link to them. It works once, only for that email, and expires in 7 days.</p>
            <input className={`input ${styles.link}`} readOnly value={inviteLink} aria-label="Invite link" />
          </div>
        )}
        {error && <p className="error-text">{error}</p>}
        <form action={invite} className={styles.form}>
          <div className="field">
            <label htmlFor="name">Name</label>
            <input id="name" name="name" className="input" maxLength={120} />
          </div>
          <div className="field">
            <label htmlFor="email">Email</label>
            <input id="email" name="email" type="email" required className="input" />
          </div>
          <div className="field">
            <label htmlFor="role">Role</label>
            <select id="role" name="role" className="select" defaultValue="crew">
              <option value="crew">Crew: clocks in, sees own hours</option>
              {company.role === "owner" && <option value="admin">Admin: manages time and customers</option>}
              {company.role === "owner" && <option value="owner">Owner: full access</option>}
            </select>
          </div>
          <button className="button" type="submit">
            Create invite
          </button>
        </form>
      </section>

      <section aria-labelledby="people" className={styles.people}>
        <h2 id="people">People</h2>
        <ul className={styles.list}>
          {(employees ?? []).map((e) => (
            <li key={e.id} className={styles.row}>
              <span className={styles.name}>{e.display_name}</span>
              <span className={styles.meta}>{e.email ?? "No email"}</span>
              <span className={styles.role}>
                {e.status === "inactive" ? "Inactive" : e.user_id ? (roleOf.get(e.user_id) ?? "Member") : "Not signed up"}
              </span>
            </li>
          ))}
        </ul>
        {(pending ?? []).length > 0 && (
          <>
            <h3 className={styles.sub}>Waiting to accept</h3>
            <ul className={styles.list}>
              {(pending ?? []).map((p) => (
                <li key={p.id} className={styles.row}>
                  <span className={styles.name}>{p.email}</span>
                  <span className={styles.meta}>
                    Expires {new Date(p.expires_at).toLocaleDateString("en-US", { month: "short", day: "numeric" })}
                  </span>
                  <span className={styles.role}>{p.role}</span>
                </li>
              ))}
            </ul>
          </>
        )}
      </section>
    </div>
  );
}
