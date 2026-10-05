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

async function createCrew(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const name = String(formData.get("crew_name") ?? "").trim();
  if (!name) redirect(`/team?error=${encodeURIComponent("Name the crew, e.g. Crew 1.")}`);
  const { error } = await (await supabaseServer()).from("crews").insert({ tenant_id: company.tenant_id, name });
  redirect(error ? `/team?error=${encodeURIComponent(friendlyError(error))}` : "/team");
}

async function setCrewMembers(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const supabase = await supabaseServer();
  const crewId = String(formData.get("crew_id"));
  const members = formData.getAll("employee_id").map(String);
  const lead = String(formData.get("lead_id") ?? "");
  const { error: delErr } = await supabase.from("crew_members").delete().eq("crew_id", crewId);
  if (delErr) redirect(`/team?error=${encodeURIComponent(friendlyError(delErr))}`);
  if (members.length > 0) {
    const { error } = await supabase.from("crew_members").insert(
      members.map((employee_id) => ({ tenant_id: company.tenant_id, crew_id: crewId, employee_id, is_lead: employee_id === lead })),
    );
    if (error) redirect(`/team?error=${encodeURIComponent(friendlyError(error))}`);
  }
  redirect("/team");
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

  const [{ data: crews }, { data: crewMembers }] = await Promise.all([
    supabase.from("crews").select("id, name").eq("tenant_id", company.tenant_id).eq("active", true).order("name"),
    supabase.from("crew_members").select("crew_id, employee_id, is_lead").eq("tenant_id", company.tenant_id),
  ]);
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

      <section aria-labelledby="crews" className={styles.people}>
        <h2 id="crews">Crews</h2>
        {(crews ?? []).length === 0 && <p className={styles.meta}>Group people into crews so you can schedule work to them.</p>}
        {(crews ?? []).map((c) => {
          const on = new Set((crewMembers ?? []).filter((m) => m.crew_id === c.id).map((m) => m.employee_id as string));
          const leadId = (crewMembers ?? []).find((m) => m.crew_id === c.id && m.is_lead)?.employee_id as string | undefined;
          const active = (employees ?? []).filter((e) => e.status === "active");
          return (
            <details key={c.id} className={styles.crew}>
              <summary>
                <span className={styles.name}>{c.name}</span>
                <span className={styles.meta}>
                  {on.size === 0 ? "No one yet" : active.filter((e) => on.has(e.id)).map((e) => e.display_name).join(", ")}
                </span>
              </summary>
              <form action={setCrewMembers} className={styles.crewForm}>
                <input type="hidden" name="crew_id" value={c.id} />
                <fieldset className={styles.checks}>
                  <legend>Who's on this crew</legend>
                  {active.map((e) => (
                    <label key={e.id} className={styles.check}>
                      <input type="checkbox" name="employee_id" value={e.id} defaultChecked={on.has(e.id)} /> {e.display_name}
                    </label>
                  ))}
                </fieldset>
                <div className="field">
                  <label htmlFor={`lead-${c.id}`}>Crew lead</label>
                  <select id={`lead-${c.id}`} name="lead_id" className="select" defaultValue={leadId ?? ""}>
                    <option value="">None</option>
                    {active.map((e) => <option key={e.id} value={e.id}>{e.display_name}</option>)}
                  </select>
                </div>
                <button className="button" type="submit">Save crew</button>
              </form>
            </details>
          );
        })}
        <form action={createCrew} className={styles.newCrew}>
          <label htmlFor="crew_name" className={styles.name}>New crew</label>
          <input id="crew_name" name="crew_name" className="input" placeholder="e.g. Crew 2" maxLength={80} />
          <button className="button quiet" type="submit">Add crew</button>
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
