import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import { siteOrigin } from "@/lib/origin";
import { readFlash, setFlash } from "@/lib/flash";
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
  // Queue the invite email (test mode: it goes to the company's test address only).
  const { error: mailError } = await supabase.rpc("send_invite_message", {
    p_tenant_id: company.tenant_id,
    p_kind: "team_invite",
    p_to: String(formData.get("email") ?? ""),
    p_link: `${await siteOrigin()}/invite/${String(token)}`,
  });
  if (mailError) console.error("[team] invite email not queued:", mailError.message);
  await setFlash("invite", String(token), "/team");
  redirect("/team?invited=1");
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
  const lead = String(formData.get("lead_id") ?? "");
  const { error } = await (await supabaseServer()).rpc("set_crew_members", {
    p_crew_id: String(formData.get("crew_id")),
    p_employee_ids: formData.getAll("employee_id").map(String),
    p_lead_id: lead || null,
  } as never);
  redirect(error ? `/team?error=${encodeURIComponent(friendlyError(error))}` : "/team");
}

async function saveEmployee(formData: FormData) {
  "use server";
  const t = (k: string) => String(formData.get(k) ?? "").trim() || null;
  const name = t("display_name");
  if (!name) redirect(`/team?error=${encodeURIComponent("Enter a name.")}`);
  const { error, count } = await (await supabaseServer()).from("employees").update({
    display_name: name, email: t("email")?.toLowerCase() ?? null, phone: t("phone"), hired_on: t("hired_on"), notes: t("notes"),
  }, { count: "exact" }).eq("id", String(formData.get("employee_id")));
  if (error) redirect(`/team?error=${encodeURIComponent(friendlyError(error))}`);
  if (!count) redirect(`/team?error=${encodeURIComponent("Only the owner can change an owner's record.")}`);
  redirect("/team?done=saved");
}

async function saveCrew(formData: FormData) {
  "use server";
  const name = String(formData.get("crew_name") ?? "").trim();
  const active = formData.get("active");
  const { error } = await (await supabaseServer()).from("crews").update({
    ...(name ? { name } : {}), ...(active ? { active: active === "on" } : {}),
  }).eq("id", String(formData.get("crew_id")));
  redirect(error ? `/team?error=${encodeURIComponent(friendlyError(error))}` : "/team?done=saved");
}

async function changeRole(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const { error } = await (await supabaseServer()).rpc("set_member_role", {
    p_tenant_id: company.tenant_id, p_user_id: String(formData.get("user_id")), p_role: String(formData.get("role")),
  });
  redirect(error ? `/team?error=${encodeURIComponent(friendlyError(error))}` : "/team?done=role");
}

async function setActive(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const supabase = await supabaseServer();
  const employeeId = String(formData.get("employee_id"));
  const userId = String(formData.get("user_id") ?? "");
  const active = formData.get("active") === "1";
  if (!active && userId) {
    // Removing access first: the database protects owners and the last owner.
    const { error } = await supabase.rpc("remove_member", { p_tenant_id: company.tenant_id, p_user_id: userId });
    if (error) redirect(`/team?error=${encodeURIComponent(friendlyError(error))}`);
  }
  const { error, count } = await supabase.from("employees").update({ status: active ? "active" : "inactive" }, { count: "exact" }).eq("id", employeeId);
  if (error) redirect(`/team?error=${encodeURIComponent(friendlyError(error))}`);
  if (!count) redirect(`/team?error=${encodeURIComponent("Only the owner can change an owner's record.")}`);
  redirect(`/team?done=${active ? "reactivated" : "deactivated"}`);
}

async function revokeInvite(formData: FormData) {
  "use server";
  const { error } = await (await supabaseServer()).rpc("revoke_invitation", { p_invitation_id: String(formData.get("invitation_id")) });
  redirect(error ? `/team?error=${encodeURIComponent(friendlyError(error))}` : "/team?done=revoked");
}

async function setPayRate(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const rate = Number(formData.get("hourly_rate"));
  const from = String(formData.get("effective_from") ?? "");
  if (!Number.isFinite(rate) || rate < 0 || !/^\d{4}-\d{2}-\d{2}$/.test(from)) {
    redirect(`/team?error=${encodeURIComponent("Enter an hourly rate and the date it starts.")}`);
  }
  const { error } = await (await supabaseServer()).from("employee_pay_rates").insert({
    tenant_id: company.tenant_id,
    employee_id: String(formData.get("employee_id")),
    hourly_rate: rate,
    effective_from: from,
  });
  redirect(error ? `/team?error=${encodeURIComponent(error.code === "23505" ? "There's already a rate starting that day." : friendlyError(error))}` : "/team");
}

export default async function TeamPage({
  searchParams,
}: {
  searchParams: Promise<{ invited?: string; error?: string; done?: string }>;
}) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const { invited, error, done } = await searchParams;
  const supabase = await supabaseServer();

  const [{ data: crews }, { data: crewMembers }] = await Promise.all([
    supabase.from("crews").select("id, name").eq("tenant_id", company.tenant_id).eq("active", true).order("name"),
    supabase.from("crew_members").select("crew_id, employee_id, is_lead").eq("tenant_id", company.tenant_id),
  ]);
  const [{ data: employees }, { data: members }, { data: pending }] = await Promise.all([
    supabase.from("employees").select("id, display_name, email, phone, hired_on, notes, status, user_id").eq("tenant_id", company.tenant_id).order("display_name"),
    supabase.from("memberships").select("user_id, role").eq("tenant_id", company.tenant_id),
    supabase
      .from("invitations")
      .select("id, email, role, expires_at")
      .eq("tenant_id", company.tenant_id)
      .is("accepted_at", null)
      .is("revoked_at", null)
      .gt("expires_at", new Date().toISOString()),
  ]);
  const { data: rates } = await supabase
    .from("employee_pay_rates")
    .select("employee_id, hourly_rate, effective_from")
    .eq("tenant_id", company.tenant_id)
    .order("effective_from", { ascending: false });
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date());
  const currentRate = new Map<string, number>();
  for (const r of rates ?? []) if (r.effective_from <= today && !currentRate.has(r.employee_id)) currentRate.set(r.employee_id, Number(r.hourly_rate));
  const roleOf = new Map((members ?? []).map((m) => [m.user_id as string, m.role as string]));

  const flashToken = invited ? await readFlash("invite") : null;
  const inviteLink = flashToken ? `${await siteOrigin()}/invite/${flashToken}` : null;

  return (
    <div className={styles.page}>
      <h1>Team</h1>
      {done && <p className="notice" role="status">{{ saved: "Saved.", role: "Role changed.", deactivated: "Deactivated. They can no longer sign in to this company.", reactivated: "Reactivated.", revoked: "Invite canceled." }[done] ?? "Saved."}</p>}

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
              <form action={saveCrew} className={styles.inlineRow}>
                <input type="hidden" name="crew_id" value={c.id} />
                <label htmlFor={`cn-${c.id}`}>Rename</label>
                <input id={`cn-${c.id}`} name="crew_name" defaultValue={c.name} className="input" maxLength={80} />
                <button className="button quiet" type="submit">Save name</button>
                <button className="button quiet" type="submit" name="active" value="off">Retire crew</button>
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
            <li key={e.id}>
              <details className={styles.person}>
                <summary className={styles.row}>
                  <span className={styles.name}>{e.display_name}</span>
                  <span className={styles.meta}>
                    {e.email ?? "No email"}
                    {currentRate.has(e.id) ? `, $${currentRate.get(e.id)!.toFixed(2)}/h` : ", no pay rate"}
                  </span>
                  <span className={styles.role}>
                    {e.status === "inactive" ? "Inactive" : e.user_id ? (roleOf.get(e.user_id) ?? "Member") : "Not signed up"}
                  </span>
                </summary>
                <form action={saveEmployee} className={styles.rateForm}>
                  <input type="hidden" name="employee_id" value={e.id} />
                  <div className="field"><label htmlFor={`en-${e.id}`}>Name</label><input id={`en-${e.id}`} name="display_name" required defaultValue={e.display_name} className="input" /></div>
                  <div className="field"><label htmlFor={`ee-${e.id}`}>Email</label><input id={`ee-${e.id}`} name="email" type="email" defaultValue={e.email ?? ""} className="input" /></div>
                  <div className="field"><label htmlFor={`ep-${e.id}`}>Phone</label><input id={`ep-${e.id}`} name="phone" type="tel" defaultValue={e.phone ?? ""} className="input" /></div>
                  <div className="field"><label htmlFor={`eh-${e.id}`}>Hired</label><input id={`eh-${e.id}`} name="hired_on" type="date" defaultValue={e.hired_on ?? ""} className="input" /></div>
                  <div className="field"><label htmlFor={`eo-${e.id}`}>Notes (office only)</label><input id={`eo-${e.id}`} name="notes" defaultValue={e.notes ?? ""} className="input" /></div>
                  <button className="button quiet" type="submit">Save details</button>
                </form>
                <form action={setPayRate} className={styles.rateForm}>
                  <input type="hidden" name="employee_id" value={e.id} />
                  <div className="field">
                    <label htmlFor={`rate-${e.id}`}>Hourly pay ($)</label>
                    <input id={`rate-${e.id}`} name="hourly_rate" type="number" min={0} step="0.01" required className="input"
                      defaultValue={currentRate.get(e.id)?.toFixed(2)} />
                  </div>
                  <div className="field">
                    <label htmlFor={`from-${e.id}`}>Starting</label>
                    <input id={`from-${e.id}`} name="effective_from" type="date" required className="input" defaultValue={today} />
                  </div>
                  <button className="button" type="submit">Save pay rate</button>
                  <p className={`hint ${styles.rateHint}`}>Earlier pay stays on earlier work, so past job costs don't change.</p>
                </form>
                <div className={styles.rateForm}>
                  {company.role === "owner" && e.user_id && e.status === "active" && (
                    <form action={changeRole} className={styles.inlineRow}>
                      <input type="hidden" name="user_id" value={e.user_id} />
                      <label htmlFor={`role-${e.id}`}>Role</label>
                      <select id={`role-${e.id}`} name="role" defaultValue={roleOf.get(e.user_id) ?? "crew"} className="select">
                        <option value="crew">Crew</option>
                        <option value="admin">Admin</option>
                        <option value="owner">Owner</option>
                      </select>
                      <button className="button quiet" type="submit">Change role</button>
                    </form>
                  )}
                  <form action={setActive} className={styles.inlineRow}>
                    <input type="hidden" name="employee_id" value={e.id} />
                    <input type="hidden" name="user_id" value={e.user_id ?? ""} />
                    {e.status === "active" ? (
                      <>
                        <input type="hidden" name="active" value="0" />
                        <button className="button quiet" type="submit">Deactivate</button>
                        <span className="hint">Removes their access. Their hours and history stay.</span>
                      </>
                    ) : (
                      <>
                        <input type="hidden" name="active" value="1" />
                        <button className="button quiet" type="submit">Reactivate</button>
                        <span className="hint">To sign in again they need a new invite.</span>
                      </>
                    )}
                  </form>
                </div>
              </details>
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
                  <form action={revokeInvite}>
                    <input type="hidden" name="invitation_id" value={p.id} />
                    <button type="submit" className="button quiet">Cancel invite</button>
                  </form>
                </li>
              ))}
            </ul>
          </>
        )}
      </section>
    </div>
  );
}
