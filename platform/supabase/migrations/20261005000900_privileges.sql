-- Table and function privileges. Re-runnable; keep this file LAST so it covers
-- everything created before it. Future migrations that add tables or functions
-- must add their grants here (a test fails if anything is left open).
--
-- Supabase grants ALL on new public tables to anon and authenticated by
-- default. We replace that with least privilege:
--   anon          : nothing
--   authenticated : only what each table's RLS policies are designed for
--   service_role  : everything (server-side only, bypasses RLS)

alter default privileges in schema public revoke all on tables from anon, authenticated;
alter default privileges in schema public revoke all on sequences from anon, authenticated;
alter default privileges in schema public revoke execute on functions from public, anon, authenticated;

revoke all on all tables in schema public from anon, authenticated;
revoke all on all sequences in schema public from anon, authenticated;
revoke execute on all functions in schema public from public, anon, authenticated;
revoke execute on all functions in schema private from public, anon, authenticated;

grant all on all tables in schema public to service_role;
grant all on all sequences in schema public to service_role;
grant execute on all functions in schema public to service_role;
grant execute on all functions in schema private to service_role;

-- Read/write tables (RLS decides which rows and who may write)
grant select, insert, update, delete on public.clients, public.properties, public.jobs,
  public.crews, public.crew_members, public.invoices, public.expenses to authenticated;
grant select, insert, update on public.employees to authenticated;
grant select, insert on public.employee_pay_rates, public.activity to authenticated;
grant select, insert, update on public.profiles to authenticated;

-- Read-only tables (all changes go through functions)
grant select on public.memberships, public.time_entries, public.time_entry_breaks,
  public.invitations, public.audit_log to authenticated;

-- Tenants: read, and owners may edit settings (not plan/status/billing fields)
grant select on public.tenants to authenticated;
grant update (name, timezone, week_start_day, overtime_weekly_hours) on public.tenants to authenticated;

-- Functions callable by signed-in users (each checks membership/role itself)
grant execute on function
  public.in_tenant(uuid), public.is_admin_or_owner(uuid), public.is_owner(uuid), public.my_employee_id(uuid),
  public.clock_in(uuid, uuid, timestamptz, uuid, text, text),
  public.clock_out(uuid, uuid, timestamptz, text),
  public.start_break(uuid, uuid, timestamptz, boolean),
  public.end_break(uuid, timestamptz),
  public.correct_time_entry(uuid, timestamptz, timestamptz, text),
  public.add_time_entry(uuid, uuid, timestamptz, timestamptz, text, uuid, text),
  public.void_time_entry(uuid, text),
  public.timesheet(uuid, date, date, boolean),
  public.weekly_hours(uuid, date),
  public.create_tenant(text, text),
  public.invite_member(uuid, text, text, uuid, text),
  public.accept_invitation(text),
  public.revoke_invitation(uuid),
  public.set_member_role(uuid, uuid, text),
  public.remove_member(uuid, uuid),
  public.my_companies()
to authenticated;

-- Private helper evaluated inside an RLS policy (runs as the caller).
-- Other private helpers are only called from SECURITY DEFINER functions.
grant execute on function private.shares_tenant_with(uuid) to authenticated;
