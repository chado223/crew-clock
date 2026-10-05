-- Table and function privileges. Re-runnable; keep this file LAST so it covers
-- everything created before it. New tables/functions must add grants here
-- (tests/40_security_posture.sql fails if a table is left open to anon).
--
-- Only tables this platform owns are touched. public.scenarios (another app
-- sharing this Supabase project) keeps its existing grants and policies.
--   anon          : nothing
--   authenticated : only what each table's RLS policies are designed for
--   service_role  : everything (server-side only, bypasses RLS)

do $$
declare t text;
begin
  foreach t in array array['tenants','profiles','memberships','employees','employee_pay_rates','crews','crew_members',
                           'clients','properties','jobs','time_entries','time_entry_breaks','invoices','expenses',
                           'invitations','audit_log','activity'] loop
    execute format('revoke all on public.%I from anon, authenticated', t);
  end loop;
end $$;
revoke all on sequence public.audit_log_id_seq from anon, authenticated;
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
grant select, insert (id, full_name) on public.profiles to authenticated;
grant update (full_name) on public.profiles to authenticated;

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
