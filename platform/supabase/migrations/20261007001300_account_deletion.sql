-- Delete my account (required by Apple for apps that create accounts).
--
-- Removes the person's sign-in and their access (memberships, customer-portal
-- access, profile). Business history stays: time entries, visits, payments,
-- audit log and the employee record keep their rows; the link to the login
-- becomes empty (those foreign keys are ON DELETE SET NULL) and the employee is
-- marked inactive. Nothing about other people or the company is touched.
--
-- Refuses when deleting would leave a company with no owner, while the person
-- is clocked in, or when the login also has the other app's data (shared
-- project only), so that app's data is never removed by Crew Clock.

create or replace function public.delete_my_account(p_confirm text)
returns void
language plpgsql security definer set search_path = '' as $$
declare
  v_uid uuid := auth.uid();
  v_other boolean := false;
  r record;
begin
  if v_uid is null then raise exception 'not_authenticated' using errcode = '28000'; end if;
  if p_confirm is distinct from 'DELETE' then raise exception 'confirm_required' using errcode = '22023'; end if;

  if exists (
    select 1 from public.memberships m
    where m.user_id = v_uid and m.role = 'owner'
      and not exists (select 1 from public.memberships o where o.tenant_id = m.tenant_id and o.role = 'owner' and o.user_id <> v_uid)
  ) then
    raise exception 'last_owner' using errcode = 'P0001',
      hint = 'Make someone else an owner first, or contact support to close the company.';
  end if;

  if exists (select 1 from public.time_entries t
             where t.user_id = v_uid and t.clock_out is null and t.voided_at is null) then
    raise exception 'still_clocked_in' using errcode = 'P0001';
  end if;

  if to_regclass('public.scenarios') is not null then
    execute 'select exists (select 1 from public.scenarios where user_id = $1)' into v_other using v_uid;
    if v_other then raise exception 'other_app_data' using errcode = 'P0001'; end if;
  end if;

  perform set_config('app.audit_reason', 'Account deleted by its owner', true);

  for r in select e.tenant_id, e.id, e.display_name from public.employees e where e.user_id = v_uid loop
    update public.employees set status = 'inactive' where id = r.id;
    perform private.log_activity(r.tenant_id, null, null, 'account_deleted',
      format('%s deleted their sign-in. Their history is kept.', r.display_name), jsonb_build_object('employee_id', r.id));
  end loop;

  -- Access rows cascade from auth.users; history columns are set to null.
  delete from auth.users where id = v_uid;
end $$;

revoke execute on function public.delete_my_account(text) from public, anon;
grant execute on function public.delete_my_account(text) to authenticated;
