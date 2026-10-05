-- ONE-TIME PRODUCTION DATA CHANGE. DO NOT RUN WITHOUT CHAD'S APPROVAL.
--
-- Makes chadwasham64@gmail.com the owner of Chad Washam Lawncare
-- (tenant 055bdb3c-c8d0-47d4-aa70-a77739054d7e) in production, after the
-- Phase 1 migrations are applied. Production currently has no memberships,
-- so nobody can see the company in the new app until this runs.
--
-- Safe to re-run: inserts only if missing, changes nothing else, deletes nothing.
-- Fails loudly if the login or company isn't exactly what we expect.

begin;

do $$
declare
  v_user uuid;
  v_tenant constant uuid := '055bdb3c-c8d0-47d4-aa70-a77739054d7e';
begin
  select id into strict v_user from auth.users where lower(email) = 'chadwasham64@gmail.com';
  if not exists (select 1 from public.tenants where id = v_tenant and name = 'Chad Washam Lawncare') then
    raise exception 'Expected tenant not found; aborting';
  end if;

  perform set_config('app.audit_reason', 'link production owner (approved by Chad)', true);

  insert into public.memberships (tenant_id, user_id, role)
  values (v_tenant, v_user, 'owner')
  on conflict (tenant_id, user_id) do nothing;

  insert into public.employees (tenant_id, user_id, display_name, email)
  select v_tenant, v_user, 'Chad Washam', 'chadwasham64@gmail.com'
  where not exists (select 1 from public.employees where tenant_id = v_tenant and user_id = v_user);

  update public.profiles set full_name = coalesce(full_name, 'Chad Washam') where id = v_user;
end $$;

-- Verify before commit: expect exactly one owner row for this login.
select m.role, e.display_name, u.email
from public.memberships m
join auth.users u on u.id = m.user_id
join public.employees e on e.tenant_id = m.tenant_id and e.user_id = m.user_id
where m.tenant_id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e';

commit;
