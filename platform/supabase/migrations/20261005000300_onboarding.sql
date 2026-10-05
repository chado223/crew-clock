-- Self-serve onboarding: any signed-in user can create a company, invite their
-- team, and manage roles. No developer involvement needed.
--
-- Membership changes happen only through these functions (no direct table
-- writes), so role rules can't be bypassed from a client.

create or replace function public.create_tenant(p_name text, p_timezone text default 'America/New_York')
returns uuid
language plpgsql security definer set search_path = '' as $$
declare
  v_uid uuid := auth.uid();
  v_name text := trim(coalesce(p_name, ''));
  v_tenant uuid;
  v_display text;
  v_email text;
begin
  if v_uid is null then raise exception 'not_authenticated' using errcode = '42501'; end if;
  if length(v_name) < 2 or length(v_name) > 120 then
    raise exception 'invalid_company_name' using errcode = '22023';
  end if;
  if (select count(*) from public.memberships where user_id = v_uid and role = 'owner') >= 10 then
    raise exception 'too_many_companies' using errcode = '54000';
  end if;

  perform set_config('app.audit_reason', 'create_tenant', true);
  insert into public.tenants (name, plan, timezone, created_by)
  values (v_name, 'trial', coalesce(nullif(trim(p_timezone), ''), 'America/New_York'), v_uid)
  returning id into v_tenant;

  insert into public.memberships (tenant_id, user_id, role) values (v_tenant, v_uid, 'owner');

  select u.email, coalesce(nullif(trim(p.full_name), ''), split_part(u.email, '@', 1), 'Owner')
    into v_email, v_display
  from auth.users u left join public.profiles p on p.id = u.id
  where u.id = v_uid;

  insert into public.employees (tenant_id, user_id, display_name, email)
  values (v_tenant, v_uid, v_display, v_email);

  return v_tenant;
end $$;

-- Returns the raw invitation token ONCE. Only its hash is stored.
-- The caller (web app / Edge Function) emails the link.
create or replace function public.invite_member(
  p_tenant_id uuid, p_email text, p_role text default 'crew',
  p_employee_id uuid default null, p_display_name text default null
) returns text
language plpgsql security definer set search_path = '' as $$
declare
  v_email text := lower(trim(coalesce(p_email, '')));
  v_token text;
  v_employee uuid := p_employee_id;
begin
  perform private.require_manager(p_tenant_id);
  if p_role not in ('owner','admin','crew') then raise exception 'invalid_role' using errcode = '22023'; end if;
  if p_role in ('owner','admin') and not public.is_owner(p_tenant_id) then
    raise exception 'only_owner_can_invite_managers' using errcode = '42501';
  end if;
  if v_email !~ '^[^@\s]+@[^@\s]+\.[^@\s]+$' then raise exception 'invalid_email' using errcode = '22023'; end if;
  if exists (select 1 from public.memberships m join auth.users u on u.id = m.user_id
             where m.tenant_id = p_tenant_id and lower(u.email) = v_email) then
    raise exception 'already_member' using errcode = '23505';
  end if;

  if v_employee is not null then
    if not exists (select 1 from public.employees where id = v_employee and tenant_id = p_tenant_id and user_id is null) then
      raise exception 'employee_not_linkable' using errcode = '22023';
    end if;
  elsif p_role = 'crew' then
    -- Reuse an unlinked employee with this email (e.g. a re-sent invite) rather than duplicating them.
    select id into v_employee from public.employees
     where tenant_id = p_tenant_id and user_id is null and lower(email) = v_email
     order by created_at desc limit 1;
  end if;
  if v_employee is null and p_role = 'crew' then
    -- New crew member: create their employee record now so managers can schedule them before they accept.
    insert into public.employees (tenant_id, display_name, email)
    values (p_tenant_id, coalesce(nullif(trim(p_display_name), ''), split_part(v_email, '@', 1)), v_email)
    returning id into v_employee;
  end if;

  perform set_config('app.audit_reason', 'invite_member', true);
  update public.invitations set revoked_at = now()
   where tenant_id = p_tenant_id and lower(email) = v_email and accepted_at is null and revoked_at is null;

  v_token := encode(extensions.gen_random_bytes(24), 'hex');
  insert into public.invitations (tenant_id, email, role, employee_id, token_hash, invited_by, expires_at)
  values (p_tenant_id, v_email, p_role, v_employee,
          encode(extensions.digest(v_token, 'sha256'), 'hex'), auth.uid(), now() + interval '7 days');
  return v_token;
end $$;

create or replace function public.accept_invitation(p_token text) returns uuid
language plpgsql security definer set search_path = '' as $$
declare
  v_uid uuid := auth.uid();
  v_inv public.invitations;
  v_email text;
  v_display text;
begin
  if v_uid is null then raise exception 'not_authenticated' using errcode = '42501'; end if;
  select * into v_inv from public.invitations
   where token_hash = encode(extensions.digest(coalesce(p_token, ''), 'sha256'), 'hex')
   for update;
  if not found or v_inv.revoked_at is not null then raise exception 'invitation_invalid' using errcode = '22023'; end if;
  if v_inv.accepted_at is not null then raise exception 'invitation_already_used' using errcode = '22023'; end if;
  if v_inv.expires_at < now() then raise exception 'invitation_expired' using errcode = '22023'; end if;

  select lower(email) into v_email from auth.users where id = v_uid;
  if v_email is distinct from lower(v_inv.email) then
    raise exception 'invitation_email_mismatch' using errcode = '42501';
  end if;
  if exists (select 1 from public.memberships where tenant_id = v_inv.tenant_id and user_id = v_uid) then
    raise exception 'already_member' using errcode = '23505';
  end if;

  perform set_config('app.audit_reason', 'accept_invitation', true);
  insert into public.memberships (tenant_id, user_id, role) values (v_inv.tenant_id, v_uid, v_inv.role::public.user_role);

  if v_inv.employee_id is not null then
    update public.employees set user_id = v_uid, status = 'active'
     where id = v_inv.employee_id and tenant_id = v_inv.tenant_id and user_id is null;
  end if;
  if not exists (select 1 from public.employees where tenant_id = v_inv.tenant_id and user_id = v_uid) then
    select coalesce(nullif(trim(full_name), ''), split_part(v_email, '@', 1)) into v_display
      from public.profiles where id = v_uid;
    insert into public.employees (tenant_id, user_id, display_name, email)
    values (v_inv.tenant_id, v_uid, coalesce(v_display, split_part(v_email, '@', 1)), v_email);
  end if;

  update public.invitations set accepted_at = now(), accepted_by = v_uid where id = v_inv.id;
  return v_inv.tenant_id;
end $$;

create or replace function public.revoke_invitation(p_invitation_id uuid) returns void
language plpgsql security definer set search_path = '' as $$
declare v_tenant uuid;
begin
  select tenant_id into v_tenant from public.invitations where id = p_invitation_id;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(v_tenant);
  perform set_config('app.audit_reason', 'revoke_invitation', true);
  update public.invitations set revoked_at = now() where id = p_invitation_id and accepted_at is null;
end $$;

create or replace function private.owner_count(p_tenant_id uuid) returns integer
language sql stable security definer set search_path = '' as $$
  select count(*)::int from public.memberships where tenant_id = p_tenant_id and role = 'owner'
$$;

create or replace function public.set_member_role(p_tenant_id uuid, p_user_id uuid, p_role text) returns void
language plpgsql security definer set search_path = '' as $$
declare v_current text;
begin
  if auth.uid() is null then raise exception 'not_authenticated' using errcode = '42501'; end if;
  if not public.is_owner(p_tenant_id) then raise exception 'only_owner_can_change_roles' using errcode = '42501'; end if;
  if p_role not in ('owner','admin','crew') then raise exception 'invalid_role' using errcode = '22023'; end if;
  select role into v_current from public.memberships where tenant_id = p_tenant_id and user_id = p_user_id for update;
  if not found then raise exception 'not_a_member' using errcode = 'P0002'; end if;
  if v_current = 'owner' and p_role <> 'owner' and private.owner_count(p_tenant_id) <= 1 then
    raise exception 'cannot_remove_last_owner' using errcode = '22023';
  end if;
  perform set_config('app.audit_reason', 'set_member_role', true);
  update public.memberships set role = p_role::public.user_role where tenant_id = p_tenant_id and user_id = p_user_id;
end $$;

-- Removes login access. The employee record and all time history stay.
create or replace function public.remove_member(p_tenant_id uuid, p_user_id uuid) returns void
language plpgsql security definer set search_path = '' as $$
declare v_role text;
begin
  perform private.require_manager(p_tenant_id);
  select role into v_role from public.memberships where tenant_id = p_tenant_id and user_id = p_user_id for update;
  if not found then raise exception 'not_a_member' using errcode = 'P0002'; end if;
  if v_role in ('owner','admin') and not public.is_owner(p_tenant_id) then
    raise exception 'only_owner_can_remove_managers' using errcode = '42501';
  end if;
  if v_role = 'owner' and private.owner_count(p_tenant_id) <= 1 then
    raise exception 'cannot_remove_last_owner' using errcode = '22023';
  end if;
  perform set_config('app.audit_reason', 'remove_member', true);
  update public.employees set status = 'inactive' where tenant_id = p_tenant_id and user_id = p_user_id;
  delete from public.memberships where tenant_id = p_tenant_id and user_id = p_user_id;
end $$;

-- What the signed-in user can access (drives the company switcher).
create or replace function public.my_companies()
returns table (tenant_id uuid, name text, role text, employee_id uuid, timezone text)
language sql stable security invoker set search_path = '' as $$
  select t.id, t.name, m.role::text, e.id, t.timezone
  from public.memberships m
  join public.tenants t on t.id = m.tenant_id
  left join public.employees e on e.tenant_id = m.tenant_id and e.user_id = m.user_id
  where m.user_id = (select auth.uid())
  order by t.name
$$;
