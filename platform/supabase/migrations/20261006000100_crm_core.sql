-- CRM core: the client record becomes the hub (ADR 0001).
-- Additive only: new columns with defaults, no data changed.
--
-- Migrations after 20261005000900_privileges carry their own grants.
-- tests/40_security_posture.sql fails if any table is left open.

alter table public.clients
  add column if not exists kind text not null default 'residential',
  add column if not exists company_name text,
  add column if not exists status text not null default 'active',
  add column if not exists lead_source text,
  add column if not exists tags text[] not null default '{}',
  add column if not exists assigned_employee_id uuid,
  add column if not exists preferred_contact text,
  add column if not exists internal_notes text;

do $$ begin
  alter table public.clients add constraint clients_kind_chk check (kind in ('residential','commercial'));
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.clients add constraint clients_status_chk check (status in ('lead','active','inactive','lost'));
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.clients add constraint clients_preferred_contact_chk check (preferred_contact in ('call','text','email'));
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.clients add constraint clients_assigned_employee_fk
    foreign key (tenant_id, assigned_employee_id) references public.employees (tenant_id, id);
exception when duplicate_object then null; end $$;

create index if not exists clients_tenant_status_idx on public.clients (tenant_id, status);
create index if not exists clients_tenant_name_idx on public.clients (tenant_id, lower(name));
create index if not exists properties_tenant_status_idx on public.properties (tenant_id, status);

-- Activity kinds are open-ended text, but must be non-empty and short.
do $$ begin
  alter table public.activity add constraint activity_kind_chk check (kind ~ '^[a-z_]{2,40}$');
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.activity add constraint activity_summary_chk check (length(trim(summary)) between 1 and 2000);
exception when duplicate_object then null; end $$;

-- Timeline entries are written by the system (triggers) or through add_client_note().
create or replace function private.log_activity(
  p_tenant uuid, p_client uuid, p_property uuid, p_kind text, p_summary text, p_data jsonb default '{}'::jsonb
) returns void
language plpgsql security definer set search_path = '' as $$
begin
  insert into public.activity (tenant_id, client_id, property_id, kind, summary, data, actor_user_id)
  values (p_tenant, p_client, p_property, p_kind, p_summary, coalesce(p_data, '{}'::jsonb), auth.uid());
end $$;

create or replace function private.client_activity() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  if tg_op = 'INSERT' then
    perform private.log_activity(new.tenant_id, new.id, null,
      case when new.status = 'lead' then 'lead_created' else 'client_created' end,
      case when new.status = 'lead' then 'Added as a lead' else 'Added as a customer' end);
  elsif new.status is distinct from old.status then
    perform private.log_activity(new.tenant_id, new.id, null, 'status_changed',
      format('Status changed from %s to %s', old.status, new.status),
      jsonb_build_object('from', old.status, 'to', new.status));
  end if;
  return new;
end $$;
drop trigger if exists client_activity on public.clients;
create trigger client_activity after insert or update of status on public.clients
  for each row execute function private.client_activity();

create or replace function private.property_activity() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  perform private.log_activity(new.tenant_id, new.client_id, new.id, 'property_added',
    format('Property added: %s', new.address_line1));
  return new;
end $$;
drop trigger if exists property_activity on public.properties;
create trigger property_activity after insert on public.properties
  for each row execute function private.property_activity();

-- Notes and logged interactions from the office.
create or replace function public.add_client_note(
  p_client_id uuid, p_kind text, p_summary text, p_property_id uuid default null
) returns public.activity
language plpgsql security definer set search_path = '' as $$
declare
  v_tenant uuid;
  v_row public.activity;
begin
  select tenant_id into v_tenant from public.clients where id = p_client_id;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(v_tenant);
  if p_kind not in ('note','call','email','sms','meeting','complaint') then
    raise exception 'invalid_activity_kind' using errcode = '22023';
  end if;
  if p_summary is null or length(trim(p_summary)) = 0 then
    raise exception 'summary_required' using errcode = '22023';
  end if;
  insert into public.activity (tenant_id, client_id, property_id, kind, summary, actor_user_id)
  values (v_tenant, p_client_id, p_property_id, p_kind, trim(p_summary), auth.uid())
  returning * into v_row;
  return v_row;
end $$;

-- Direct inserts into activity are replaced by add_client_note().
drop policy if exists activity_insert on public.activity;
revoke insert on public.activity from authenticated;

revoke execute on function private.log_activity(uuid, uuid, uuid, text, text, jsonb) from public, anon, authenticated;
revoke execute on function public.add_client_note(uuid, text, text, uuid) from public, anon;
grant execute on function public.add_client_note(uuid, text, text, uuid) to authenticated;
revoke execute on function private.client_activity() from public, anon, authenticated;
revoke execute on function private.property_activity() from public, anon, authenticated;
