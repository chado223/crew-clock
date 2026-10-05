-- Phase 1 foundation: tenancy, people, security, audit.
--
-- Safe on the existing production database:
--   * no table or column is dropped, no row is deleted
--   * existing policies are replaced (policies hold no data)
--   * new constraints on existing data are added NOT VALID, then validated;
--     if legacy data fails validation, a WARNING is raised and the constraint
--     still protects all new writes
-- See docs/decisions/0001 and 0002.

create extension if not exists pgcrypto with schema extensions;
create extension if not exists btree_gist with schema extensions;

create schema if not exists private;          -- internal functions, not exposed through the API
revoke all on schema private from public;
grant usage on schema private to authenticated, service_role;

-- ---------------------------------------------------------------------------
-- Tenants
-- ---------------------------------------------------------------------------
alter table public.tenants
  add column if not exists timezone text not null default 'America/New_York',
  add column if not exists week_start_day smallint not null default 1,          -- ISO: 1 = Monday
  add column if not exists overtime_weekly_hours numeric(5,2) not null default 40,
  add column if not exists status text not null default 'active',
  add column if not exists created_by uuid references auth.users (id) on delete set null,
  add column if not exists updated_at timestamptz not null default now();

do $$ begin
  alter table public.tenants add constraint tenants_week_start_day_chk check (week_start_day between 1 and 7);
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.tenants add constraint tenants_status_chk check (status in ('active','suspended','closed'));
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.tenants add constraint tenants_overtime_chk check (overtime_weekly_hours > 0 and overtime_weekly_hours <= 168);
exception when duplicate_object then null; end $$;

create or replace function private.validate_tenant_timezone() returns trigger
language plpgsql set search_path = '' as $$
begin
  if not exists (select 1 from pg_catalog.pg_timezone_names where name = new.timezone) then
    raise exception 'invalid_timezone: %', new.timezone using errcode = '22023';
  end if;
  return new;
end $$;
drop trigger if exists tenants_validate_tz on public.tenants;
create trigger tenants_validate_tz before insert or update of timezone on public.tenants
  for each row execute function private.validate_tenant_timezone();

-- ---------------------------------------------------------------------------
-- Memberships (login access to a company)
-- ---------------------------------------------------------------------------
do $$ begin
  alter table public.memberships add constraint memberships_role_chk check (role in ('owner','admin','crew')) not valid;
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.memberships validate constraint memberships_role_chk;
exception when check_violation then
  raise warning 'memberships contain roles outside owner/admin/crew; constraint enforced for new rows only';
end $$;
create index if not exists memberships_user_idx on public.memberships (user_id);

-- ---------------------------------------------------------------------------
-- Profiles
-- ---------------------------------------------------------------------------
alter table public.profiles add column if not exists updated_at timestamptz not null default now();

-- Create a profile for every new auth user. Named to run after any existing
-- trigger, and ON CONFLICT so it never breaks sign-up if one already exists.
create or replace function private.handle_new_user() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  insert into public.profiles (id, full_name)
  values (new.id, nullif(trim(coalesce(new.raw_user_meta_data ->> 'full_name', '')), ''))
  on conflict (id) do nothing;
  return new;
end $$;
drop trigger if exists zz_cc_on_auth_user_created on auth.users;
create trigger zz_cc_on_auth_user_created after insert on auth.users
  for each row execute function private.handle_new_user();

insert into public.profiles (id, full_name)
select u.id, nullif(trim(coalesce(u.raw_user_meta_data ->> 'full_name', '')), '')
from auth.users u
on conflict (id) do nothing;

-- ---------------------------------------------------------------------------
-- Parent keys for tenant-safe foreign keys: (tenant_id, id)
-- ---------------------------------------------------------------------------
do $$
declare t text;
begin
  foreach t in array array['clients','jobs','time_entries','invoices','expenses'] loop
    begin
      execute format('alter table public.%I add constraint %I unique (tenant_id, id)', t, t || '_tenant_id_id_key');
    exception when duplicate_table or duplicate_object then null;
    end;
  end loop;
end $$;

-- ---------------------------------------------------------------------------
-- Employees: a person who works for a company. May or may not have a login.
-- ---------------------------------------------------------------------------
create table if not exists public.employees (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  user_id uuid references auth.users (id) on delete set null,
  display_name text not null check (length(trim(display_name)) between 1 and 120),
  email text,
  phone text,
  status text not null default 'active' check (status in ('active','inactive')),
  hired_on date,
  notes text,
  legacy_names text[] not null default '{}',    -- names used in the old Crew Clock, for history matching
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, id)
);
create unique index if not exists employees_tenant_user_uidx on public.employees (tenant_id, user_id) where user_id is not null;
create index if not exists employees_tenant_status_idx on public.employees (tenant_id, status);

-- Pay is kept apart from the employee row so crew can see coworkers' names
-- without seeing anyone's pay. Rows are history: a raise is a new row.
create table if not exists public.employee_pay_rates (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  employee_id uuid not null,
  hourly_rate numeric(10,2) not null check (hourly_rate >= 0),
  effective_from date not null,
  created_by uuid references auth.users (id) on delete set null,
  created_at timestamptz not null default now(),
  foreign key (tenant_id, employee_id) references public.employees (tenant_id, id),
  unique (employee_id, effective_from)
);

-- ---------------------------------------------------------------------------
-- Crews
-- ---------------------------------------------------------------------------
create table if not exists public.crews (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  name text not null check (length(trim(name)) between 1 and 80),
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, id)
);
create table if not exists public.crew_members (
  tenant_id uuid not null,
  crew_id uuid not null,
  employee_id uuid not null,
  is_lead boolean not null default false,
  created_at timestamptz not null default now(),
  primary key (crew_id, employee_id),
  foreign key (tenant_id, crew_id) references public.crews (tenant_id, id) on delete cascade,
  foreign key (tenant_id, employee_id) references public.employees (tenant_id, id)
);

-- ---------------------------------------------------------------------------
-- Clients (CRM hub) and properties (a client can have many)
-- ---------------------------------------------------------------------------
alter table public.clients add column if not exists updated_at timestamptz not null default now();
create index if not exists clients_tenant_created_idx on public.clients (tenant_id, created_at desc);

create table if not exists public.properties (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  client_id uuid not null,
  label text,
  address_line1 text not null,
  address_line2 text,
  city text,
  region text,
  postal_code text,
  country text not null default 'US',
  latitude double precision check (latitude between -90 and 90),
  longitude double precision check (longitude between -180 and 180),
  access_notes text,
  gate_code text,
  lawn_sqft integer check (lawn_sqft >= 0),
  notes text,
  status text not null default 'active' check (status in ('active','inactive')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, id),
  foreign key (tenant_id, client_id) references public.clients (tenant_id, id)
);
create index if not exists properties_tenant_client_idx on public.properties (tenant_id, client_id);

-- ---------------------------------------------------------------------------
-- Jobs, invoices, expenses: links + tenant-safe FKs
-- ---------------------------------------------------------------------------
alter table public.jobs
  add column if not exists property_id uuid,
  add column if not exists updated_at timestamptz not null default now();
alter table public.invoices add column if not exists updated_at timestamptz not null default now();
alter table public.expenses
  add column if not exists job_id uuid,
  add column if not exists created_by uuid references auth.users (id) on delete set null,
  add column if not exists created_at timestamptz not null default now();

create index if not exists jobs_tenant_status_idx on public.jobs (tenant_id, status);
create index if not exists invoices_tenant_status_idx on public.invoices (tenant_id, status);
create index if not exists expenses_tenant_spent_idx on public.expenses (tenant_id, spent_at desc);

-- Composite FKs: a child row can only reference a parent in the SAME company.
do $$
declare
  fk record;
begin
  for fk in
    select * from (values
      ('jobs',     'jobs_tenant_client_fk',    '(tenant_id, client_id)',   'clients (tenant_id, id)'),
      ('jobs',     'jobs_tenant_property_fk',  '(tenant_id, property_id)', 'properties (tenant_id, id)'),
      ('jobs',     'jobs_tenant_crew_fk',      '(tenant_id, crew_id)',     'crews (tenant_id, id)'),
      ('invoices', 'invoices_tenant_client_fk','(tenant_id, client_id)',   'clients (tenant_id, id)'),
      ('expenses', 'expenses_tenant_job_fk',   '(tenant_id, job_id)',      'jobs (tenant_id, id)'),
      ('time_entries','time_entries_tenant_job_fk','(tenant_id, job_id)',  'jobs (tenant_id, id)')
    ) v(tbl, name, cols, ref)
  loop
    begin
      execute format('alter table public.%I add constraint %I foreign key %s references public.%s not valid',
                     fk.tbl, fk.name, fk.cols, fk.ref);
    exception when duplicate_object then null;
    end;
    begin
      execute format('alter table public.%I validate constraint %I', fk.tbl, fk.name);
    exception when foreign_key_violation then
      raise warning 'Existing rows in % reference another company or a missing row (%). New writes are blocked; existing rows need review.', fk.tbl, fk.name;
    end;
  end loop;
end $$;

-- ---------------------------------------------------------------------------
-- Invitations (self-serve onboarding; functions in a later migration)
-- ---------------------------------------------------------------------------
create table if not exists public.invitations (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  email text not null,
  role text not null check (role in ('owner','admin','crew')),
  employee_id uuid,
  token_hash text not null unique,
  invited_by uuid references auth.users (id) on delete set null,
  expires_at timestamptz not null,
  accepted_at timestamptz,
  accepted_by uuid references auth.users (id) on delete set null,
  revoked_at timestamptz,
  created_at timestamptz not null default now(),
  foreign key (tenant_id, employee_id) references public.employees (tenant_id, id)
);
create index if not exists invitations_tenant_idx on public.invitations (tenant_id, created_at desc);
create unique index if not exists invitations_one_pending_uidx on public.invitations (tenant_id, lower(email))
  where accepted_at is null and revoked_at is null;

-- ---------------------------------------------------------------------------
-- Audit log (append-only) and customer activity timeline (CRM)
-- ---------------------------------------------------------------------------
create table if not exists public.audit_log (
  id bigint generated always as identity primary key,
  tenant_id uuid,                     -- no FK: audit history outlives what it describes
  actor_user_id uuid,
  action text not null,               -- insert | update | delete | <function name>
  entity_type text not null,
  entity_id text,
  before jsonb,
  after jsonb,
  reason text,
  created_at timestamptz not null default now()
);
create index if not exists audit_log_tenant_created_idx on public.audit_log (tenant_id, created_at desc);
create index if not exists audit_log_entity_idx on public.audit_log (tenant_id, entity_type, entity_id);

create table if not exists public.activity (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  client_id uuid,
  property_id uuid,
  kind text not null,                 -- note | call | email | sms | job_completed | invoice_sent | payment_received | ...
  summary text not null,
  data jsonb not null default '{}'::jsonb,
  actor_user_id uuid references auth.users (id) on delete set null,
  occurred_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  foreign key (tenant_id, client_id) references public.clients (tenant_id, id),
  foreign key (tenant_id, property_id) references public.properties (tenant_id, id)
);
create index if not exists activity_client_timeline_idx on public.activity (tenant_id, client_id, occurred_at desc);

-- ---------------------------------------------------------------------------
-- Generic triggers
-- ---------------------------------------------------------------------------
create or replace function private.set_updated_at() returns trigger
language plpgsql set search_path = '' as $$
begin new.updated_at := now(); return new; end $$;

-- A row can never move to another company.
create or replace function private.prevent_tenant_change() returns trigger
language plpgsql set search_path = '' as $$
begin
  if new.tenant_id is distinct from old.tenant_id then
    raise exception 'tenant_id_immutable' using errcode = '42501';
  end if;
  return new;
end $$;

-- Writes before/after to audit_log. Reason comes from the calling function via
-- set_config('app.audit_reason', ..., true).
create or replace function private.audit_row_change() returns trigger
language plpgsql security definer set search_path = '' as $$
declare
  v_old jsonb := case when tg_op in ('UPDATE','DELETE') then to_jsonb(old) end;
  v_new jsonb := case when tg_op in ('INSERT','UPDATE') then to_jsonb(new) end;
  v_row jsonb := coalesce(v_new, v_old);
  v_tenant uuid;
  v_entity text;
begin
  if tg_op = 'UPDATE' and (v_old - 'updated_at') = (v_new - 'updated_at') then
    return new;
  end if;
  v_tenant := case when tg_table_name = 'tenants' then (v_row ->> 'id')::uuid else (v_row ->> 'tenant_id')::uuid end;
  v_entity := coalesce(v_row ->> 'id', v_row ->> 'user_id');
  insert into public.audit_log (tenant_id, actor_user_id, action, entity_type, entity_id, before, after, reason)
  values (v_tenant, auth.uid(), lower(tg_op), tg_table_name, v_entity, v_old, v_new,
          nullif(current_setting('app.audit_reason', true), ''));
  return coalesce(new, old);
end $$;

do $$
declare t text;
begin
  foreach t in array array['tenants','profiles','employees','crews','clients','properties','jobs','invoices'] loop
    execute format('drop trigger if exists set_updated_at on public.%I', t);
    execute format('create trigger set_updated_at before update on public.%I for each row execute function private.set_updated_at()', t);
  end loop;

  foreach t in array array['memberships','employees','employee_pay_rates','crews','crew_members','clients','properties',
                           'jobs','time_entries','invoices','expenses','invitations','activity'] loop
    execute format('drop trigger if exists prevent_tenant_change on public.%I', t);
    execute format('create trigger prevent_tenant_change before update of tenant_id on public.%I for each row execute function private.prevent_tenant_change()', t);
  end loop;

  foreach t in array array['tenants','memberships','employees','employee_pay_rates','clients','properties',
                           'jobs','time_entries','invoices','expenses','invitations'] loop
    execute format('drop trigger if exists audit_row_change on public.%I', t);
    execute format('create trigger audit_row_change after insert or update or delete on public.%I for each row execute function private.audit_row_change()', t);
  end loop;
end $$;

-- ---------------------------------------------------------------------------
-- Backfill: every member gets an employee record (non-destructive)
-- ---------------------------------------------------------------------------
insert into public.employees (tenant_id, user_id, display_name, email)
select m.tenant_id, m.user_id,
       coalesce(nullif(trim(p.full_name), ''), split_part(u.email, '@', 1), 'Team member'),
       u.email
from public.memberships m
join auth.users u on u.id = m.user_id
left join public.profiles p on p.id = m.user_id
where not exists (select 1 from public.employees e where e.tenant_id = m.tenant_id and e.user_id = m.user_id);

-- Time entries belong to an employee (the person), not a login.
alter table public.time_entries add column if not exists employee_id uuid;
alter table public.time_entries alter column user_id drop not null;
update public.time_entries te
set employee_id = e.id
from public.employees e
where te.employee_id is null and e.tenant_id = te.tenant_id and e.user_id = te.user_id;
do $$ begin
  alter table public.time_entries add constraint time_entries_tenant_employee_fk
    foreign key (tenant_id, employee_id) references public.employees (tenant_id, id);
exception when duplicate_object then null; end $$;
create index if not exists time_entries_tenant_clock_in_idx on public.time_entries (tenant_id, clock_in);
create index if not exists time_entries_employee_clock_in_idx on public.time_entries (employee_id, clock_in);

-- employees.user_id must be a member of that company
do $$ begin
  alter table public.employees add constraint employees_membership_fk
    foreign key (tenant_id, user_id) references public.memberships (tenant_id, user_id)
    on delete set null (user_id);
exception when duplicate_object then null; end $$;

-- ---------------------------------------------------------------------------
-- Authorization helpers. SECURITY DEFINER + fixed search_path so they can read
-- memberships without recursing through memberships' own RLS.
-- ---------------------------------------------------------------------------
create or replace function public.in_tenant(tid uuid) returns boolean
language sql stable security definer set search_path = '' as $$
  select exists (select 1 from public.memberships m where m.tenant_id = tid and m.user_id = (select auth.uid()))
$$;

create or replace function public.is_admin_or_owner(tid uuid) returns boolean
language sql stable security definer set search_path = '' as $$
  select exists (select 1 from public.memberships m
                 where m.tenant_id = tid and m.user_id = (select auth.uid()) and m.role in ('owner','admin'))
$$;

create or replace function public.is_owner(tid uuid) returns boolean
language sql stable security definer set search_path = '' as $$
  select exists (select 1 from public.memberships m
                 where m.tenant_id = tid and m.user_id = (select auth.uid()) and m.role = 'owner')
$$;

create or replace function public.my_employee_id(tid uuid) returns uuid
language sql stable security definer set search_path = '' as $$
  select e.id from public.employees e
  where e.tenant_id = tid and e.user_id = (select auth.uid()) and e.status = 'active'
$$;

create or replace function private.shares_tenant_with(other uuid) returns boolean
language sql stable security definer set search_path = '' as $$
  select exists (select 1 from public.memberships a join public.memberships b on a.tenant_id = b.tenant_id
                 where a.user_id = (select auth.uid()) and b.user_id = other)
$$;

-- ---------------------------------------------------------------------------
-- Row Level Security: replace every existing policy on managed tables
-- ---------------------------------------------------------------------------
do $$
declare
  t text;
  p record;
begin
  foreach t in array array['tenants','profiles','memberships','employees','employee_pay_rates','crews','crew_members',
                           'clients','properties','jobs','time_entries','invoices','expenses','invitations',
                           'audit_log','activity'] loop
    execute format('alter table public.%I enable row level security', t);
    for p in select policyname from pg_policies where schemaname = 'public' and tablename = t loop
      execute format('drop policy %I on public.%I', p.policyname, t);
    end loop;
  end loop;
end $$;

-- tenants
create policy tenants_select on public.tenants for select to authenticated using (public.in_tenant(id));
create policy tenants_update on public.tenants for update to authenticated
  using (public.is_owner(id)) with check (public.is_owner(id));

-- profiles
create policy profiles_select on public.profiles for select to authenticated
  using (id = (select auth.uid()) or private.shares_tenant_with(id));
create policy profiles_insert on public.profiles for insert to authenticated with check (id = (select auth.uid()));
create policy profiles_update on public.profiles for update to authenticated
  using (id = (select auth.uid())) with check (id = (select auth.uid()));

-- memberships: read your company's roster; changes only through functions
create policy memberships_select on public.memberships for select to authenticated using (public.in_tenant(tenant_id));

-- employees: everyone in the company sees names; owners/admins manage
create policy employees_select on public.employees for select to authenticated using (public.in_tenant(tenant_id));
create policy employees_insert on public.employees for insert to authenticated with check (public.is_admin_or_owner(tenant_id));
create policy employees_update on public.employees for update to authenticated
  using (public.is_admin_or_owner(tenant_id)) with check (public.is_admin_or_owner(tenant_id));

-- pay rates: owners/admins only; history is insert-only
create policy pay_select on public.employee_pay_rates for select to authenticated using (public.is_admin_or_owner(tenant_id));
create policy pay_insert on public.employee_pay_rates for insert to authenticated with check (public.is_admin_or_owner(tenant_id));

-- operational tables: members read, owners/admins write
do $$
declare t text;
begin
  foreach t in array array['crews','crew_members','clients','properties','jobs'] loop
    execute format('create policy %I on public.%I for select to authenticated using (public.in_tenant(tenant_id))', t || '_select', t);
    execute format('create policy %I on public.%I for insert to authenticated with check (public.is_admin_or_owner(tenant_id))', t || '_insert', t);
    execute format('create policy %I on public.%I for update to authenticated using (public.is_admin_or_owner(tenant_id)) with check (public.is_admin_or_owner(tenant_id))', t || '_update', t);
    execute format('create policy %I on public.%I for delete to authenticated using (public.is_admin_or_owner(tenant_id))', t || '_delete', t);
  end loop;
  -- financial tables: owners/admins only, every operation
  foreach t in array array['invoices','expenses'] loop
    execute format('create policy %I on public.%I for select to authenticated using (public.is_admin_or_owner(tenant_id))', t || '_select', t);
    execute format('create policy %I on public.%I for insert to authenticated with check (public.is_admin_or_owner(tenant_id))', t || '_insert', t);
    execute format('create policy %I on public.%I for update to authenticated using (public.is_admin_or_owner(tenant_id)) with check (public.is_admin_or_owner(tenant_id))', t || '_update', t);
    execute format('create policy %I on public.%I for delete to authenticated using (public.is_admin_or_owner(tenant_id))', t || '_delete', t);
  end loop;
end $$;

-- time: owners/admins see the company; crew see their own. Writes via functions only.
create policy time_entries_select on public.time_entries for select to authenticated
  using (public.is_admin_or_owner(tenant_id) or employee_id = public.my_employee_id(tenant_id));

create policy invitations_select on public.invitations for select to authenticated using (public.is_admin_or_owner(tenant_id));
create policy audit_log_select on public.audit_log for select to authenticated using (public.is_admin_or_owner(tenant_id));
create policy activity_select on public.activity for select to authenticated using (public.is_admin_or_owner(tenant_id));
create policy activity_insert on public.activity for insert to authenticated with check (public.is_admin_or_owner(tenant_id));
