-- Field operations: services, jobs (one-off and recurring), visits, assignments.
-- One integrated model (ADR 0001):
--   client -> property -> job -> visit -> (time entries, completion, history)
-- Additive only. Existing jobs rows are kept; unknown statuses are tolerated.

-- ---------------------------------------------------------------------------
-- Services catalog (shared vocabulary for jobs, estimates, invoices)
-- ---------------------------------------------------------------------------
create table if not exists public.services (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  name text not null check (length(trim(name)) between 1 and 120),
  default_price numeric(10,2) check (default_price >= 0),
  default_minutes integer check (default_minutes between 1 and 1440),
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, id)
);
create unique index if not exists services_tenant_name_uidx on public.services (tenant_id, lower(name));

-- ---------------------------------------------------------------------------
-- Jobs: the agreement to do work at a property, once or on a schedule
-- ---------------------------------------------------------------------------
alter table public.jobs
  add column if not exists kind text not null default 'one_off',
  add column if not exists service_id uuid,
  add column if not exists price numeric(10,2),
  add column if not exists est_minutes integer,
  add column if not exists notes text,
  add column if not exists starts_on date,
  add column if not exists ends_on date,
  add column if not exists interval_weeks smallint,
  add column if not exists weekday smallint;

do $$ begin
  alter table public.jobs add constraint jobs_kind_chk check (kind in ('one_off','recurring'));
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.jobs add constraint jobs_recurrence_chk check (
    kind = 'one_off'
    or (interval_weeks between 1 and 12 and weekday between 1 and 7 and starts_on is not null
        and (ends_on is null or ends_on >= starts_on)));
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.jobs add constraint jobs_price_chk check (price is null or price >= 0);
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.jobs add constraint jobs_est_minutes_chk check (est_minutes is null or est_minutes between 1 and 1440);
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.jobs add constraint jobs_status_chk
    check (status in ('scheduled','active','paused','completed','canceled')) not valid;
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.jobs validate constraint jobs_status_chk;
exception when check_violation then
  raise warning 'Some existing jobs have a status outside the new set; enforced for new writes only.';
end $$;
do $$ begin
  alter table public.jobs add constraint jobs_tenant_service_fk
    foreign key (tenant_id, service_id) references public.services (tenant_id, id);
exception when duplicate_object then null; end $$;

create index if not exists jobs_tenant_property_idx on public.jobs (tenant_id, property_id);
create index if not exists jobs_tenant_client_idx on public.jobs (tenant_id, client_id);

-- ---------------------------------------------------------------------------
-- Visits: one dated occurrence of a job; what crews actually work
-- ---------------------------------------------------------------------------
create table if not exists public.visits (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  job_id uuid not null,
  client_id uuid,
  property_id uuid,
  scheduled_date date not null,
  generated_for date,                         -- set when created from a recurring job; makes generation idempotent
  crew_id uuid,
  sort_order integer not null default 0,
  status text not null default 'scheduled'
    check (status in ('scheduled','in_progress','completed','skipped','canceled')),
  status_reason text,
  price numeric(10,2) check (price is null or price >= 0),
  est_minutes integer check (est_minutes is null or est_minutes between 1 and 1440),
  started_at timestamptz,
  completed_at timestamptz,
  completed_by uuid references auth.users (id) on delete set null,
  completion_notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, id),
  foreign key (tenant_id, job_id) references public.jobs (tenant_id, id),
  foreign key (tenant_id, client_id) references public.clients (tenant_id, id),
  foreign key (tenant_id, property_id) references public.properties (tenant_id, id),
  foreign key (tenant_id, crew_id) references public.crews (tenant_id, id),
  check (status <> 'completed' or completed_at is not null)
);
create unique index if not exists visits_job_generated_uidx on public.visits (job_id, generated_for) where generated_for is not null;
create index if not exists visits_tenant_date_idx on public.visits (tenant_id, scheduled_date);
create index if not exists visits_tenant_crew_date_idx on public.visits (tenant_id, crew_id, scheduled_date);
create index if not exists visits_job_idx on public.visits (job_id);

-- People on a visit beyond (or instead of) a whole crew.
create table if not exists public.visit_assignments (
  tenant_id uuid not null,
  visit_id uuid not null,
  employee_id uuid not null,
  created_at timestamptz not null default now(),
  primary key (visit_id, employee_id),
  foreign key (tenant_id, visit_id) references public.visits (tenant_id, id) on delete cascade,
  foreign key (tenant_id, employee_id) references public.employees (tenant_id, id)
);
create index if not exists visit_assignments_employee_idx on public.visit_assignments (employee_id);

-- Time worked can be tied to a visit (job costing later).
alter table public.time_entries add column if not exists visit_id uuid;
do $$ begin
  alter table public.time_entries add constraint time_entries_tenant_visit_fk
    foreign key (tenant_id, visit_id) references public.visits (tenant_id, id);
exception when duplicate_object then null; end $$;

-- ---------------------------------------------------------------------------
-- Generic triggers on the new tables
-- ---------------------------------------------------------------------------
do $$
declare t text;
begin
  foreach t in array array['services','visits'] loop
    execute format('drop trigger if exists set_updated_at on public.%I', t);
    execute format('create trigger set_updated_at before update on public.%I for each row execute function private.set_updated_at()', t);
  end loop;
  foreach t in array array['services','visits','visit_assignments'] loop
    execute format('drop trigger if exists prevent_tenant_change on public.%I', t);
    execute format('create trigger prevent_tenant_change before update of tenant_id on public.%I for each row execute function private.prevent_tenant_change()', t);
    execute format('drop trigger if exists audit_row_change on public.%I', t);
    execute format('create trigger audit_row_change after insert or update or delete on public.%I for each row execute function private.audit_row_change()', t);
  end loop;
end $$;

-- A visit's customer and property always follow its job (no mismatches).
create or replace function private.visit_defaults() returns trigger
language plpgsql security definer set search_path = '' as $$
declare j public.jobs;
begin
  select * into j from public.jobs where id = new.job_id and tenant_id = new.tenant_id;
  if not found then raise exception 'job_not_found' using errcode = 'P0002'; end if;
  new.client_id := j.client_id;
  new.property_id := j.property_id;
  if tg_op = 'INSERT' then
    new.price := coalesce(new.price, j.price);
    new.est_minutes := coalesce(new.est_minutes, j.est_minutes);
    new.crew_id := coalesce(new.crew_id, j.crew_id);
  end if;
  return new;
end $$;
drop trigger if exists visit_defaults on public.visits;
create trigger visit_defaults before insert or update of job_id, client_id, property_id on public.visits
  for each row execute function private.visit_defaults();

-- ---------------------------------------------------------------------------
-- Who is assigned to a visit
-- ---------------------------------------------------------------------------
create or replace function private.is_assigned_to_visit(p_visit_id uuid) returns boolean
language sql stable security definer set search_path = '' as $$
  select exists (
    select 1
    from public.visits v
    join public.employees e on e.tenant_id = v.tenant_id and e.user_id = (select auth.uid()) and e.status = 'active'
    where v.id = p_visit_id
      and (
        exists (select 1 from public.visit_assignments a where a.visit_id = v.id and a.employee_id = e.id)
        or exists (select 1 from public.crew_members cm where cm.crew_id = v.crew_id and cm.employee_id = e.id)
      )
  )
$$;

-- ---------------------------------------------------------------------------
-- Row Level Security
-- ---------------------------------------------------------------------------
alter table public.services enable row level security;
alter table public.visits enable row level security;
alter table public.visit_assignments enable row level security;

drop policy if exists services_select on public.services;
drop policy if exists services_insert on public.services;
drop policy if exists services_update on public.services;
create policy services_select on public.services for select to authenticated using (public.in_tenant(tenant_id));
create policy services_insert on public.services for insert to authenticated with check (public.is_admin_or_owner(tenant_id));
create policy services_update on public.services for update to authenticated
  using (public.is_admin_or_owner(tenant_id)) with check (public.is_admin_or_owner(tenant_id));

-- Managers see and plan everything; crew see only visits they're on.
drop policy if exists visits_select on public.visits;
drop policy if exists visits_insert on public.visits;
drop policy if exists visits_update on public.visits;
drop policy if exists visits_delete on public.visits;
create policy visits_select on public.visits for select to authenticated
  using (public.is_admin_or_owner(tenant_id) or private.is_assigned_to_visit(id));
create policy visits_insert on public.visits for insert to authenticated with check (public.is_admin_or_owner(tenant_id));
create policy visits_update on public.visits for update to authenticated
  using (public.is_admin_or_owner(tenant_id)) with check (public.is_admin_or_owner(tenant_id));
create policy visits_delete on public.visits for delete to authenticated
  using (public.is_admin_or_owner(tenant_id) and status = 'scheduled');

drop policy if exists visit_assignments_select on public.visit_assignments;
drop policy if exists visit_assignments_insert on public.visit_assignments;
drop policy if exists visit_assignments_delete on public.visit_assignments;
create policy visit_assignments_select on public.visit_assignments for select to authenticated
  using (public.is_admin_or_owner(tenant_id) or employee_id = public.my_employee_id(tenant_id));
create policy visit_assignments_insert on public.visit_assignments for insert to authenticated with check (public.is_admin_or_owner(tenant_id));
create policy visit_assignments_delete on public.visit_assignments for delete to authenticated using (public.is_admin_or_owner(tenant_id));

-- ---------------------------------------------------------------------------
-- Scheduling functions
-- ---------------------------------------------------------------------------

-- Create visits for recurring jobs in a date range. Safe to run repeatedly.
create or replace function public.generate_visits(p_tenant_id uuid, p_from date, p_to date) returns integer
language plpgsql security definer set search_path = '' as $$
declare
  v_count integer;
begin
  if auth.uid() is not null then perform private.require_manager(p_tenant_id); end if;
  if p_to < p_from or p_to - p_from > 62 then raise exception 'invalid_date_range' using errcode = '22023'; end if;

  insert into public.visits (tenant_id, job_id, scheduled_date, generated_for)
  select j.tenant_id, j.id, d::date, d::date
  from public.jobs j
  cross join lateral (
    select j.starts_on + ((j.weekday - extract(isodow from j.starts_on)::int + 7) % 7) as first_on
  ) f
  cross join lateral generate_series(greatest(p_from, f.first_on), least(p_to, coalesce(j.ends_on, p_to)), interval '1 day') d
  where j.tenant_id = p_tenant_id
    and j.kind = 'recurring'
    and j.status in ('scheduled','active')
    and j.property_id is not null
    and extract(isodow from d) = j.weekday
    and ((d::date - f.first_on) / 7) % j.interval_weeks = 0
  on conflict (job_id, generated_for) where generated_for is not null do nothing;

  get diagnostics v_count = row_count;
  return v_count;
end $$;

create or replace function private.visit_label(v public.visits) returns text
language sql stable security definer set search_path = '' as $$
  select coalesce(j.title, 'Visit') || coalesce(' at ' || p.address_line1, '')
  from public.jobs j left join public.properties p on p.id = v.property_id
  where j.id = v.job_id
$$;

create or replace function public.reschedule_visit(
  p_visit_id uuid, p_date date, p_crew_id uuid default null, p_reason text default null
) returns public.visits
language plpgsql security definer set search_path = '' as $$
declare
  v public.visits;
begin
  select * into v from public.visits where id = p_visit_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(v.tenant_id);
  if v.status <> 'scheduled' then raise exception 'visit_not_scheduled' using errcode = '22023'; end if;
  if p_date is null then raise exception 'date_required' using errcode = '22023'; end if;

  update public.visits
     set scheduled_date = p_date,
         crew_id = coalesce(p_crew_id, crew_id),
         status_reason = nullif(trim(coalesce(p_reason, '')), '')
   where id = p_visit_id
  returning * into v;

  if v.client_id is not null then
    perform private.log_activity(v.tenant_id, v.client_id, v.property_id, 'visit_rescheduled',
      format('%s moved to %s%s', private.visit_label(v), to_char(p_date, 'Mon FMDD'),
             coalesce(' (' || v.status_reason || ')', '')),
      jsonb_build_object('visit_id', v.id, 'date', p_date));
  end if;
  return v;
end $$;

create or replace function public.start_visit(p_visit_id uuid) returns public.visits
language plpgsql security definer set search_path = '' as $$
declare v public.visits;
begin
  select * into v from public.visits where id = p_visit_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  if not (public.is_admin_or_owner(v.tenant_id) or private.is_assigned_to_visit(v.id)) then
    raise exception 'forbidden' using errcode = '42501';
  end if;
  if v.status = 'in_progress' then return v; end if;
  if v.status <> 'scheduled' then raise exception 'visit_not_scheduled' using errcode = '22023'; end if;
  update public.visits set status = 'in_progress', started_at = now() where id = p_visit_id returning * into v;
  return v;
end $$;

create or replace function public.complete_visit(p_visit_id uuid, p_notes text default null) returns public.visits
language plpgsql security definer set search_path = '' as $$
declare v public.visits;
begin
  select * into v from public.visits where id = p_visit_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  if not (public.is_admin_or_owner(v.tenant_id) or private.is_assigned_to_visit(v.id)) then
    raise exception 'forbidden' using errcode = '42501';
  end if;
  if v.status = 'completed' then return v; end if;
  if v.status not in ('scheduled','in_progress') then raise exception 'visit_not_open' using errcode = '22023'; end if;

  update public.visits
     set status = 'completed',
         started_at = coalesce(started_at, now()),
         completed_at = now(),
         completed_by = auth.uid(),
         completion_notes = nullif(trim(coalesce(p_notes, '')), '')
   where id = p_visit_id
  returning * into v;

  if v.client_id is not null then
    perform private.log_activity(v.tenant_id, v.client_id, v.property_id, 'visit_completed',
      private.visit_label(v) || ' completed' || coalesce(': ' || v.completion_notes, ''),
      jsonb_build_object('visit_id', v.id, 'date', v.scheduled_date));
  end if;
  return v;
end $$;

-- Skip (e.g. rain, customer request) or cancel. Manager only, reason required.
create or replace function public.skip_visit(p_visit_id uuid, p_reason text, p_cancel boolean default false)
returns public.visits
language plpgsql security definer set search_path = '' as $$
declare
  v public.visits;
  v_reason text := private.require_reason(p_reason);
begin
  select * into v from public.visits where id = p_visit_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(v.tenant_id);
  if v.status not in ('scheduled','in_progress') then raise exception 'visit_not_open' using errcode = '22023'; end if;
  update public.visits
     set status = case when p_cancel then 'canceled' else 'skipped' end,
         status_reason = v_reason
   where id = p_visit_id
  returning * into v;
  if v.client_id is not null then
    perform private.log_activity(v.tenant_id, v.client_id, v.property_id,
      case when p_cancel then 'visit_canceled' else 'visit_skipped' end,
      format('%s on %s %s: %s', private.visit_label(v), to_char(v.scheduled_date, 'Mon FMDD'),
             case when p_cancel then 'canceled' else 'skipped' end, v_reason),
      jsonb_build_object('visit_id', v.id));
  end if;
  return v;
end $$;

-- Replace the people assigned to a visit.
create or replace function public.assign_visit(p_visit_id uuid, p_crew_id uuid, p_employee_ids uuid[] default '{}')
returns public.visits
language plpgsql security definer set search_path = '' as $$
declare v public.visits;
begin
  select * into v from public.visits where id = p_visit_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(v.tenant_id);
  if p_crew_id is not null and not exists (select 1 from public.crews where id = p_crew_id and tenant_id = v.tenant_id) then
    raise exception 'crew_not_found' using errcode = 'P0002';
  end if;
  if exists (select 1 from unnest(coalesce(p_employee_ids, '{}')) e(id)
             where not exists (select 1 from public.employees x where x.id = e.id and x.tenant_id = v.tenant_id)) then
    raise exception 'employee_not_found' using errcode = 'P0002';
  end if;
  update public.visits set crew_id = p_crew_id where id = p_visit_id returning * into v;
  delete from public.visit_assignments where visit_id = p_visit_id;
  insert into public.visit_assignments (tenant_id, visit_id, employee_id)
  select v.tenant_id, v.id, e.id from unnest(coalesce(p_employee_ids, '{}')) e(id) on conflict do nothing;
  return v;
end $$;

-- One read model for the board, the crew app and reports. SECURITY INVOKER:
-- managers get the whole company, crew get only their own visits.
create or replace function public.schedule(p_tenant_id uuid, p_from date, p_to date)
returns table (
  visit_id uuid,
  scheduled_date date,
  status text,
  status_reason text,
  sort_order integer,
  job_id uuid,
  job_title text,
  job_kind text,
  client_id uuid,
  client_name text,
  client_phone text,
  property_id uuid,
  address text,
  access_notes text,
  latitude double precision,
  longitude double precision,
  crew_id uuid,
  crew_name text,
  assignees text[],
  est_minutes integer,
  price numeric,
  started_at timestamptz,
  completed_at timestamptz,
  completion_notes text
)
language sql stable security invoker set search_path = '' as $$
  select v.id, v.scheduled_date, v.status, v.status_reason, v.sort_order,
         j.id, j.title, j.kind,
         c.id, c.name, c.phone,
         p.id, concat_ws(', ', p.address_line1, p.city), p.access_notes, p.latitude, p.longitude,
         cr.id, cr.name,
         coalesce((select array_agg(e.display_name order by e.display_name)
                   from public.visit_assignments a join public.employees e on e.id = a.employee_id
                   where a.visit_id = v.id), '{}'),
         v.est_minutes, v.price, v.started_at, v.completed_at, v.completion_notes
  from public.visits v
  join public.jobs j on j.id = v.job_id
  left join public.clients c on c.id = v.client_id
  left join public.properties p on p.id = v.property_id
  left join public.crews cr on cr.id = v.crew_id
  where v.tenant_id = p_tenant_id and v.scheduled_date between p_from and p_to
  order by v.scheduled_date, cr.name nulls last, v.sort_order, j.title
$$;

-- ---------------------------------------------------------------------------
-- Privileges for this migration's objects
-- ---------------------------------------------------------------------------
revoke all on public.services, public.visits, public.visit_assignments from anon, authenticated;
grant select, insert, update on public.services to authenticated;
grant select, insert, update, delete on public.visits to authenticated;
grant select, insert, delete on public.visit_assignments to authenticated;
grant all on public.services, public.visits, public.visit_assignments to service_role;

revoke execute on function private.visit_defaults(), private.is_assigned_to_visit(uuid), private.visit_label(public.visits)
  from public, anon, authenticated;
grant execute on function private.is_assigned_to_visit(uuid) to authenticated;  -- used in RLS

revoke execute on function public.generate_visits(uuid, date, date), public.reschedule_visit(uuid, date, uuid, text),
  public.start_visit(uuid), public.complete_visit(uuid, text), public.skip_visit(uuid, text, boolean),
  public.assign_visit(uuid, uuid, uuid[]), public.schedule(uuid, date, date)
  from public, anon;
grant execute on function public.generate_visits(uuid, date, date), public.reschedule_visit(uuid, date, uuid, text),
  public.start_visit(uuid), public.complete_visit(uuid, text), public.skip_visit(uuid, text, boolean),
  public.assign_visit(uuid, uuid, uuid[]), public.schedule(uuid, date, date)
  to authenticated, service_role;
