-- Routes: each crew's day as an ordered list of stops.
--
-- The order of stops lives in ONE place: visits.sort_order (what the schedule,
-- the crew app and the portal already read). A route plan records who/what set
-- that order (by hand, the free straight-line planner, or a paid road-routing
-- provider later) and its drive estimates. Swapping in a paid provider only
-- changes how the order is computed, never where it is stored.

-- ---------------------------------------------------------------------------
-- Yards: where crews start and end the day
-- ---------------------------------------------------------------------------
create table if not exists public.yards (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  name text not null default 'Yard' check (length(btrim(name)) between 1 and 80),
  address_line1 text,
  city text,
  region text,
  postal_code text,
  latitude double precision check (latitude between -90 and 90),
  longitude double precision check (longitude between -180 and 180),
  is_default boolean not null default false,
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, id)
);
create unique index if not exists yards_one_default_uidx on public.yards (tenant_id) where is_default and active;

-- ---------------------------------------------------------------------------
-- Route plans: one per crew per day
-- ---------------------------------------------------------------------------
create table if not exists public.route_plans (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  crew_id uuid not null,
  route_date date not null,
  start_yard_id uuid,
  provider text not null default 'manual' check (provider ~ '^[a-z][a-z0-9_]{1,30}$'),
  road_based boolean not null default false,       -- false = straight-line estimate
  est_drive_minutes integer check (est_drive_minutes between 0 and 2880),
  est_drive_miles numeric(8,1) check (est_drive_miles >= 0),
  stop_count integer not null default 0,
  planned_by uuid references auth.users (id) on delete set null,
  planned_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, id),
  unique (tenant_id, crew_id, route_date),
  foreign key (tenant_id, crew_id) references public.crews (tenant_id, id),
  foreign key (tenant_id, start_yard_id) references public.yards (tenant_id, id)
);
create index if not exists route_plans_tenant_date_idx on public.route_plans (tenant_id, route_date);

do $$
declare t text;
begin
  foreach t in array array['yards','route_plans'] loop
    execute format('drop trigger if exists set_updated_at on public.%I', t);
    execute format('create trigger set_updated_at before update on public.%I for each row execute function private.set_updated_at()', t);
    execute format('drop trigger if exists prevent_tenant_change on public.%I', t);
    execute format('create trigger prevent_tenant_change before update of tenant_id on public.%I for each row execute function private.prevent_tenant_change()', t);
    execute format('drop trigger if exists audit_row_change on public.%I', t);
    execute format('create trigger audit_row_change after insert or update or delete on public.%I for each row execute function private.audit_row_change()', t);
  end loop;
end $$;

-- Crew member of a given crew?
create or replace function private.is_on_crew(p_crew_id uuid) returns boolean
language sql stable security definer set search_path = '' as $$
  select exists (
    select 1 from public.crew_members cm
    join public.employees e on e.id = cm.employee_id and e.user_id = (select auth.uid()) and e.status = 'active'
    where cm.crew_id = p_crew_id
  )
$$;

alter table public.yards enable row level security;
alter table public.route_plans enable row level security;

drop policy if exists yards_select on public.yards;
drop policy if exists yards_insert on public.yards;
drop policy if exists yards_update on public.yards;
create policy yards_select on public.yards for select to authenticated using (public.in_tenant(tenant_id));
create policy yards_insert on public.yards for insert to authenticated with check (public.is_admin_or_owner(tenant_id));
create policy yards_update on public.yards for update to authenticated
  using (public.is_admin_or_owner(tenant_id)) with check (public.is_admin_or_owner(tenant_id));

drop policy if exists route_plans_select on public.route_plans;
create policy route_plans_select on public.route_plans for select to authenticated
  using (public.is_admin_or_owner(tenant_id) or (public.in_tenant(tenant_id) and private.is_on_crew(crew_id)));

revoke all on public.yards, public.route_plans from anon, authenticated;
grant select on public.yards, public.route_plans to authenticated;
grant insert (tenant_id, name, address_line1, city, region, postal_code, latitude, longitude, is_default, active) on public.yards to authenticated;
grant update (name, address_line1, city, region, postal_code, latitude, longitude, is_default, active) on public.yards to authenticated;
grant all on public.yards, public.route_plans to service_role;

-- ---------------------------------------------------------------------------
-- Setting the order
-- ---------------------------------------------------------------------------
-- p_visit_ids must list every non-canceled visit the crew has that day, once.
-- Finished stops may be included (they keep their place in the day's story).
create or replace function public.set_route_order(
  p_tenant_id uuid,
  p_crew_id uuid,
  p_date date,
  p_visit_ids uuid[],
  p_provider text default 'manual',
  p_road_based boolean default false,
  p_est_drive_minutes integer default null,
  p_est_drive_miles numeric default null,
  p_start_yard_id uuid default null
) returns public.route_plans
language plpgsql security definer set search_path = '' as $$
declare
  v_expected uuid[];
  r public.route_plans;
begin
  perform private.require_manager(p_tenant_id);
  if p_date is null or p_crew_id is null then raise exception 'date_required' using errcode = '22023'; end if;
  if not exists (select 1 from public.crews where id = p_crew_id and tenant_id = p_tenant_id) then
    raise exception 'crew_not_found' using errcode = 'P0002';
  end if;
  if p_start_yard_id is not null and not exists (select 1 from public.yards where id = p_start_yard_id and tenant_id = p_tenant_id) then
    raise exception 'not_found' using errcode = 'P0002';
  end if;

  select coalesce(array_agg(v.id order by v.id), '{}') into v_expected
  from public.visits v
  where v.tenant_id = p_tenant_id and v.crew_id = p_crew_id and v.scheduled_date = p_date and v.status <> 'canceled';

  if coalesce(cardinality(p_visit_ids), 0) <> cardinality(v_expected)
     or (select array_agg(x order by x) from (select distinct unnest(p_visit_ids) x) d) is distinct from
        (case when cardinality(v_expected) = 0 then null else v_expected end) then
    raise exception 'route_mismatch' using errcode = '22023';
  end if;

  perform set_config('app.audit_reason', 'route order (' || p_provider || ')', true);
  update public.visits v set sort_order = o.n * 10
  from unnest(p_visit_ids) with ordinality as o(id, n)
  where v.id = o.id and v.sort_order is distinct from o.n * 10;

  insert into public.route_plans (tenant_id, crew_id, route_date, start_yard_id, provider, road_based,
                                  est_drive_minutes, est_drive_miles, stop_count, planned_by, planned_at)
  values (p_tenant_id, p_crew_id, p_date, p_start_yard_id, p_provider, coalesce(p_road_based, false),
          p_est_drive_minutes, round(p_est_drive_miles, 1), cardinality(p_visit_ids), auth.uid(), now())
  on conflict (tenant_id, crew_id, route_date) do update set
    start_yard_id = excluded.start_yard_id, provider = excluded.provider, road_based = excluded.road_based,
    est_drive_minutes = excluded.est_drive_minutes, est_drive_miles = excluded.est_drive_miles,
    stop_count = excluded.stop_count, planned_by = excluded.planned_by, planned_at = excluded.planned_at
  returning * into r;
  perform set_config('app.audit_reason', '', true);
  return r;
end $$;

-- A visit that changes crew or day drops out of its old route's estimate.
create or replace function private.visit_route_changed() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  update public.route_plans set est_drive_minutes = null, est_drive_miles = null, provider = 'manual', road_based = false
  where tenant_id = old.tenant_id and crew_id = old.crew_id and route_date = old.scheduled_date
    and (est_drive_minutes is not null or est_drive_miles is not null);
  if new.crew_id is not null then
    update public.route_plans set est_drive_minutes = null, est_drive_miles = null, provider = 'manual', road_based = false
    where tenant_id = new.tenant_id and crew_id = new.crew_id and route_date = new.scheduled_date
      and (est_drive_minutes is not null or est_drive_miles is not null);
  end if;
  return new;
end $$;
drop trigger if exists visit_route_changed on public.visits;
create trigger visit_route_changed after update of crew_id, scheduled_date on public.visits
  for each row when (old.crew_id is distinct from new.crew_id or old.scheduled_date is distinct from new.scheduled_date)
  execute function private.visit_route_changed();

-- New visits land at the end of their crew's day instead of the top.
create or replace function private.visit_sort_default() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  if new.sort_order = 0 and new.crew_id is not null then
    select coalesce(max(v.sort_order), 0) + 10 into new.sort_order
    from public.visits v
    where v.tenant_id = new.tenant_id and v.crew_id = new.crew_id and v.scheduled_date = new.scheduled_date
      and v.id <> new.id;
  end if;
  return new;
end $$;
drop trigger if exists visit_sort_default on public.visits;
-- Runs after visit_defaults (alphabetical) so the crew from the job is known.
create trigger zz_visit_sort_default before insert on public.visits
  for each row execute function private.visit_sort_default();

-- ---------------------------------------------------------------------------
-- Privileges
-- ---------------------------------------------------------------------------
revoke execute on function private.is_on_crew(uuid), private.visit_route_changed(), private.visit_sort_default()
  from public, anon, authenticated;
grant execute on function private.is_on_crew(uuid) to authenticated;   -- used in RLS

revoke execute on function public.set_route_order(uuid, uuid, date, uuid[], text, boolean, integer, numeric, uuid) from public, anon;
grant execute on function public.set_route_order(uuid, uuid, date, uuid[], text, boolean, integer, numeric, uuid) to authenticated, service_role;
