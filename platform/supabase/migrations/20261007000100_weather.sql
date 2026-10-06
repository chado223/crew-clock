-- Weather automation (National Weather Service).
--
-- How it works:
--   1. A server-side worker (platform/packages/weather) asks weather_worker_targets()
--      which properties have weather-sensitive visits coming up, per company.
--   2. It fills in missing coordinates (free US Census geocoder), resolves each
--      point to its NWS forecast grid once, fetches the forecast, and saves one
--      row per property per day with weather_worker_save_forecast().
--   3. weather_evaluate() applies the company's own rules to those forecasts and
--      opens, updates or clears weather alerts on the affected visits.
--   4. The office sees alerts and decides: move the visit, acknowledge, or dismiss.
--      Nothing is moved automatically and no customer is contacted (yet).
--
-- Worker functions run only as service_role (never from a browser or phone).
-- Forecasts and alerts are internal dispatch information: customers never see them.

-- ---------------------------------------------------------------------------
-- Per-company settings
-- ---------------------------------------------------------------------------
create table if not exists public.weather_settings (
  tenant_id uuid primary key references public.tenants (id) on delete restrict,
  enabled boolean not null default false,
  rain_chance_pct integer not null default 60 check (rain_chance_pct between 1 and 100),
  wind_mph integer not null default 25 check (wind_mph between 5 and 150),
  min_temp_f integer default 35 check (min_temp_f between -40 and 120),     -- day's high below this = too cold
  max_temp_f integer default 100 check (max_temp_f between 40 and 140),     -- day's high at/above this = heat
  lookahead_days integer not null default 3 check (lookahead_days between 1 and 7),
  -- What happens when an alert opens. 'flag' = show it to the office only.
  -- Customer/crew notices plug in here once communications providers exist.
  on_alert text not null default 'flag' check (on_alert in ('flag')),
  updated_at timestamptz not null default now(),
  updated_by uuid references auth.users (id) on delete set null,
  check (min_temp_f is null or max_temp_f is null or min_temp_f < max_temp_f)
);

-- Some services ignore weather (e.g. a gutter cleaning in light rain).
alter table public.services add column if not exists weather_sensitive boolean not null default true;

-- Where a property's coordinates came from (manual entry vs geocoder).
alter table public.properties add column if not exists geocode_source text
  check (geocode_source in ('manual','census','provider'));
alter table public.properties add column if not exists geocoded_at timestamptz;

-- ---------------------------------------------------------------------------
-- NWS grid point per property (resolved once, reused until coordinates change)
-- ---------------------------------------------------------------------------
create table if not exists public.weather_points (
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  property_id uuid not null,
  latitude double precision not null,
  longitude double precision not null,
  grid_office text not null,
  grid_x integer not null,
  grid_y integer not null,
  resolved_at timestamptz not null default now(),
  primary key (property_id),
  foreign key (tenant_id, property_id) references public.properties (tenant_id, id) on delete cascade
);
create index if not exists weather_points_tenant_idx on public.weather_points (tenant_id);

-- ---------------------------------------------------------------------------
-- Daily forecast per property (latest fetch wins)
-- ---------------------------------------------------------------------------
create table if not exists public.weather_forecasts (
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  property_id uuid not null,
  forecast_date date not null,
  precip_pct integer check (precip_pct between 0 and 100),
  prior_night_precip_pct integer check (prior_night_precip_pct between 0 and 100),
  wind_mph integer check (wind_mph >= 0),
  temp_high_f integer,
  temp_low_f integer,
  summary text,
  source text not null default 'nws',
  fetched_at timestamptz not null default now(),
  primary key (property_id, forecast_date),
  foreign key (tenant_id, property_id) references public.properties (tenant_id, id) on delete cascade
);
create index if not exists weather_forecasts_tenant_date_idx on public.weather_forecasts (tenant_id, forecast_date);

-- ---------------------------------------------------------------------------
-- Alerts on visits
-- ---------------------------------------------------------------------------
create table if not exists public.weather_alerts (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  visit_id uuid not null,
  property_id uuid,
  forecast_date date not null,
  reasons text[] not null check (cardinality(reasons) > 0),
  precip_pct integer,
  wind_mph integer,
  temp_high_f integer,
  summary text,
  status text not null default 'open'
    check (status in ('open','acknowledged','dismissed','cleared','moved')),
  handled_by uuid references auth.users (id) on delete set null,
  handled_at timestamptz,
  handled_note text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, id),
  unique (visit_id, forecast_date),
  foreign key (tenant_id, visit_id) references public.visits (tenant_id, id) on delete cascade
);
create index if not exists weather_alerts_tenant_status_idx on public.weather_alerts (tenant_id, status, forecast_date);

-- Generic triggers
do $$
declare t text;
begin
  foreach t in array array['weather_settings','weather_alerts'] loop
    execute format('drop trigger if exists set_updated_at on public.%I', t);
    execute format('create trigger set_updated_at before update on public.%I for each row execute function private.set_updated_at()', t);
  end loop;
  foreach t in array array['weather_settings','weather_points','weather_forecasts','weather_alerts'] loop
    execute format('drop trigger if exists prevent_tenant_change on public.%I', t);
    execute format('create trigger prevent_tenant_change before update of tenant_id on public.%I for each row execute function private.prevent_tenant_change()', t);
  end loop;
  -- Settings and office decisions are audited; forecast refreshes are not (noise).
  foreach t in array array['weather_settings','weather_alerts'] loop
    execute format('drop trigger if exists audit_row_change on public.%I', t);
    execute format('create trigger audit_row_change after insert or update or delete on public.%I for each row execute function private.audit_row_change()', t);
  end loop;
end $$;

-- Coordinates changed by hand: drop the cached grid so it is resolved again.
create or replace function private.property_coords_changed() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  if new.latitude is distinct from old.latitude or new.longitude is distinct from old.longitude then
    delete from public.weather_points where property_id = new.id;
    if new.geocode_source is not distinct from old.geocode_source and new.latitude is not null then
      new.geocode_source := 'manual';
      new.geocoded_at := now();
    end if;
  end if;
  -- An address change makes geocoded coordinates stale.
  if (new.address_line1, new.city, new.region, new.postal_code) is distinct from
     (old.address_line1, old.city, old.region, old.postal_code)
     and new.geocode_source = 'census' and new.latitude is not distinct from old.latitude then
    new.latitude := null; new.longitude := null; new.geocode_source := null; new.geocoded_at := null;
    delete from public.weather_points where property_id = new.id;
  end if;
  return new;
end $$;
drop trigger if exists property_coords_changed on public.properties;
create trigger property_coords_changed before update of latitude, longitude, address_line1, city, region, postal_code
  on public.properties for each row execute function private.property_coords_changed();

-- ---------------------------------------------------------------------------
-- Row Level Security: office (owner/admin) only. Crew and customers see nothing here.
-- ---------------------------------------------------------------------------
alter table public.weather_settings enable row level security;
alter table public.weather_points enable row level security;
alter table public.weather_forecasts enable row level security;
alter table public.weather_alerts enable row level security;

drop policy if exists weather_settings_select on public.weather_settings;
drop policy if exists weather_settings_insert on public.weather_settings;
drop policy if exists weather_settings_update on public.weather_settings;
create policy weather_settings_select on public.weather_settings for select to authenticated using (public.is_admin_or_owner(tenant_id));
create policy weather_settings_insert on public.weather_settings for insert to authenticated with check (public.is_admin_or_owner(tenant_id));
create policy weather_settings_update on public.weather_settings for update to authenticated
  using (public.is_admin_or_owner(tenant_id)) with check (public.is_admin_or_owner(tenant_id));

drop policy if exists weather_points_select on public.weather_points;
create policy weather_points_select on public.weather_points for select to authenticated using (public.is_admin_or_owner(tenant_id));

drop policy if exists weather_forecasts_select on public.weather_forecasts;
create policy weather_forecasts_select on public.weather_forecasts for select to authenticated using (public.is_admin_or_owner(tenant_id));

drop policy if exists weather_alerts_select on public.weather_alerts;
create policy weather_alerts_select on public.weather_alerts for select to authenticated using (public.is_admin_or_owner(tenant_id));

revoke all on public.weather_settings, public.weather_points, public.weather_forecasts, public.weather_alerts from anon, authenticated;
grant select on public.weather_settings, public.weather_points, public.weather_forecasts, public.weather_alerts to authenticated;
grant insert (tenant_id, enabled, rain_chance_pct, wind_mph, min_temp_f, max_temp_f, lookahead_days, on_alert)
  on public.weather_settings to authenticated;
grant update (enabled, rain_chance_pct, wind_mph, min_temp_f, max_temp_f, lookahead_days, on_alert)
  on public.weather_settings to authenticated;
grant all on public.weather_settings, public.weather_points, public.weather_forecasts, public.weather_alerts to service_role;

create or replace function private.weather_settings_stamp() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  new.updated_by := coalesce(auth.uid(), new.updated_by);
  return new;
end $$;
drop trigger if exists weather_settings_stamp on public.weather_settings;
create trigger weather_settings_stamp before insert or update on public.weather_settings
  for each row execute function private.weather_settings_stamp();

-- ---------------------------------------------------------------------------
-- Rules: one place decides what weather is a problem
-- ---------------------------------------------------------------------------
create or replace function private.weather_reasons(s public.weather_settings, f public.weather_forecasts)
returns text[] language sql immutable set search_path = '' as $$
  select coalesce(array_remove(array[
    case when f.precip_pct is not null and f.precip_pct >= s.rain_chance_pct then 'rain' end,
    case when f.wind_mph is not null and f.wind_mph >= s.wind_mph then 'wind' end,
    case when s.max_temp_f is not null and f.temp_high_f is not null and f.temp_high_f >= s.max_temp_f then 'heat' end,
    case when s.min_temp_f is not null and f.temp_high_f is not null and f.temp_high_f < s.min_temp_f then 'cold' end
  ], null), '{}')
$$;

-- Company-local "today".
create or replace function private.tenant_today(p_tenant_id uuid) returns date
language sql stable security definer set search_path = '' as $$
  select (now() at time zone coalesce(t.timezone, 'America/New_York'))::date from public.tenants t where t.id = p_tenant_id
$$;

-- Applies the company's rules to the latest forecasts. Returns alerts opened.
-- Idempotent: running it twice changes nothing the second time.
create or replace function public.weather_evaluate(p_tenant_id uuid) returns integer
language plpgsql security definer set search_path = '' as $$
declare
  s public.weather_settings;
  v_today date;
  v_opened integer := 0;
begin
  if auth.uid() is not null then perform private.require_manager(p_tenant_id); end if;
  select * into s from public.weather_settings where tenant_id = p_tenant_id;
  v_today := private.tenant_today(p_tenant_id);

  -- Alerts whose visit moved, finished, was skipped, or no longer matches: cleared.
  -- (A visit moved by the office from an alert is already 'moved'.)
  update public.weather_alerts a set status = 'cleared', updated_at = now()
  from public.visits v
  where a.tenant_id = p_tenant_id and a.visit_id = v.id
    and a.status in ('open','acknowledged')
    and (v.status <> 'scheduled' or v.scheduled_date <> a.forecast_date or s.tenant_id is null or not s.enabled);

  if s.tenant_id is null or not s.enabled then return 0; end if;

  with candidates as (
    select v.id as visit_id, v.property_id, v.scheduled_date, f as fc,
           private.weather_reasons(s, f) as reasons
    from public.visits v
    join public.jobs j on j.id = v.job_id
    left join public.services sv on sv.id = j.service_id
    join public.weather_forecasts f on f.property_id = v.property_id and f.forecast_date = v.scheduled_date
    where v.tenant_id = p_tenant_id and v.status = 'scheduled'
      and v.scheduled_date between v_today and v_today + s.lookahead_days
      and coalesce(sv.weather_sensitive, true)
  ),
  cleared as (   -- conditions improved
    update public.weather_alerts a set status = 'cleared', updated_at = now()
    from candidates c
    where a.visit_id = c.visit_id and a.forecast_date = c.scheduled_date
      and a.status in ('open','acknowledged') and cardinality(c.reasons) = 0
    returning a.id
  ),
  refreshed as ( -- still bad: keep numbers current; reopen if it had cleared
    update public.weather_alerts a set
      reasons = c.reasons, precip_pct = (c.fc).precip_pct, wind_mph = (c.fc).wind_mph,
      temp_high_f = (c.fc).temp_high_f, summary = (c.fc).summary,
      status = case when a.status = 'cleared' then 'open' else a.status end,
      updated_at = now()
    from candidates c
    where a.visit_id = c.visit_id and a.forecast_date = c.scheduled_date
      and cardinality(c.reasons) > 0 and a.status in ('open','acknowledged','cleared')
      and (a.reasons <> c.reasons or a.precip_pct is distinct from (c.fc).precip_pct
           or a.wind_mph is distinct from (c.fc).wind_mph or a.temp_high_f is distinct from (c.fc).temp_high_f
           or a.status = 'cleared')
    returning a.id
  ),
  opened as (
    insert into public.weather_alerts (tenant_id, visit_id, property_id, forecast_date, reasons,
                                       precip_pct, wind_mph, temp_high_f, summary)
    select p_tenant_id, c.visit_id, c.property_id, c.scheduled_date, c.reasons,
           (c.fc).precip_pct, (c.fc).wind_mph, (c.fc).temp_high_f, (c.fc).summary
    from candidates c
    where cardinality(c.reasons) > 0
    on conflict (visit_id, forecast_date) do nothing
    returning id
  )
  select count(*) into v_opened from opened;

  return v_opened;
end $$;

-- Office decision on an alert. Moving the visit uses reschedule_visit (audited, in history).
create or replace function public.handle_weather_alert(
  p_alert_id uuid, p_action text, p_new_date date default null, p_note text default null
) returns void
language plpgsql security definer set search_path = '' as $$
declare a public.weather_alerts;
begin
  select * into a from public.weather_alerts where id = p_alert_id;
  if not found then raise exception 'alert_not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(a.tenant_id);
  if p_action not in ('acknowledge','dismiss','move') then raise exception 'invalid_action' using errcode = '22023'; end if;
  if a.status not in ('open','acknowledged') then raise exception 'alert_closed' using errcode = '22023'; end if;
  if p_note is not null and length(p_note) > 500 then raise exception 'note_too_long' using errcode = '22023'; end if;

  if p_action = 'move' then
    if p_new_date is null or p_new_date = a.forecast_date then raise exception 'date_required' using errcode = '22023'; end if;
    perform public.reschedule_visit(a.visit_id, p_new_date, null,
      'Weather: ' || array_to_string(a.reasons, ', ') || coalesce(' — ' || nullif(btrim(p_note), ''), ''));
  end if;

  update public.weather_alerts set
    status = case p_action when 'acknowledge' then 'acknowledged' when 'dismiss' then 'dismissed' else 'moved' end,
    handled_by = auth.uid(), handled_at = now(), handled_note = nullif(btrim(p_note), '')
  where id = p_alert_id;
end $$;

-- Office view: upcoming visits with their forecast and alert, in company-local days.
create or replace function public.weather_outlook(p_tenant_id uuid)
returns table (
  visit_id uuid, scheduled_date date, client_name text, property_address text, crew_name text,
  weather_sensitive boolean, precip_pct integer, wind_mph integer, temp_high_f integer, summary text,
  fetched_at timestamptz, alert_id uuid, alert_status text, reasons text[]
)
language plpgsql stable security definer set search_path = '' as $$
declare v_today date; v_days integer;
begin
  perform private.require_manager(p_tenant_id);
  v_today := private.tenant_today(p_tenant_id);
  select coalesce(max(lookahead_days), 3) into v_days from public.weather_settings where tenant_id = p_tenant_id;
  return query
  select v.id, v.scheduled_date, c.name,
         concat_ws(', ', p.address_line1, p.city), cr.name,
         coalesce(sv.weather_sensitive, true),
         f.precip_pct, f.wind_mph, f.temp_high_f, f.summary, f.fetched_at,
         a.id, a.status, a.reasons
  from public.visits v
  join public.jobs j on j.id = v.job_id
  left join public.services sv on sv.id = j.service_id
  left join public.clients c on c.id = v.client_id
  left join public.properties p on p.id = v.property_id
  left join public.crews cr on cr.id = v.crew_id
  left join public.weather_forecasts f on f.property_id = v.property_id and f.forecast_date = v.scheduled_date
  left join public.weather_alerts a on a.visit_id = v.id and a.forecast_date = v.scheduled_date
  where v.tenant_id = p_tenant_id and v.status = 'scheduled'
    and v.scheduled_date between v_today and v_today + v_days
  order by v.scheduled_date, v.sort_order, c.name;
end $$;

-- ---------------------------------------------------------------------------
-- Worker API (service_role only)
-- ---------------------------------------------------------------------------

-- Properties needing a forecast: enabled companies, weather-sensitive scheduled
-- visits inside each company's lookahead window.
create or replace function public.weather_worker_targets()
returns table (
  tenant_id uuid, property_id uuid, address text, latitude double precision, longitude double precision,
  grid_office text, grid_x integer, grid_y integer, first_date date, last_date date
)
language sql stable security definer set search_path = '' as $$
  select distinct on (p.id)
    p.tenant_id, p.id,
    concat_ws(', ', p.address_line1, p.city, concat_ws(' ', p.region, p.postal_code)),
    p.latitude, p.longitude, wp.grid_office, wp.grid_x, wp.grid_y,
    private.tenant_today(s.tenant_id), private.tenant_today(s.tenant_id) + s.lookahead_days
  from public.weather_settings s
  join public.tenants t on t.id = s.tenant_id and coalesce(t.status, 'active') = 'active'
  join public.visits v on v.tenant_id = s.tenant_id and v.status = 'scheduled'
  join public.jobs j on j.id = v.job_id
  left join public.services sv on sv.id = j.service_id
  join public.properties p on p.id = v.property_id and p.status = 'active'
  left join public.weather_points wp on wp.property_id = p.id
  where s.enabled and coalesce(sv.weather_sensitive, true)
    and v.scheduled_date between private.tenant_today(s.tenant_id) and private.tenant_today(s.tenant_id) + s.lookahead_days
    and coalesce(p.country, 'US') = 'US'          -- NWS covers the US only
  order by p.id
$$;

-- Coordinates from the geocoder. Never overwrites coordinates someone entered.
create or replace function public.weather_worker_save_coords(
  p_property_id uuid, p_latitude double precision, p_longitude double precision, p_source text default 'census'
) returns void
language plpgsql security definer set search_path = '' as $$
begin
  if p_source not in ('census','provider') then raise exception 'invalid_source' using errcode = '22023'; end if;
  update public.properties set latitude = p_latitude, longitude = p_longitude,
         geocode_source = p_source, geocoded_at = now()
  where id = p_property_id and latitude is null;
end $$;

create or replace function public.weather_worker_save_point(
  p_property_id uuid, p_office text, p_grid_x integer, p_grid_y integer
) returns void
language plpgsql security definer set search_path = '' as $$
begin
  insert into public.weather_points (tenant_id, property_id, latitude, longitude, grid_office, grid_x, grid_y)
  select p.tenant_id, p.id, p.latitude, p.longitude, p_office, p_grid_x, p_grid_y
  from public.properties p where p.id = p_property_id and p.latitude is not null
  on conflict (property_id) do update set
    latitude = excluded.latitude, longitude = excluded.longitude,
    grid_office = excluded.grid_office, grid_x = excluded.grid_x, grid_y = excluded.grid_y, resolved_at = now();
end $$;

-- p_days: [{"date":"2026-10-06","precip_pct":40,"prior_night_precip_pct":10,"wind_mph":12,
--           "temp_high_f":71,"temp_low_f":50,"summary":"Chance Showers"}]
create or replace function public.weather_worker_save_forecast(p_property_id uuid, p_days jsonb, p_source text default 'nws')
returns integer
language plpgsql security definer set search_path = '' as $$
declare v_tenant uuid; n integer;
begin
  select tenant_id into v_tenant from public.properties where id = p_property_id;
  if v_tenant is null then raise exception 'property_not_found' using errcode = 'P0002'; end if;
  if jsonb_typeof(p_days) <> 'array' or jsonb_array_length(p_days) > 14 then
    raise exception 'invalid_forecast' using errcode = '22023';
  end if;
  insert into public.weather_forecasts (tenant_id, property_id, forecast_date, precip_pct, prior_night_precip_pct,
                                        wind_mph, temp_high_f, temp_low_f, summary, source, fetched_at)
  select v_tenant, p_property_id, (d->>'date')::date,
         least(100, greatest(0, (d->>'precip_pct')::integer)),
         least(100, greatest(0, (d->>'prior_night_precip_pct')::integer)),
         greatest(0, (d->>'wind_mph')::integer),
         (d->>'temp_high_f')::integer, (d->>'temp_low_f')::integer,
         left(d->>'summary', 200), p_source, now()
  from jsonb_array_elements(p_days) d
  on conflict (property_id, forecast_date) do update set
    precip_pct = excluded.precip_pct, prior_night_precip_pct = excluded.prior_night_precip_pct,
    wind_mph = excluded.wind_mph, temp_high_f = excluded.temp_high_f, temp_low_f = excluded.temp_low_f,
    summary = excluded.summary, source = excluded.source, fetched_at = excluded.fetched_at;
  get diagnostics n = row_count;
  -- Old forecasts are useless; keep two weeks for "what did we know" questions.
  delete from public.weather_forecasts where property_id = p_property_id and forecast_date < current_date - 14;
  return n;
end $$;

-- Companies with weather turned on (the worker evaluates each after saving).
create or replace function public.weather_worker_tenants() returns setof uuid
language sql stable security definer set search_path = '' as $$
  select s.tenant_id from public.weather_settings s
  join public.tenants t on t.id = s.tenant_id and coalesce(t.status, 'active') = 'active'
  where s.enabled
$$;

-- ---------------------------------------------------------------------------
-- Privileges
-- ---------------------------------------------------------------------------
revoke execute on function private.property_coords_changed(), private.weather_settings_stamp(),
  private.weather_reasons(public.weather_settings, public.weather_forecasts), private.tenant_today(uuid)
  from public, anon, authenticated;

revoke execute on function public.weather_evaluate(uuid), public.handle_weather_alert(uuid, text, date, text),
  public.weather_outlook(uuid) from public, anon;
grant execute on function public.weather_evaluate(uuid), public.handle_weather_alert(uuid, text, date, text),
  public.weather_outlook(uuid) to authenticated, service_role;

revoke execute on function public.weather_worker_targets(),
  public.weather_worker_save_coords(uuid, double precision, double precision, text),
  public.weather_worker_save_point(uuid, text, integer, integer),
  public.weather_worker_save_forecast(uuid, jsonb, text),
  public.weather_worker_tenants()
  from public, anon, authenticated;
grant execute on function public.weather_worker_targets(),
  public.weather_worker_save_coords(uuid, double precision, double precision, text),
  public.weather_worker_save_point(uuid, text, integer, integer),
  public.weather_worker_save_forecast(uuid, jsonb, text),
  public.weather_worker_tenants()
  to service_role;
