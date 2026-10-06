-- Editing work after it's set up: change a recurring job's price, crew, day,
-- frequency, end date, or pause/end it. Past visits are never touched; future
-- scheduled visits are canceled (kept, with a reason) or updated to match.

create or replace function public.update_job(
  p_job_id uuid,
  p_title text default null,
  p_price numeric default null,
  p_est_minutes integer default null,
  p_crew_id uuid default null,
  p_clear_crew boolean default false,
  p_interval_weeks integer default null,
  p_weekday integer default null,
  p_ends_on date default null,
  p_clear_ends_on boolean default false,
  p_status text default null,              -- active | paused | completed | canceled
  p_apply_to_scheduled boolean default true,
  p_reason text default null
) returns public.jobs
language plpgsql security definer set search_path = '' as $$
declare
  j public.jobs;
  n public.jobs;
  v_today date;
  v_changes text[] := '{}';
  v_canceled integer := 0;
  v_stopped boolean;
  v_rhythm boolean;
begin
  select * into j from public.jobs where id = p_job_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(j.tenant_id);
  v_today := private.tenant_today(j.tenant_id);

  if p_title is not null and length(btrim(p_title)) = 0 then raise exception 'title_required' using errcode = '22023'; end if;
  if p_price is not null and p_price < 0 then raise exception 'invalid_amount' using errcode = '22023'; end if;
  if p_status is not null and p_status not in ('active','paused','completed','canceled') then
    raise exception 'invalid_status' using errcode = '22023';
  end if;
  if p_crew_id is not null and not exists (select 1 from public.crews where id = p_crew_id and tenant_id = j.tenant_id) then
    raise exception 'crew_not_found' using errcode = 'P0002';
  end if;
  if (p_interval_weeks is not null or p_weekday is not null) and j.kind <> 'recurring' then
    raise exception 'not_recurring' using errcode = '22023';
  end if;
  if p_interval_weeks is not null and p_interval_weeks not between 1 and 52 then raise exception 'invalid_interval' using errcode = '22023'; end if;
  if p_weekday is not null and p_weekday not between 1 and 7 then raise exception 'invalid_weekday' using errcode = '22023'; end if;
  if p_ends_on is not null and p_ends_on < coalesce(j.starts_on, p_ends_on) then raise exception 'invalid_date_range' using errcode = '22023'; end if;

  perform set_config('app.audit_reason', coalesce(nullif(btrim(p_reason), ''), 'job edited'), true);
  update public.jobs set
    title = coalesce(nullif(btrim(p_title), ''), title),
    price = coalesce(round(p_price, 2), price),
    est_minutes = coalesce(p_est_minutes, est_minutes),
    crew_id = case when p_clear_crew then null else coalesce(p_crew_id, crew_id) end,
    interval_weeks = coalesce(p_interval_weeks, interval_weeks),
    weekday = coalesce(p_weekday, weekday),
    ends_on = case when p_clear_ends_on then null else coalesce(p_ends_on, ends_on) end,
    status = coalesce(p_status, status)
  where id = j.id
  returning * into n;

  if n.title is distinct from j.title then v_changes := v_changes || ('renamed to "' || n.title || '"'); end if;
  if n.price is distinct from j.price then v_changes := v_changes || format('price $%s → $%s', coalesce(j.price, 0), coalesce(n.price, 0)); end if;
  if n.crew_id is distinct from j.crew_id then v_changes := v_changes || 'crew changed'::text; end if;
  if n.interval_weeks is distinct from j.interval_weeks or n.weekday is distinct from j.weekday then v_changes := v_changes || 'schedule changed'::text; end if;
  if n.ends_on is distinct from j.ends_on then v_changes := v_changes || coalesce('ends ' || to_char(n.ends_on, 'Mon FMDD, YYYY'), 'no end date'); end if;
  if n.status is distinct from j.status then v_changes := v_changes || n.status; end if;

  v_stopped := n.status in ('paused','completed','canceled');
  v_rhythm := n.interval_weeks is distinct from j.interval_weeks or n.weekday is distinct from j.weekday;

  -- Future visits that no longer belong: canceled (kept), freed for regeneration.
  update public.visits v set
    status = 'canceled',
    status_reason = case when v_stopped then 'Job ' || n.status
                         when v_rhythm then 'Schedule changed'
                         else 'After job end date' end,
    generated_for = null
  where v.job_id = n.id and v.status = 'scheduled' and v.scheduled_date >= v_today
    and (v_stopped
         or (n.ends_on is not null and v.scheduled_date > n.ends_on)
         or (v_rhythm and v.generated_for is not null));
  get diagnostics v_canceled = row_count;

  -- Remaining future visits pick up the new price, time and crew.
  if p_apply_to_scheduled then
    update public.visits v set
      price = case when n.price is distinct from j.price then n.price else v.price end,
      est_minutes = case when n.est_minutes is distinct from j.est_minutes then n.est_minutes else v.est_minutes end,
      crew_id = case when n.crew_id is distinct from j.crew_id and v.crew_id is not distinct from j.crew_id then n.crew_id else v.crew_id end
    where v.job_id = n.id and v.status = 'scheduled' and v.scheduled_date >= v_today;
  end if;

  -- New rhythm (or resumed): put the next six weeks back on the schedule.
  if n.kind = 'recurring' and n.status in ('scheduled','active') and (v_rhythm or j.status = 'paused') then
    perform public.generate_visits(n.tenant_id, v_today, v_today + 42);
  end if;

  if cardinality(v_changes) > 0 and n.client_id is not null then
    perform private.log_activity(n.tenant_id, n.client_id, n.property_id, 'job_updated',
      format('%s: %s%s', n.title, array_to_string(v_changes, ', '),
             case when v_canceled > 0 then format(' (%s upcoming visit%s canceled)', v_canceled, case when v_canceled = 1 then '' else 's' end) else '' end),
      jsonb_build_object('job_id', n.id));
  end if;
  perform set_config('app.audit_reason', '', true);
  return n;
end $$;

-- A property can't be archived while active work points at it.
create or replace function private.property_archive_guard() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  if new.status = 'inactive' and old.status = 'active'
     and exists (select 1 from public.jobs j where j.property_id = new.id and j.status in ('scheduled','active')) then
    raise exception 'property_has_active_jobs' using errcode = '22023';
  end if;
  return new;
end $$;
drop trigger if exists property_archive_guard on public.properties;
create trigger property_archive_guard before update of status on public.properties
  for each row execute function private.property_archive_guard();

revoke execute on function private.property_archive_guard() from public, anon, authenticated;
revoke execute on function public.update_job(uuid, text, numeric, integer, uuid, boolean, integer, integer, date, boolean, text, boolean, text) from public, anon;
grant execute on function public.update_job(uuid, text, numeric, integer, uuid, boolean, integer, integer, date, boolean, text, boolean, text) to authenticated, service_role;
