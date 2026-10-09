-- Second readiness audit: field and office fixes.
--
-- 1. Job costing groups a visit's on-site time by the day the work was
--    actually done (not the day it was scheduled), so a rained-out Monday
--    visit done Tuesday is costed against Tuesday's paid hours once.
-- 2. reopen_visit: a stop the crew couldn't do, a canceled visit, or a visit
--    marked done by mistake (not yet billed) goes back on the schedule, with a
--    reason. History is in the audit log.
-- 3. Customers set to inactive or lost stop getting recurring visits; their
--    upcoming scheduled visits are canceled (kept). Reactivating fills again.

-- 1 -------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.visit_costing(p_tenant_id uuid, p_from date, p_to date)
 RETURNS TABLE(visit_id uuid, scheduled_date date, job_id uuid, job_title text, client_id uuid, client_name text, property_id uuid, revenue numeric, workers integer, onsite_minutes integer, overhead_minutes integer, labor_cost numeric, margin numeric, margin_pct numeric, missing_rates integer)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
AS $function$
begin
  perform private.require_manager(p_tenant_id);
  if p_to < p_from or p_to - p_from > 366 then raise exception 'invalid_date_range' using errcode = '22023'; end if;

  return query
  with done as (
    select v.id, v.scheduled_date, v.job_id, v.client_id, v.property_id, v.crew_id,
           coalesce(v.price, 0) as price, v.started_at, v.completed_at
    from public.visits v
    where v.tenant_id = p_tenant_id and v.status = 'completed'
      and v.scheduled_date between p_from and p_to
  ),
  workers as (
    select d.id as visit_id, cm.employee_id from done d join public.crew_members cm on cm.crew_id = d.crew_id
    union
    select a.visit_id, a.employee_id from public.visit_assignments a join done d on d.id = a.visit_id
  ),
  shifts as (
    -- one calculation: paid seconds per shift, already net of unpaid breaks
    select t.entry_id, t.employee_id, t.work_date, t.clock_in, t.clock_out, t.worked_seconds
    from public.timesheet(p_tenant_id, p_from - 1, p_to + 1) t
    where t.status = 'closed'
  ),
  onsite as (
    select w.visit_id, w.employee_id,
           private.tenant_date(p_tenant_id, coalesce(d.started_at, d.completed_at)) as scheduled_date,
           coalesce(sum(greatest(0, extract(epoch from (
             least(s.clock_out, d.completed_at) - greatest(s.clock_in, coalesce(d.started_at, d.completed_at))
           )))), 0)::bigint as seconds
    from workers w
    join done d on d.id = w.visit_id
    left join shifts s on s.employee_id = w.employee_id
      and s.clock_in < d.completed_at and s.clock_out > coalesce(d.started_at, d.completed_at)
    group by w.visit_id, w.employee_id, private.tenant_date(p_tenant_id, coalesce(d.started_at, d.completed_at))
  ),
  day_paid as (
    select s.employee_id, s.work_date, sum(s.worked_seconds)::bigint as seconds
    from shifts s group by s.employee_id, s.work_date
  ),
  day_onsite as (
    select o.employee_id, o.scheduled_date, sum(o.seconds)::bigint as seconds
    from onsite o group by o.employee_id, o.scheduled_date
  ),
  per_worker as (
    select o.visit_id, o.employee_id, o.scheduled_date, o.seconds as onsite_s,
           case when dos.seconds > 0
                then (greatest(coalesce(dp.seconds, 0) - dos.seconds, 0) * o.seconds::numeric / dos.seconds)
                else 0 end as overhead_s,
           private.pay_rate_on(o.employee_id, o.scheduled_date) as rate
    from onsite o
    join day_onsite dos on dos.employee_id = o.employee_id and dos.scheduled_date = o.scheduled_date
    left join day_paid dp on dp.employee_id = o.employee_id and dp.work_date = o.scheduled_date
  ),
  per_visit as (
    select pw.visit_id,
           count(*) filter (where pw.onsite_s > 0)::int as workers,
           sum(pw.onsite_s)::numeric as onsite_s,
           sum(pw.overhead_s)::numeric as overhead_s,
           round(sum((pw.onsite_s + pw.overhead_s) / 3600.0 * coalesce(pw.rate, 0)), 2) as cost,
           count(*) filter (where pw.onsite_s > 0 and pw.rate is null)::int as missing_rates
    from per_worker pw group by pw.visit_id
  )
  select d.id, d.scheduled_date, d.job_id, j.title, d.client_id, c.name, d.property_id,
         d.price,
         coalesce(pv.workers, 0),
         round(coalesce(pv.onsite_s, 0) / 60)::int,
         round(coalesce(pv.overhead_s, 0) / 60)::int,
         coalesce(pv.cost, 0),
         d.price - coalesce(pv.cost, 0),
         case when d.price > 0 then round((d.price - coalesce(pv.cost, 0)) / d.price * 100, 1) end,
         coalesce(pv.missing_rates, 0)
  from done d
  join public.jobs j on j.id = d.job_id
  left join public.clients c on c.id = d.client_id
  left join per_visit pv on pv.visit_id = d.id
  order by d.scheduled_date, j.title;
end $function$;

-- 2 -------------------------------------------------------------------------
create or replace function public.reopen_visit(p_visit_id uuid, p_date date, p_reason text)
returns public.visits
language plpgsql security definer set search_path = '' as $$
declare
  v public.visits;
  v_reason text := private.require_reason(p_reason);
  v_was text;
begin
  select * into v from public.visits where id = p_visit_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(v.tenant_id);
  if v.status not in ('skipped','canceled','completed') then raise exception 'visit_not_closed' using errcode = '22023'; end if;
  if v.status = 'completed' and exists (
       select 1 from public.invoice_lines l join public.invoices i on i.id = l.invoice_id
       where l.visit_id = v.id and i.status <> 'void') then
    raise exception 'visit_billed' using errcode = '22023';
  end if;
  v_was := v.status;
  perform set_config('app.audit_reason', 'reopen_visit: ' || v_reason, true);
  update public.visits
     set status = 'scheduled',
         scheduled_date = coalesce(p_date, scheduled_date),
         started_at = null, completed_at = null, completed_by = null,
         status_reason = v_reason
   where id = v.id
  returning * into v;
  if v.client_id is not null then
    perform private.log_activity(v.tenant_id, v.client_id, v.property_id, 'visit_reopened',
      format('%s put back on the schedule for %s (was %s): %s', private.visit_label(v), to_char(v.scheduled_date, 'Mon FMDD'), v_was, v_reason),
      jsonb_build_object('visit_id', v.id, 'was', v_was));
  end if;
  perform set_config('app.audit_reason', '', true);
  return v;
end $$;
revoke execute on function public.reopen_visit(uuid, date, text) from public, anon;
grant execute on function public.reopen_visit(uuid, date, text) to authenticated;

-- 3 -------------------------------------------------------------------------
create or replace function public.generate_visits(p_tenant_id uuid, p_from date, p_to date)
returns integer
language plpgsql security definer set search_path = '' as $$
declare
  v_count integer;
begin
  if auth.uid() is not null then perform private.require_manager(p_tenant_id); end if;
  if p_to < p_from or p_to - p_from > 62 then raise exception 'invalid_date_range' using errcode = '22023'; end if;

  insert into public.visits (tenant_id, job_id, scheduled_date, generated_for)
  select j.tenant_id, j.id, d::date, d::date
  from public.jobs j
  join public.clients c on c.id = j.client_id and c.status in ('active','lead')
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

create or replace function private.client_status_changed() returns trigger
language plpgsql security definer set search_path = '' as $$
declare n integer;
begin
  if new.status in ('inactive','lost') and old.status not in ('inactive','lost') then
    update public.visits v set status = 'canceled', status_reason = 'Customer ' || new.status, generated_for = null
     where v.client_id = new.id and v.status = 'scheduled' and v.scheduled_date >= private.tenant_today(new.tenant_id);
    get diagnostics n = row_count;
    if n > 0 then
      perform private.log_activity(new.tenant_id, new.id, null, 'visits_canceled',
        format('%s upcoming visit%s canceled: customer marked %s', n, case when n = 1 then '' else 's' end, new.status), '{}'::jsonb);
    end if;
  elsif new.status in ('active','lead') and old.status in ('inactive','lost') then
    perform public.generate_visits(new.tenant_id, private.tenant_today(new.tenant_id), private.tenant_today(new.tenant_id) + 21);
  end if;
  return new;
end $$;
revoke execute on function private.client_status_changed() from public, anon, authenticated;
drop trigger if exists client_status_changed on public.clients;
create trigger client_status_changed after update of status on public.clients
  for each row when (old.status is distinct from new.status) execute function private.client_status_changed();
