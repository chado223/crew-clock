-- Job costing: labor cost and margin per completed visit.
--
-- Lawn crews clock in for the day and drive between stops, so labor is split:
--   on-site  = each worker's paid time overlapping the visit (started -> completed)
--   overhead = rest of that worker's paid day (drive, load-up, breaks paid),
--              spread over that day's visits in proportion to on-site time
--   cost     = (on-site + overhead) x the worker's pay rate on that date
-- Paid time comes from timesheet(), the single hours calculation (ADR 0002).
-- Workers = the visit's crew members plus direct assignees.
-- Overtime premium is not applied yet (base rate); noted in cost_basis.

create index if not exists employee_pay_rates_lookup_idx on public.employee_pay_rates (employee_id, effective_from desc);
create index if not exists visits_tenant_completed_idx on public.visits (tenant_id, completed_at) where status = 'completed';

create or replace function private.pay_rate_on(p_employee_id uuid, p_date date) returns numeric
language sql stable security definer set search_path = '' as $$
  select r.hourly_rate from public.employee_pay_rates r
  where r.employee_id = p_employee_id and r.effective_from <= p_date
  order by r.effective_from desc limit 1
$$;

create or replace function public.visit_costing(p_tenant_id uuid, p_from date, p_to date)
returns table (
  visit_id uuid,
  scheduled_date date,
  job_id uuid,
  job_title text,
  client_id uuid,
  client_name text,
  property_id uuid,
  revenue numeric,
  workers integer,
  onsite_minutes integer,
  overhead_minutes integer,
  labor_cost numeric,
  margin numeric,
  margin_pct numeric,
  missing_rates integer
)
language plpgsql stable security definer set search_path = '' as $$
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
    select w.visit_id, w.employee_id, d.scheduled_date,
           coalesce(sum(greatest(0, extract(epoch from (
             least(s.clock_out, d.completed_at) - greatest(s.clock_in, coalesce(d.started_at, d.completed_at))
           )))), 0)::bigint as seconds
    from workers w
    join done d on d.id = w.visit_id
    left join shifts s on s.employee_id = w.employee_id
      and s.clock_in < d.completed_at and s.clock_out > coalesce(d.started_at, d.completed_at)
    group by w.visit_id, w.employee_id, d.scheduled_date
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
end $$;

revoke execute on function private.pay_rate_on(uuid, date) from public, anon, authenticated;
revoke execute on function public.visit_costing(uuid, date, date) from public, anon;
grant execute on function public.visit_costing(uuid, date, date) to authenticated, service_role;
