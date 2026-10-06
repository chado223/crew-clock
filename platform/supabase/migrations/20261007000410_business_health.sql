-- Owner command center: what needs attention (each item points at a record),
-- and business-health views built on the existing single calculations:
--   hours        -> timesheet()        (ADR 0002)
--   job labor    -> visit_costing()    (job costing)
--   work done    -> completed visits' price (same as visit_costing.revenue)
--   collected    -> payments.received_on
--   billed       -> invoices.sent_at
-- Definitions are written out in docs/METRICS.md and on screen.

-- ---------------------------------------------------------------------------
-- Payroll cost for a period: every paid (closed, not voided) hour x the rate
-- in effect that day. Includes time not on any visit (shop days, travel).
-- ---------------------------------------------------------------------------
create or replace function private.payroll_cost(p_tenant_id uuid, p_from date, p_to date)
returns table (paid_seconds bigint, cost numeric, missing_rate_shifts integer)
language sql stable security definer set search_path = '' as $$
  select coalesce(sum(t.worked_seconds), 0)::bigint,
         round(coalesce(sum(t.worked_seconds / 3600.0 * coalesce(private.pay_rate_on(t.employee_id, t.work_date), 0)), 0), 2),
         count(*) filter (where private.pay_rate_on(t.employee_id, t.work_date) is null)::int
  from public.timesheet(p_tenant_id, p_from, p_to) t
  where t.status = 'closed'
$$;

-- ---------------------------------------------------------------------------
-- What needs attention now. One row per actionable record.
-- severity: 1 = act today, 2 = this week, 3 = when you can
-- ref_type tells the app where to link: visit, invoice, estimate, client, time_entry, message
-- ---------------------------------------------------------------------------
create or replace function public.owner_attention(p_tenant_id uuid)
returns table (
  kind text, severity integer, title text, detail text,
  ref_type text, ref_id uuid, ref_date date, client_id uuid, amount numeric
)
language plpgsql stable security definer set search_path = '' as $$
declare v_today date;
begin
  perform private.require_manager(p_tenant_id);
  v_today := private.tenant_today(p_tenant_id);
  return query
  with items as (
    -- Clocked in a long time: probably forgot to clock out
    select 'long_shift'::text as kind, 1 as severity,
           e.display_name || ' has been clocked in over 12 hours' as title,
           'Since ' || to_char(te.clock_in at time zone (select timezone from public.tenants where id = p_tenant_id), 'Dy FMHH12:MI AM') as detail,
           'time_entry'::text as ref_type, te.id as ref_id, null::date as ref_date, null::uuid as client_id, null::numeric as amount
    from public.time_entries te join public.employees e on e.id = te.employee_id
    where te.tenant_id = p_tenant_id and te.clock_out is null and te.voided_at is null and te.clock_in < now() - interval '12 hours'
    union all
    -- Weather risk
    select 'weather', 1, initcap(array_to_string(a.reasons, ' + ')) || ' forecast: ' || coalesce(c.name, 'visit'),
           to_char(a.forecast_date, 'Dy Mon FMDD') || coalesce(' · ' || a.precip_pct || '% rain', '') || coalesce(' · ' || a.wind_mph || ' mph', ''),
           'visit', a.visit_id, a.forecast_date, v.client_id, null
    from public.weather_alerts a join public.visits v on v.id = a.visit_id left join public.clients c on c.id = v.client_id
    where a.tenant_id = p_tenant_id and a.status = 'open' and a.forecast_date >= v_today
    union all
    -- Missed: still scheduled but the day has passed
    select 'missed_visit', 1, 'Not done: ' || coalesce(c.name, j.title), to_char(v.scheduled_date, 'Dy Mon FMDD') || ' · still marked scheduled',
           'visit', v.id, v.scheduled_date, v.client_id, v.price
    from public.visits v join public.jobs j on j.id = v.job_id left join public.clients c on c.id = v.client_id
    where v.tenant_id = p_tenant_id and v.status = 'scheduled' and v.scheduled_date between v_today - 14 and v_today - 1
    union all
    -- Nobody assigned for today or tomorrow
    select 'unassigned', 1, 'No crew: ' || coalesce(c.name, j.title), to_char(v.scheduled_date, 'Dy Mon FMDD'),
           'visit', v.id, v.scheduled_date, v.client_id, v.price
    from public.visits v join public.jobs j on j.id = v.job_id left join public.clients c on c.id = v.client_id
    where v.tenant_id = p_tenant_id and v.status = 'scheduled' and v.scheduled_date between v_today and v_today + 1
      and v.crew_id is null and not exists (select 1 from public.visit_assignments x where x.visit_id = v.id)
    union all
    -- Customer requests
    select 'request', 2, 'Request from ' || c.name, left(r.details, 120),
           'client', r.client_id, r.preferred_date, r.client_id, null
    from public.service_requests r join public.clients c on c.id = r.client_id
    where r.tenant_id = p_tenant_id and r.status = 'new'
    union all
    -- Approved but not turned into work
    select 'estimate_approved', 2, 'Approved, not scheduled: ' || e.number || ' · ' || c.name, 'Approved ' || to_char(e.decided_at, 'Mon FMDD'),
           'estimate', e.id, null, e.client_id, e.subtotal
    from public.estimates e join public.clients c on c.id = e.client_id
    where e.tenant_id = p_tenant_id and e.status = 'approved'
    union all
    -- Sent estimates going quiet or expiring
    select 'estimate_followup', case when e.valid_until <= v_today + 3 then 2 else 3 end,
           'Follow up: ' || e.number || ' · ' || c.name,
           'Sent ' || to_char(e.sent_at, 'Mon FMDD') || coalesce(', expires ' || to_char(e.valid_until, 'Mon FMDD'), ''),
           'estimate', e.id, e.valid_until, e.client_id, e.subtotal
    from public.estimates e join public.clients c on c.id = e.client_id
    where e.tenant_id = p_tenant_id and e.status = 'sent'
      and (e.sent_at < now() - interval '5 days' or e.valid_until <= v_today + 7)
    union all
    -- Overdue invoices
    select 'invoice_overdue', case when inv.due_at < now() - interval '30 days' then 1 else 2 end,
           'Overdue: ' || coalesce(inv.number, 'invoice') || ' · ' || coalesce(c.name, ''),
           (v_today - (inv.due_at at time zone 'UTC')::date) || ' days past due',
           'invoice', inv.id, (inv.due_at at time zone 'UTC')::date, inv.client_id, inv.total - inv.amount_paid
    from public.invoices inv left join public.clients c on c.id = inv.client_id
    where inv.tenant_id = p_tenant_id and inv.status in ('sent','partial','overdue')
      and inv.total - inv.amount_paid > 0 and inv.due_at < now()
    union all
    -- Finished work not billed, per customer
    select 'unbilled', 2, 'Not invoiced: ' || coalesce(c.name, 'customer'),
           count(*) || ' finished visit' || case when count(*) = 1 then '' else 's' end || ' since ' || to_char(min(v.scheduled_date), 'Mon FMDD'),
           'client', v.client_id, min(v.scheduled_date), v.client_id, sum(coalesce(v.price, 0))
    from public.visits v left join public.clients c on c.id = v.client_id
    where v.tenant_id = p_tenant_id and v.status = 'completed' and v.client_id is not null
      and not exists (select 1 from public.invoice_lines l where l.visit_id = v.id)
    group by v.client_id, c.name
    union all
    -- Messages that didn't go out
    select 'message_problem', 2,
           case when m.status = 'failed' then 'Message failed: ' else 'Message held: ' end || coalesce(c.name, m.to_address, ''),
           replace(coalesce(m.suppressed_reason, m.error, ''), '_', ' '),
           'message', m.id, m.created_at::date, m.client_id, null
    from public.messages m left join public.clients c on c.id = m.client_id
    where m.tenant_id = p_tenant_id and m.created_at > now() - interval '7 days'
      and (m.status = 'failed' or (m.status = 'suppressed' and m.suppressed_reason <> 'messaging_off'))
    union all
    -- Leads nobody has touched in a week
    select 'lead_followup', 3, 'Lead: ' || c.name,
           'Last activity ' || coalesce(to_char(la.last_at, 'Mon FMDD'), 'never'),
           'client', c.id, null, c.id, null
    from public.clients c
    left join lateral (select max(a.occurred_at) as last_at from public.activity a where a.client_id = c.id) la on true
    where c.tenant_id = p_tenant_id and c.status = 'lead' and coalesce(la.last_at, c.created_at) < now() - interval '7 days'
  )
  select i.kind, i.severity, i.title, i.detail, i.ref_type, i.ref_id, i.ref_date, i.client_id, round(i.amount, 2)
  from items i
  order by i.severity, i.ref_date nulls last, i.title
  limit 200;
end $$;

-- ---------------------------------------------------------------------------
-- Profitability grouped by customer, property, service, job or crew.
-- Same rows as the Profit page (visit_costing) plus expenses tied to visits.
-- ---------------------------------------------------------------------------
create or replace function public.profitability(p_tenant_id uuid, p_from date, p_to date, p_by text)
returns table (
  key uuid, label text, sub text, visits integer, revenue numeric, labor_cost numeric, expenses numeric,
  margin numeric, margin_pct numeric, onsite_minutes integer, missing_rates integer
)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_manager(p_tenant_id);
  if p_by not in ('customer','property','service','job','crew') then raise exception 'invalid_group' using errcode = '22023'; end if;
  return query
  with vc as (select * from public.visit_costing(p_tenant_id, p_from, p_to)),
  x as (
    select vc.*, v.crew_id, j.service_id,
           coalesce((select sum(ex.amount) from public.expenses ex where ex.visit_id = vc.visit_id), 0) as exp
    from vc join public.visits v on v.id = vc.visit_id join public.jobs j on j.id = vc.job_id
  ),
  g as (
    select case p_by when 'customer' then x.client_id when 'property' then x.property_id when 'service' then x.service_id
                     when 'job' then x.job_id else x.crew_id end as k,
           count(*)::int as n, sum(x.revenue) as rev, sum(x.labor_cost) as lab, sum(x.exp) as ex,
           sum(x.onsite_minutes)::int as mins, sum(x.missing_rates)::int as miss
    from x group by 1
  )
  select g.k,
         case p_by
           when 'customer' then coalesce((select c.name from public.clients c where c.id = g.k), 'No customer')
           when 'property' then coalesce((select p.address_line1 from public.properties p where p.id = g.k), 'No property')
           when 'service' then coalesce((select s.name from public.services s where s.id = g.k), 'No service set')
           when 'job' then coalesce((select j.title from public.jobs j where j.id = g.k), 'Job')
           else coalesce((select cr.name from public.crews cr where cr.id = g.k), 'No crew') end,
         case p_by
           when 'property' then (select c.name from public.properties p join public.clients c on c.id = p.client_id where p.id = g.k)
           when 'job' then (select c.name from public.jobs j join public.clients c on c.id = j.client_id where j.id = g.k)
           else null end,
         g.n, round(g.rev, 2), round(g.lab, 2), round(g.ex, 2),
         round(g.rev - g.lab - g.ex, 2),
         case when g.rev > 0 then round((g.rev - g.lab - g.ex) / g.rev * 100, 1) end,
         g.mins, g.miss
  from g
  order by (g.rev - g.lab - g.ex) desc;
end $$;

-- ---------------------------------------------------------------------------
-- Customers drifting away: served in the last 6 months, nothing in 45 days,
-- nothing scheduled, still marked active.
-- ---------------------------------------------------------------------------
create or replace function public.at_risk_customers(p_tenant_id uuid)
returns table (client_id uuid, name text, last_visit date, visits_180d integer, revenue_180d numeric, open_balance numeric)
language plpgsql stable security definer set search_path = '' as $$
declare v_today date;
begin
  perform private.require_manager(p_tenant_id);
  v_today := private.tenant_today(p_tenant_id);
  return query
  select c.id, c.name, max(v.scheduled_date), count(*)::int, round(sum(coalesce(v.price, 0)), 2),
         coalesce((select round(sum(i.total - i.amount_paid), 2) from public.invoices i
                   where i.client_id = c.id and i.status in ('sent','partial','overdue')), 0)
  from public.clients c
  join public.visits v on v.client_id = c.id and v.status = 'completed' and v.scheduled_date >= v_today - 180
  where c.tenant_id = p_tenant_id and c.status = 'active'
    and not exists (select 1 from public.visits u where u.client_id = c.id and u.status = 'scheduled' and u.scheduled_date >= v_today)
    and not exists (select 1 from public.jobs j where j.client_id = c.id and j.kind = 'recurring' and j.status in ('scheduled','active')
                    and (j.ends_on is null or j.ends_on >= v_today))
  group by c.id, c.name
  having max(v.scheduled_date) < v_today - 45
  order by sum(coalesce(v.price, 0)) desc;
end $$;

-- ---------------------------------------------------------------------------
-- Open invoices with aging (as of company today).
-- ---------------------------------------------------------------------------
create or replace function public.receivables(p_tenant_id uuid)
returns table (invoice_id uuid, number text, client_id uuid, client_name text, sent_on date, due_on date,
               balance numeric, days_overdue integer, bucket text)
language plpgsql stable security definer set search_path = '' as $$
declare v_today date;
begin
  perform private.require_manager(p_tenant_id);
  v_today := private.tenant_today(p_tenant_id);
  return query
  select i.id, i.number, i.client_id, c.name, i.sent_at::date, (i.due_at at time zone 'UTC')::date,
         round(i.total - i.amount_paid, 2),
         greatest(0, v_today - coalesce((i.due_at at time zone 'UTC')::date, v_today))::int,
         case when i.due_at is null or (i.due_at at time zone 'UTC')::date >= v_today then 'current'
              when v_today - (i.due_at at time zone 'UTC')::date <= 30 then '1-30'
              when v_today - (i.due_at at time zone 'UTC')::date <= 60 then '31-60'
              when v_today - (i.due_at at time zone 'UTC')::date <= 90 then '61-90'
              else '90+' end
  from public.invoices i left join public.clients c on c.id = i.client_id
  where i.tenant_id = p_tenant_id and i.status in ('sent','partial','overdue') and i.total - i.amount_paid > 0
  order by i.due_at nulls last;
end $$;

-- ---------------------------------------------------------------------------
-- Business health for a period, plus point-in-time pictures (pipeline,
-- recurring revenue, workload). Every figure has a matching list elsewhere.
-- ---------------------------------------------------------------------------
create or replace function public.business_health(p_tenant_id uuid, p_from date, p_to date) returns jsonb
language plpgsql stable security definer set search_path = '' as $$
declare
  v_today date;
  r jsonb;
  vc_rev numeric; vc_lab numeric; vc_miss integer; vc_n integer; vc_onsite integer;
  pay record;
  v_exp numeric;
begin
  perform private.require_manager(p_tenant_id);
  if p_to < p_from or p_to - p_from > 366 then raise exception 'invalid_date_range' using errcode = '22023'; end if;
  v_today := private.tenant_today(p_tenant_id);

  select coalesce(sum(revenue), 0), coalesce(sum(labor_cost), 0), coalesce(sum(missing_rates), 0)::int, count(*)::int,
         coalesce(sum(onsite_minutes), 0)::int
    into vc_rev, vc_lab, vc_miss, vc_n, vc_onsite
  from public.visit_costing(p_tenant_id, p_from, p_to);
  select * into pay from private.payroll_cost(p_tenant_id, p_from, p_to);
  select coalesce(sum(amount), 0) into v_exp from public.expenses where tenant_id = p_tenant_id and spent_at between p_from and p_to;

  select jsonb_build_object(
    'from', p_from, 'to', p_to, 'today', v_today,
    'money', jsonb_build_object(
      'work_done', round(vc_rev, 2),
      'completed_visits', vc_n,
      'billed', (select coalesce(round(sum(total), 2), 0) from public.invoices
                 where tenant_id = p_tenant_id and status <> 'void' and sent_at is not null
                   and (sent_at at time zone 'UTC')::date between p_from and p_to),
      'collected', (select coalesce(round(sum(amount), 2), 0) from public.payments
                    where tenant_id = p_tenant_id and received_on between p_from and p_to),
      'job_labor', round(vc_lab, 2),
      'payroll', pay.cost,
      'paid_hours', round(pay.paid_seconds / 3600.0, 1),
      'unallocated_labor', greatest(round(pay.cost - vc_lab, 2), 0),
      'expenses', round(v_exp, 2),
      'gross_profit', round(vc_rev - pay.cost - v_exp, 2),
      'labor_pct', case when vc_rev > 0 then round(pay.cost / vc_rev * 100, 1) end,
      'gross_margin_pct', case when vc_rev > 0 then round((vc_rev - pay.cost - v_exp) / vc_rev * 100, 1) end,
      'revenue_per_paid_hour', case when pay.paid_seconds > 0 then round(vc_rev / (pay.paid_seconds / 3600.0), 2) end,
      'utilization_pct', case when pay.paid_seconds > 0 then round(vc_onsite * 60.0 / pay.paid_seconds * 100, 1) end,
      'missing_rates', vc_miss + pay.missing_rate_shifts),
    'receivables', (
      select jsonb_build_object(
        'total', coalesce(sum(balance), 0), 'count', count(*),
        'current', coalesce(sum(balance) filter (where bucket = 'current'), 0),
        'd1_30', coalesce(sum(balance) filter (where bucket = '1-30'), 0),
        'd31_60', coalesce(sum(balance) filter (where bucket = '31-60'), 0),
        'd61_90', coalesce(sum(balance) filter (where bucket = '61-90'), 0),
        'd90_plus', coalesce(sum(balance) filter (where bucket = '90+'), 0))
      from public.receivables(p_tenant_id)),
    'pipeline', (
      select jsonb_build_object(
        'created', count(*),
        'sent', count(*) filter (where status <> 'draft'),
        'open', count(*) filter (where status = 'sent'),
        'open_value', coalesce(sum(subtotal) filter (where status = 'sent'), 0),
        'won', count(*) filter (where status in ('approved','converted')),
        'won_value', coalesce(sum(subtotal) filter (where status in ('approved','converted')), 0),
        'lost', count(*) filter (where status in ('declined','expired')),
        'win_rate_pct', case when count(*) filter (where status in ('approved','converted','declined','expired')) > 0
          then round(count(*) filter (where status in ('approved','converted'))::numeric * 100
                     / count(*) filter (where status in ('approved','converted','declined','expired')), 1) end,
        'avg_days_to_decision', round(avg(extract(epoch from (decided_at - sent_at)) / 86400)
                                  filter (where decided_at is not null and sent_at is not null), 1))
      from public.estimates where tenant_id = p_tenant_id and created_at::date between p_from and p_to),
    'recurring', (
      select jsonb_build_object(
        'jobs', count(*),
        'customers', count(distinct client_id),
        'monthly_value', coalesce(round(sum(coalesce(price, 0) * 52.0 / 12 / greatest(interval_weeks, 1)), 2), 0))
      from public.jobs
      where tenant_id = p_tenant_id and kind = 'recurring' and status in ('scheduled','active')
        and coalesce(starts_on, v_today) <= v_today + 30 and (ends_on is null or ends_on >= v_today)),
    'customers', jsonb_build_object(
      'active', (select count(*) from public.clients where tenant_id = p_tenant_id and status = 'active'),
      'served_in_period', (select count(distinct client_id) from public.visits
                           where tenant_id = p_tenant_id and status = 'completed' and scheduled_date between p_from and p_to),
      'new_in_period', (select count(*) from public.clients c where c.tenant_id = p_tenant_id
                          and exists (select 1 from public.visits v where v.client_id = c.id and v.status = 'completed')
                          and (select min(v.scheduled_date) from public.visits v where v.client_id = c.id and v.status = 'completed')
                              between p_from and p_to),
      'lost_in_period', (select count(distinct a.client_id) from public.activity a
                         where a.tenant_id = p_tenant_id and a.kind = 'status_changed'
                           and a.occurred_at::date between p_from and p_to
                           and (a.data->>'to') in ('lost','inactive')),
      'at_risk', (select count(*) from public.at_risk_customers(p_tenant_id))),
    'workload', coalesce((
      select jsonb_agg(jsonb_build_object('date', d::date, 'visits', coalesce(w.n, 0), 'minutes', coalesce(w.mins, 0),
                                          'value', coalesce(w.val, 0), 'unassigned', coalesce(w.un, 0)) order by d)
      from generate_series(v_today, v_today + 13, interval '1 day') d
      left join (
        select v.scheduled_date, count(*) as n, sum(coalesce(v.est_minutes, 0)) as mins, sum(coalesce(v.price, 0)) as val,
               count(*) filter (where v.crew_id is null and not exists (select 1 from public.visit_assignments x where x.visit_id = v.id)) as un
        from public.visits v
        where v.tenant_id = p_tenant_id and v.status in ('scheduled','in_progress') and v.scheduled_date between v_today and v_today + 13
        group by v.scheduled_date
      ) w on w.scheduled_date = d::date), '[]'::jsonb),
    'crews', coalesce((
      select jsonb_agg(jsonb_build_object('crew_id', p.key, 'crew', p.label, 'visits', p.visits, 'revenue', p.revenue,
                                          'labor_cost', p.labor_cost, 'margin', p.margin, 'onsite_hours', round(p.onsite_minutes / 60.0, 1),
                                          'paid_hours', ph.hours,
                                          'utilization_pct', case when ph.hours > 0 then round(p.onsite_minutes / 60.0 / ph.hours * 100, 1) end)
                       order by p.label)
      from public.profitability(p_tenant_id, p_from, p_to, 'crew') p
      left join lateral (
        select round(coalesce(sum(t.worked_seconds), 0) / 3600.0, 1) as hours
        from public.timesheet(p_tenant_id, p_from, p_to) t
        where t.status = 'closed' and t.employee_id in (select cm.employee_id from public.crew_members cm where cm.crew_id = p.key)
      ) ph on true
      where p.key is not null), '[]'::jsonb)
  ) into r;
  return r;
end $$;

-- ---------------------------------------------------------------------------
-- Privileges
-- ---------------------------------------------------------------------------
revoke execute on function private.payroll_cost(uuid, date, date) from public, anon, authenticated;
do $$
declare f text;
begin
  foreach f in array array[
    'public.owner_attention(uuid)', 'public.profitability(uuid, date, date, text)', 'public.at_risk_customers(uuid)',
    'public.receivables(uuid)', 'public.business_health(uuid, date, date)'] loop
    execute format('revoke execute on function %s from public, anon', f);
    execute format('grant execute on function %s to authenticated, service_role', f);
  end loop;
end $$;
