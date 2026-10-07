-- Second readiness audit: money and people controls.
--
-- 1. Admins can't set pay for themselves or for an owner, by any route (the
--    old rule matched only an *active* employee record, so deactivating your
--    own record got around it).
-- 2. Admins can't add, correct or void their OWN hours; an owner has to.
--    Owners may correct anyone, including themselves (it is their payroll).
-- 3. Breaks can be corrected by a manager (forgotten "End break", wrong
--    times, or mark paid), with a reason, audited. Nothing is deleted.
-- 4. Expenses are voided with a reason, never deleted; only the business
--    fields can be edited; who/when recorded is set by the server.
-- 5. Voiding an invoice marks it void before releasing its visits, and the
--    line guard only allows that release on a void invoice.

-- 1 -------------------------------------------------------------------------
create or replace function private.is_owner_employee(p_employee_id uuid) returns boolean
language sql stable security definer set search_path = '' as $$
  select exists (select 1 from public.employees e join public.memberships m
                   on m.tenant_id = e.tenant_id and m.user_id = e.user_id and m.role = 'owner'
                 where e.id = p_employee_id)
$$;
create or replace function private.is_own_employee(p_employee_id uuid) returns boolean
language sql stable security definer set search_path = '' as $$
  select exists (select 1 from public.employees e where e.id = p_employee_id and e.user_id = (select auth.uid()))
$$;
revoke execute on function private.is_owner_employee(uuid), private.is_own_employee(uuid) from public, anon;
grant execute on function private.is_owner_employee(uuid), private.is_own_employee(uuid) to authenticated;

drop policy if exists pay_insert on public.employee_pay_rates;
create policy pay_insert on public.employee_pay_rates for insert to authenticated
  with check (public.is_owner(tenant_id)
              or (public.is_admin_or_owner(tenant_id)
                  and not private.is_own_employee(employee_id)
                  and not private.is_owner_employee(employee_id)));

-- 2 -------------------------------------------------------------------------
create or replace function private.require_can_edit_hours(p_tenant_id uuid, p_employee_id uuid) returns void
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_manager(p_tenant_id);
  if not public.is_owner(p_tenant_id)
     and (private.is_own_employee(p_employee_id) or private.is_owner_employee(p_employee_id)) then
    raise exception 'owner_must_approve' using errcode = '42501';
  end if;
end $$;
revoke execute on function private.require_can_edit_hours(uuid, uuid) from public, anon, authenticated;

create or replace function public.correct_time_entry(
  p_entry_id uuid, p_clock_in timestamptz, p_clock_out timestamptz, p_reason text
) returns public.time_entries
language plpgsql security definer set search_path = '' as $$
declare
  v_row public.time_entries;
  v_reason text := private.require_reason(p_reason);
begin
  select * into v_row from public.time_entries where id = p_entry_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_can_edit_hours(v_row.tenant_id, v_row.employee_id);
  if v_row.voided_at is not null then raise exception 'entry_voided' using errcode = '22023'; end if;
  if p_clock_in is null then raise exception 'clock_in_required' using errcode = '22023'; end if;
  if p_clock_out is not null and p_clock_out <= p_clock_in then
    raise exception 'clock_out_before_clock_in' using errcode = '22023';
  end if;
  if p_clock_in > now() + interval '5 minutes' or p_clock_out > now() + interval '5 minutes' then
    raise exception 'punch_in_future' using errcode = '22023';
  end if;

  perform set_config('app.audit_reason', 'correct_time_entry: ' || v_reason, true);
  begin
    update public.time_entries
       set clock_in = p_clock_in, clock_out = p_clock_out,
           needs_review = false, review_note = null
     where id = p_entry_id
    returning * into v_row;
  exception
    when exclusion_violation then raise exception 'overlaps_existing_shift' using errcode = '23P01';
    when unique_violation then raise exception 'employee_already_has_open_shift' using errcode = '23P01';
  end;
  -- A break can't outlast the shift it belongs to.
  update public.time_entry_breaks set ended_at = p_clock_out
   where time_entry_id = p_entry_id and p_clock_out is not null and (ended_at is null or ended_at > p_clock_out)
     and started_at < p_clock_out;
  return v_row;
end $$;

create or replace function public.add_time_entry(
  p_tenant_id uuid, p_employee_id uuid, p_clock_in timestamptz, p_clock_out timestamptz,
  p_reason text, p_job_id uuid default null, p_notes text default null
) returns public.time_entries
language plpgsql security definer set search_path = '' as $$
declare
  v_reason text := private.require_reason(p_reason);
  v_row public.time_entries;
begin
  perform private.require_manager(p_tenant_id);
  if not exists (select 1 from public.employees where id = p_employee_id and tenant_id = p_tenant_id) then
    raise exception 'employee_not_found' using errcode = 'P0002';
  end if;
  perform private.require_can_edit_hours(p_tenant_id, p_employee_id);
  if p_clock_in is null or p_clock_out is null then
    raise exception 'clock_in_and_clock_out_required' using errcode = '22023';
  end if;
  if p_clock_out <= p_clock_in then raise exception 'clock_out_before_clock_in' using errcode = '22023'; end if;
  if p_clock_out > now() + interval '5 minutes' then raise exception 'punch_in_future' using errcode = '22023'; end if;

  perform set_config('app.audit_reason', 'add_time_entry: ' || v_reason, true);
  begin
    insert into public.time_entries
      (tenant_id, employee_id, user_id, job_id, clock_in, clock_out, notes, source, received_at, clock_out_received_at, created_by)
    values
      (p_tenant_id, p_employee_id, (select user_id from public.employees where id = p_employee_id),
       p_job_id, p_clock_in, p_clock_out, p_notes, 'admin', now(), now(), auth.uid())
    returning * into v_row;
  exception when exclusion_violation then
    raise exception 'overlaps_existing_shift' using errcode = '23P01';
  end;
  return v_row;
end $$;

create or replace function public.void_time_entry(p_entry_id uuid, p_reason text) returns public.time_entries
language plpgsql security definer set search_path = '' as $$
declare
  v_reason text := private.require_reason(p_reason);
  v_row public.time_entries;
begin
  select * into v_row from public.time_entries where id = p_entry_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_can_edit_hours(v_row.tenant_id, v_row.employee_id);
  if v_row.voided_at is not null then return v_row; end if;
  perform set_config('app.audit_reason', 'void_time_entry: ' || v_reason, true);
  update public.time_entries
     set voided_at = now(), voided_by = auth.uid(), void_reason = v_reason
   where id = p_entry_id
  returning * into v_row;
  return v_row;
end $$;

-- 3 -------------------------------------------------------------------------
create or replace function public.correct_break(
  p_break_id uuid, p_started_at timestamptz, p_ended_at timestamptz, p_paid boolean, p_reason text
) returns public.time_entry_breaks
language plpgsql security definer set search_path = '' as $$
declare
  v_reason text := private.require_reason(p_reason);
  b public.time_entry_breaks;
  te public.time_entries;
begin
  select * into b from public.time_entry_breaks where id = p_break_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_can_edit_hours(b.tenant_id, b.employee_id);
  select * into te from public.time_entries where id = b.time_entry_id;
  if te.voided_at is not null then raise exception 'entry_voided' using errcode = '22023'; end if;
  if p_started_at is null then raise exception 'date_required' using errcode = '22023'; end if;
  if p_ended_at is not null and p_ended_at <= p_started_at then
    raise exception 'break_end_before_start' using errcode = '22023';
  end if;
  if p_started_at < te.clock_in or (te.clock_out is not null and (p_ended_at is null or p_ended_at > te.clock_out)) then
    raise exception 'break_outside_shift' using errcode = '22023';
  end if;
  perform set_config('app.audit_reason', 'correct_break: ' || v_reason, true);
  update public.time_entry_breaks
     set started_at = p_started_at, ended_at = p_ended_at, paid = coalesce(p_paid, paid)
   where id = p_break_id
  returning * into b;
  return b;
end $$;
revoke execute on function public.correct_break(uuid, timestamptz, timestamptz, boolean, text) from public, anon;
grant execute on function public.correct_break(uuid, timestamptz, timestamptz, boolean, text) to authenticated;

-- 4 -------------------------------------------------------------------------
alter table public.expenses add column if not exists voided_at timestamptz;
alter table public.expenses add column if not exists voided_by uuid references auth.users (id) on delete set null;
alter table public.expenses add column if not exists void_reason text;

drop policy if exists expenses_delete on public.expenses;
revoke delete on public.expenses from authenticated;
revoke insert, update on public.expenses from authenticated;
grant insert (tenant_id, category, amount, spent_at, note, visit_id, job_id, client_id) on public.expenses to authenticated;
grant update (category, amount, spent_at, note, visit_id, job_id, client_id) on public.expenses to authenticated;
drop policy if exists expenses_update on public.expenses;
create policy expenses_update on public.expenses for update to authenticated
  using (public.is_admin_or_owner(tenant_id) and voided_at is null)
  with check (public.is_admin_or_owner(tenant_id));

create or replace function public.void_expense(p_expense_id uuid, p_reason text) returns void
language plpgsql security definer set search_path = '' as $$
declare
  e public.expenses;
  v_reason text := private.require_reason(p_reason);
begin
  select * into e from public.expenses where id = p_expense_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(e.tenant_id);
  if e.voided_at is not null then return; end if;
  perform set_config('app.audit_reason', 'void expense: ' || v_reason, true);
  update public.expenses set voided_at = now(), voided_by = auth.uid(), void_reason = v_reason where id = e.id;
end $$;
revoke execute on function public.void_expense(uuid, text) from public, anon;
grant execute on function public.void_expense(uuid, text) to authenticated;

-- Totals ignore voided expenses (same functions, one added condition each).
CREATE OR REPLACE FUNCTION public.business_health(p_tenant_id uuid, p_from date, p_to date)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
AS $function$
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
  select coalesce(sum(amount), 0) into v_exp from public.expenses where tenant_id = p_tenant_id and spent_at between p_from and p_to and voided_at is null;

  select jsonb_build_object(
    'from', p_from, 'to', p_to, 'today', v_today,
    'money', jsonb_build_object(
      'work_done', round(vc_rev, 2),
      'completed_visits', vc_n,
      'billed', (select coalesce(round(sum(total), 2), 0) from public.invoices
                 where tenant_id = p_tenant_id and status <> 'void' and sent_at is not null
                   and private.tenant_date(p_tenant_id, sent_at) between p_from and p_to),
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
      from public.estimates where tenant_id = p_tenant_id and private.tenant_date(p_tenant_id, created_at) between p_from and p_to),
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
                           and private.tenant_date(p_tenant_id, a.occurred_at) between p_from and p_to
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
end $function$;

CREATE OR REPLACE FUNCTION public.profitability(p_tenant_id uuid, p_from date, p_to date, p_by text)
 RETURNS TABLE(key uuid, label text, sub text, visits integer, revenue numeric, labor_cost numeric, expenses numeric, margin numeric, margin_pct numeric, onsite_minutes integer, missing_rates integer)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
AS $function$
begin
  perform private.require_manager(p_tenant_id);
  if p_by not in ('customer','property','service','job','crew') then raise exception 'invalid_group' using errcode = '22023'; end if;
  return query
  with vc as (select * from public.visit_costing(p_tenant_id, p_from, p_to)),
  rev as (
    select case p_by when 'customer' then vc.client_id when 'property' then vc.property_id when 'service' then j.service_id
                     when 'job' then vc.job_id else v.crew_id end as k,
           count(*)::int as n, sum(vc.revenue) as rev, sum(vc.labor_cost) as lab,
           sum(vc.onsite_minutes)::int as mins, sum(vc.missing_rates)::int as miss
    from vc join public.visits v on v.id = vc.visit_id join public.jobs j on j.id = vc.job_id
    group by 1
  ),
  ex as (
    select case p_by when 'customer' then e.client_id
                     when 'property' then coalesce(v.property_id, j.property_id)
                     when 'service' then j.service_id
                     when 'job' then e.job_id
                     else coalesce(v.crew_id, j.crew_id) end as k,
           sum(e.amount) as amt
    from public.expenses e
    left join public.visits v on v.id = e.visit_id
    left join public.jobs j on j.id = e.job_id
    where e.tenant_id = p_tenant_id and e.spent_at between p_from and p_to and e.voided_at is null
      and (e.visit_id is not null or e.job_id is not null or e.client_id is not null)
    group by 1
  ),
  g as (
    select u.k, sum(u.n)::int as n, sum(u.rev) as rev, sum(u.lab) as lab, sum(u.ex) as ex,
           sum(u.mins)::int as mins, sum(u.miss)::int as miss
    from (
      select r.k, r.n, r.rev, r.lab, 0::numeric as ex, r.mins, r.miss from rev r
      union all
      select x.k, 0, 0, 0, x.amt, 0, 0 from ex x
    ) u
    group by u.k
  )
  select g.k,
         case p_by
           when 'customer' then coalesce((select c.name from public.clients c where c.id = g.k), 'No customer')
           when 'property' then coalesce((select p.address_line1 from public.properties p where p.id = g.k), 'Not tied to a property')
           when 'service' then coalesce((select s.name from public.services s where s.id = g.k), 'No service set')
           when 'job' then coalesce((select j.title from public.jobs j where j.id = g.k), 'Not tied to a job')
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
end $function$;

-- 5 -------------------------------------------------------------------------
create or replace function private.guard_draft_lines() returns trigger
language plpgsql security definer set search_path = '' as $$
declare v_status text;
begin
  if tg_table_name = 'estimate_lines' then
    select status into v_status from public.estimates where id = coalesce(new.estimate_id, old.estimate_id);
  else
    select status into v_status from public.invoices where id = coalesce(new.invoice_id, old.invoice_id);
    -- Releasing visits is allowed only on an invoice that void_invoice has already voided.
    if tg_op = 'UPDATE' and v_status = 'void'
       and current_setting('app.voiding_invoice', true) = old.invoice_id::text
       and new.visit_id is null and new.voided_visit_id is not distinct from old.visit_id
       and (new.description, new.quantity, new.unit_price) = (old.description, old.quantity, old.unit_price) then
      return new;
    end if;
  end if;
  if v_status is distinct from 'draft' then
    raise exception 'document_not_draft' using errcode = '22023';
  end if;
  return coalesce(new, old);
end $$;

create or replace function public.void_invoice(p_invoice_id uuid, p_reason text) returns void
language plpgsql security definer set search_path = '' as $$
declare inv public.invoices;
begin
  select * into inv from public.invoices where id = p_invoice_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(inv.tenant_id);
  if coalesce(btrim(p_reason), '') = '' then raise exception 'reason_required' using errcode = '22023'; end if;
  if inv.status = 'void' then return; end if;
  if exists (select 1 from public.payments where invoice_id = inv.id and voided_at is null) then
    raise exception 'invoice_has_payments' using errcode = '22023';
  end if;
  perform set_config('app.audit_reason', 'void invoice: ' || btrim(p_reason), true);
  update public.invoices set status = 'void', voided_at = now(), void_reason = btrim(p_reason) where id = inv.id;
  perform set_config('app.voiding_invoice', inv.id::text, true);
  update public.invoice_lines set voided_visit_id = visit_id, visit_id = null where invoice_id = inv.id and visit_id is not null;
  perform set_config('app.voiding_invoice', '', true);
  perform private.log_activity(inv.tenant_id, inv.client_id, null, 'invoice_voided',
    format('Invoice %s voided: %s', coalesce(inv.number, ''), btrim(p_reason)), jsonb_build_object('invoice_id', inv.id));
  perform set_config('app.audit_reason', '', true);
end $$;
