-- Hardening from the 2026-10-06 readiness audit (independent review).
--   M1  customers' photos/visits follow the visit's CURRENT customer; a job's
--       property must belong to its customer; finished work can't be moved
--       to another customer's job.
--   M2  crew read only what their stops need (via schedule()); no prices,
--       CRM notes, other customers, or coworkers' HR records.
--   M3  profiles: each person reads only their own row (protects the other
--       app's billing fields).
--   M4  admins can't raise their own pay, change owners' records, delete sent
--       invoices or edit sent estimates. Invoices and payments are voided, not deleted.
--   L2  only managers can mark a break as paid.
--   L5  no sequence access for anon/authenticated.
--   Dates in dashboard functions use the company's time zone.
-- Visit start/finish accept the phone's time (offline), within the punch window.

-- ===========================================================================
-- M3 profiles
-- ===========================================================================
drop policy if exists profiles_select on public.profiles;
create policy profiles_select on public.profiles for select to authenticated using (id = (select auth.uid()));

-- ===========================================================================
-- M2 least privilege for crew
-- ===========================================================================
drop policy if exists clients_select on public.clients;
create policy clients_select on public.clients for select to authenticated using (public.is_admin_or_owner(tenant_id));
drop policy if exists properties_select on public.properties;
create policy properties_select on public.properties for select to authenticated using (public.is_admin_or_owner(tenant_id));
drop policy if exists jobs_select on public.jobs;
create policy jobs_select on public.jobs for select to authenticated using (public.is_admin_or_owner(tenant_id));
drop policy if exists services_select on public.services;
create policy services_select on public.services for select to authenticated using (public.is_admin_or_owner(tenant_id));
drop policy if exists visits_select on public.visits;
create policy visits_select on public.visits for select to authenticated using (public.is_admin_or_owner(tenant_id));
drop policy if exists visit_assignments_select on public.visit_assignments;
create policy visit_assignments_select on public.visit_assignments for select to authenticated using (public.is_admin_or_owner(tenant_id));
drop policy if exists employees_select on public.employees;
create policy employees_select on public.employees for select to authenticated
  using (public.is_admin_or_owner(tenant_id) or user_id = (select auth.uid()));

-- The crew's window into the schedule: only their own stops, only what the
-- stop needs. Prices are for the office.
create or replace function public.schedule(p_tenant_id uuid, p_from date, p_to date)
returns table (
  visit_id uuid, scheduled_date date, status text, status_reason text, sort_order integer,
  job_id uuid, job_title text, job_kind text, client_id uuid, client_name text, client_phone text,
  property_id uuid, address text, access_notes text, latitude double precision, longitude double precision,
  crew_id uuid, crew_name text, assignees text[], est_minutes integer, price numeric,
  started_at timestamptz, completed_at timestamptz, completion_notes text
)
language plpgsql stable security definer set search_path = '' as $$
declare v_mgr boolean := public.is_admin_or_owner(p_tenant_id);
begin
  if not v_mgr and not public.in_tenant(p_tenant_id) then return; end if;
  if p_to < p_from or p_to - p_from > 400 then raise exception 'invalid_date_range' using errcode = '22023'; end if;
  return query
  select v.id, v.scheduled_date, v.status, v.status_reason, v.sort_order,
         j.id, j.title, j.kind,
         c.id, c.name, c.phone,
         p.id, concat_ws(', ', p.address_line1, p.city), p.access_notes, p.latitude, p.longitude,
         cr.id, cr.name,
         coalesce((select array_agg(e.display_name order by e.display_name)
                   from public.visit_assignments a join public.employees e on e.id = a.employee_id
                   where a.visit_id = v.id), '{}'),
         v.est_minutes, case when v_mgr then v.price end, v.started_at, v.completed_at, v.completion_notes
  from public.visits v
  join public.jobs j on j.id = v.job_id
  left join public.clients c on c.id = v.client_id
  left join public.properties p on p.id = v.property_id
  left join public.crews cr on cr.id = v.crew_id
  where v.tenant_id = p_tenant_id and v.scheduled_date between p_from and p_to
    and (v_mgr or private.is_assigned_to_visit(v.id))
  order by v.scheduled_date, cr.name nulls last, v.sort_order, j.title;
end $$;
revoke execute on function public.schedule(uuid, date, date) from public, anon;
grant execute on function public.schedule(uuid, date, date) to authenticated, service_role;

-- Visit start/finish: narrow result (no price), optional phone time for offline taps.
drop function if exists public.start_visit(uuid);
drop function if exists public.complete_visit(uuid, text);

create or replace function public.start_visit(p_visit_id uuid, p_at timestamptz default null)
returns table (visit_id uuid, status text, started_at timestamptz, completed_at timestamptz)
language plpgsql security definer set search_path = '' as $$
#variable_conflict use_column
declare v public.visits; v_at timestamptz;
begin
  select * into v from public.visits where id = p_visit_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  if not (public.is_admin_or_owner(v.tenant_id) or private.is_assigned_to_visit(v.id)) then
    raise exception 'forbidden' using errcode = '42501';
  end if;
  if v.status = 'scheduled' then
    v_at := private.check_punch_time(p_at);
    update public.visits set status = 'in_progress', started_at = v_at where id = v.id returning * into v;
  elsif v.status <> 'in_progress' then
    raise exception 'visit_not_scheduled' using errcode = '22023';
  end if;
  return query select v.id, v.status, v.started_at, v.completed_at;
end $$;

create or replace function public.complete_visit(p_visit_id uuid, p_notes text default null, p_at timestamptz default null)
returns table (visit_id uuid, status text, started_at timestamptz, completed_at timestamptz)
language plpgsql security definer set search_path = '' as $$
#variable_conflict use_column
declare v public.visits; v_at timestamptz;
begin
  select * into v from public.visits where id = p_visit_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  if not (public.is_admin_or_owner(v.tenant_id) or private.is_assigned_to_visit(v.id)) then
    raise exception 'forbidden' using errcode = '42501';
  end if;
  if v.status <> 'completed' then
    if v.status not in ('scheduled','in_progress') then raise exception 'visit_not_open' using errcode = '22023'; end if;
    -- Office marking a missed visit done after the fact: no time window.
    v_at := case when p_at is not null and public.is_admin_or_owner(v.tenant_id) and p_at <= now() then p_at
                 else private.check_punch_time(p_at) end;
    update public.visits
       set status = 'completed',
           started_at = coalesce(started_at, v_at),
           completed_at = greatest(v_at, coalesce(started_at, v_at)),
           completed_by = auth.uid(),
           completion_notes = nullif(btrim(coalesce(p_notes, '')), '')
     where id = v.id
    returning * into v;
    if v.client_id is not null then
      perform private.log_activity(v.tenant_id, v.client_id, v.property_id, 'visit_completed',
        private.visit_label(v) || ' completed' || coalesce(': ' || v.completion_notes, ''),
        jsonb_build_object('visit_id', v.id, 'date', v.scheduled_date));
    end if;
  end if;
  return query select v.id, v.status, v.started_at, v.completed_at;
end $$;
revoke execute on function public.start_visit(uuid, timestamptz), public.complete_visit(uuid, text, timestamptz) from public, anon;
grant execute on function public.start_visit(uuid, timestamptz), public.complete_visit(uuid, text, timestamptz) to authenticated, service_role;

-- ===========================================================================
-- M1 customer / property / job consistency
-- ===========================================================================
do $$ begin
  alter table public.properties add constraint properties_tenant_client_id_key unique (tenant_id, client_id, id);
exception when duplicate_table or duplicate_object then null; end $$;

do $$ begin
  alter table public.jobs add constraint jobs_property_belongs_to_client
    foreign key (tenant_id, client_id, property_id) references public.properties (tenant_id, client_id, id) not valid;
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.jobs validate constraint jobs_property_belongs_to_client;
exception when foreign_key_violation then
  raise warning 'Some existing jobs point at another customer''s property; constraint left unvalidated for review.';
end $$;

do $$ begin
  alter table public.estimates add constraint estimates_property_belongs_to_client
    foreign key (tenant_id, client_id, property_id) references public.properties (tenant_id, client_id, id) not valid;
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.estimates validate constraint estimates_property_belongs_to_client;
exception when foreign_key_violation then
  raise warning 'Some existing estimates point at another customer''s property; constraint left unvalidated for review.';
end $$;

-- A job with finished visits keeps its customer (history stays with who was served).
-- Open visits follow the job's new customer/property.
create or replace function private.job_customer_guard() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  if new.client_id is distinct from old.client_id
     and exists (select 1 from public.visits v where v.job_id = new.id and v.status in ('completed','skipped','in_progress')) then
    raise exception 'job_has_history' using errcode = '22023';
  end if;
  return new;
end $$;
drop trigger if exists job_customer_guard on public.jobs;
create trigger job_customer_guard before update of client_id on public.jobs
  for each row execute function private.job_customer_guard();

create or replace function private.job_follow_visits() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  update public.visits set job_id = job_id   -- re-runs visit_defaults: customer/property follow the job
  where job_id = new.id and status = 'scheduled';
  return null;
end $$;
drop trigger if exists job_follow_visits on public.jobs;
create trigger job_follow_visits after update of client_id, property_id on public.jobs
  for each row when (old.client_id is distinct from new.client_id or old.property_id is distinct from new.property_id)
  execute function private.job_follow_visits();

create or replace function private.visit_history_guard() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  if old.status in ('completed','skipped') and new.job_id is distinct from old.job_id then
    raise exception 'visit_has_history' using errcode = '22023';
  end if;
  return new;
end $$;
drop trigger if exists visit_history_guard on public.visits;
create trigger visit_history_guard before update of job_id on public.visits
  for each row execute function private.visit_history_guard();

-- Photos follow the visit's current customer, not a copy.
create or replace function private.can_view_visit_photo(p_name text) returns boolean
language sql stable security definer set search_path = '' as $$
  select exists (
    select 1 from public.visit_photos p
    join public.visits v on v.id = p.visit_id
    where p.storage_path = p_name
      and (
        public.is_admin_or_owner(p.tenant_id)
        or private.is_assigned_to_visit(p.visit_id)
        or (p.customer_visible and p.hidden_at is null and v.status = 'completed'
            and exists (select 1 from public.portal_access a
                        where a.client_id = v.client_id and a.tenant_id = v.tenant_id
                          and a.user_id = (select auth.uid()) and a.status = 'active'))
      )
  )
$$;

create or replace function public.portal_visit_photos(p_client_id uuid, p_visit_id uuid default null)
returns table (photo_id uuid, visit_id uuid, visit_date date, service text, kind text, caption text, storage_path text, taken_at timestamptz)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_portal_client(p_client_id);
  return query
  select p.id, p.visit_id, v.scheduled_date, coalesce(s.name, j.title), p.kind, p.caption, p.storage_path, p.taken_at
  from public.visit_photos p
  join public.visits v on v.id = p.visit_id and v.status = 'completed' and v.client_id = p_client_id
  join public.jobs j on j.id = v.job_id
  left join public.services s on s.id = j.service_id
  where p.customer_visible and p.hidden_at is null
    and (p_visit_id is null or p.visit_id = p_visit_id)
  order by v.scheduled_date desc, p.kind desc, p.taken_at
  limit 200;
end $$;

-- ===========================================================================
-- M4 money and people controls
-- ===========================================================================
-- Pay: owners set anyone's; admins set others' but never their own.
drop policy if exists pay_insert on public.employee_pay_rates;
create policy pay_insert on public.employee_pay_rates for insert to authenticated
  with check (public.is_owner(tenant_id)
              or (public.is_admin_or_owner(tenant_id) and employee_id is distinct from public.my_employee_id(tenant_id)));

-- Employees: managers edit profile fields; admins can't touch owners' records; links to logins are system-managed.
revoke update on public.employees from authenticated;
grant update (display_name, email, phone, status, hired_on, notes) on public.employees to authenticated;
drop policy if exists employees_update on public.employees;
create policy employees_update on public.employees for update to authenticated
  using (public.is_admin_or_owner(tenant_id)
         and (public.is_owner(tenant_id) or user_id is null
              or not exists (select 1 from public.memberships m where m.tenant_id = employees.tenant_id
                             and m.user_id = employees.user_id and m.role = 'owner')))
  with check (public.is_admin_or_owner(tenant_id));

-- Sent estimates are a record of what the customer was offered.
revoke update on public.estimates from authenticated;
grant update (notes, valid_until) on public.estimates to authenticated;
drop policy if exists estimates_delete on public.estimates;
create policy estimates_delete on public.estimates for delete to authenticated
  using (public.is_admin_or_owner(tenant_id) and status = 'draft');

-- Invoices and payments are voided, never deleted.
revoke delete on public.invoices from authenticated;
alter table public.invoices add column if not exists voided_at timestamptz;
alter table public.invoices add column if not exists void_reason text;
alter table public.invoice_lines add column if not exists voided_visit_id uuid;

create or replace function private.guard_draft_lines() returns trigger
language plpgsql security definer set search_path = '' as $$
declare v_status text;
begin
  if tg_table_name = 'estimate_lines' then
    select status into v_status from public.estimates where id = coalesce(new.estimate_id, old.estimate_id);
  else
    select status into v_status from public.invoices where id = coalesce(new.invoice_id, old.invoice_id);
    -- Voiding releases the visits so they can be billed again; nothing else changes.
    if tg_op = 'UPDATE' and current_setting('app.voiding_invoice', true) = old.invoice_id::text
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
  perform set_config('app.voiding_invoice', inv.id::text, true);
  update public.invoice_lines set voided_visit_id = visit_id, visit_id = null where invoice_id = inv.id and visit_id is not null;
  perform set_config('app.voiding_invoice', '', true);
  update public.invoices set status = 'void', voided_at = now(), void_reason = btrim(p_reason) where id = inv.id;
  perform private.log_activity(inv.tenant_id, inv.client_id, null, 'invoice_voided',
    format('Invoice %s voided: %s', coalesce(inv.number, ''), btrim(p_reason)), jsonb_build_object('invoice_id', inv.id));
  perform set_config('app.audit_reason', '', true);
end $$;

create or replace function public.void_payment(p_payment_id uuid, p_reason text) returns void
language plpgsql security definer set search_path = '' as $$
declare pm public.payments; inv public.invoices;
begin
  select * into pm from public.payments where id = p_payment_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(pm.tenant_id);
  if coalesce(btrim(p_reason), '') = '' then raise exception 'reason_required' using errcode = '22023'; end if;
  if pm.voided_at is not null then return; end if;
  perform set_config('app.audit_reason', 'void payment: ' || btrim(p_reason), true);
  update public.payments set voided_at = now(), void_reason = btrim(p_reason) where id = pm.id;
  perform private.recalc_invoice(pm.invoice_id);
  select * into inv from public.invoices where id = pm.invoice_id;
  perform private.log_activity(inv.tenant_id, inv.client_id, null, 'payment_voided',
    format('Payment of $%s on %s voided: %s', to_char(pm.amount, 'FM999,999,990.00'), coalesce(inv.number, 'invoice'), btrim(p_reason)),
    jsonb_build_object('invoice_id', inv.id, 'payment_id', pm.id));
  perform set_config('app.audit_reason', '', true);
end $$;

-- ===========================================================================
-- L2 paid breaks are a manager decision
-- ===========================================================================
create or replace function public.start_break(
  p_tenant_id uuid, p_client_event_id uuid default null, p_at timestamptz default null, p_paid boolean default false
) returns public.time_entry_breaks
language plpgsql security definer set search_path = '' as $$
declare
  v_emp uuid := private.require_self_employee(p_tenant_id);
  v_at timestamptz;
  v_entry public.time_entries;
  v_row public.time_entry_breaks;
begin
  if p_client_event_id is not null then
    select * into v_row from public.time_entry_breaks where tenant_id = p_tenant_id and client_event_id = p_client_event_id;
    if found and v_row.employee_id = v_emp then return v_row; end if;
  end if;
  v_at := private.check_punch_time(p_at);
  select * into v_entry from public.time_entries
   where employee_id = v_emp and clock_out is null and voided_at is null and not needs_review;
  if not found then raise exception 'not_clocked_in' using errcode = '22023'; end if;
  if v_at < v_entry.clock_in then raise exception 'break_before_clock_in' using errcode = '22023'; end if;
  begin
    insert into public.time_entry_breaks (tenant_id, time_entry_id, employee_id, started_at, paid, client_event_id)
    values (p_tenant_id, v_entry.id, v_emp, v_at, coalesce(p_paid, false) and public.is_admin_or_owner(p_tenant_id), p_client_event_id)
    returning * into v_row;
  exception when unique_violation then
    raise exception 'already_on_break' using errcode = '23P01';
  end;
  return v_row;
end $$;

-- ===========================================================================
-- L5 sequences
-- ===========================================================================
revoke all on all sequences in schema public from anon, authenticated;

-- ===========================================================================
-- Company-local dates in dashboard functions
-- ===========================================================================
create or replace function private.tenant_date(p_tenant_id uuid, p_ts timestamptz) returns date
language sql stable security definer set search_path = '' as $$
  select (p_ts at time zone coalesce((select timezone from public.tenants where id = p_tenant_id), 'America/New_York'))::date
$$;

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
           (v_today - private.tenant_date(p_tenant_id, inv.due_at)) || ' days past due',
           'invoice', inv.id, private.tenant_date(p_tenant_id, inv.due_at), inv.client_id, inv.total - inv.amount_paid
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
           'message', m.id, private.tenant_date(p_tenant_id, m.created_at), m.client_id, null
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

create or replace function public.receivables(p_tenant_id uuid)
returns table (invoice_id uuid, number text, client_id uuid, client_name text, sent_on date, due_on date,
               balance numeric, days_overdue integer, bucket text)
language plpgsql stable security definer set search_path = '' as $$
declare v_today date;
begin
  perform private.require_manager(p_tenant_id);
  v_today := private.tenant_today(p_tenant_id);
  return query
  select i.id, i.number, i.client_id, c.name, private.tenant_date(p_tenant_id, i.sent_at), private.tenant_date(p_tenant_id, i.due_at),
         round(i.total - i.amount_paid, 2),
         greatest(0, v_today - coalesce(private.tenant_date(p_tenant_id, i.due_at), v_today))::int,
         case when i.due_at is null or private.tenant_date(p_tenant_id, i.due_at) >= v_today then 'current'
              when v_today - private.tenant_date(p_tenant_id, i.due_at) <= 30 then '1-30'
              when v_today - private.tenant_date(p_tenant_id, i.due_at) <= 60 then '31-60'
              when v_today - private.tenant_date(p_tenant_id, i.due_at) <= 90 then '61-90'
              else '90+' end
  from public.invoices i left join public.clients c on c.id = i.client_id
  where i.tenant_id = p_tenant_id and i.status in ('sent','partial','overdue') and i.total - i.amount_paid > 0
  order by i.due_at nulls last;
end $$;

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
end $$;

-- ===========================================================================
-- Indexes for the dashboard and customer pages
-- ===========================================================================
create index if not exists visits_client_status_date_idx on public.visits (client_id, status, scheduled_date);
create index if not exists activity_client_occurred_idx on public.activity (client_id, occurred_at desc);
create index if not exists invoices_client_idx on public.invoices (client_id);
create index if not exists estimates_client_idx on public.estimates (client_id);
create index if not exists expenses_visit_idx on public.expenses (visit_id) where visit_id is not null;
create index if not exists payments_tenant_received_idx on public.payments (tenant_id, received_on);

-- ===========================================================================
-- Privileges for new/changed functions
-- ===========================================================================
revoke execute on function private.job_customer_guard(), private.job_follow_visits(), private.visit_history_guard(),
  private.tenant_date(uuid, timestamptz) from public, anon, authenticated;
revoke execute on function public.void_invoice(uuid, text), public.void_payment(uuid, text) from public, anon;
grant execute on function public.void_invoice(uuid, text), public.void_payment(uuid, text) to authenticated, service_role;
