-- Field sync problems: when a crew phone's saved action is refused by the
-- server (punch too old, stop canceled by the office, ...), the phone keeps it
-- on its own problem list AND reports it here, so the office sees it on Today
-- and can fix the hours. Reported by the person themselves; resolved by a manager.

create table if not exists public.sync_problems (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  employee_id uuid,
  user_id uuid references auth.users (id) on delete set null,
  action_kind text not null check (action_kind ~ '^[a-z_]{2,40}$'),
  action_label text not null check (length(action_label) between 1 and 80),
  happened_at timestamptz not null,
  error text not null check (length(error) <= 500),
  client_event_id uuid,
  reported_at timestamptz not null default now(),
  resolved_at timestamptz,
  resolved_by uuid references auth.users (id) on delete set null,
  resolution text,
  unique (tenant_id, id),
  foreign key (tenant_id, employee_id) references public.employees (tenant_id, id)
);
create unique index if not exists sync_problems_event_uidx on public.sync_problems (tenant_id, client_event_id) where client_event_id is not null;
create index if not exists sync_problems_open_idx on public.sync_problems (tenant_id) where resolved_at is null;
alter table public.sync_problems enable row level security;
revoke all on public.sync_problems from anon, authenticated;
grant select on public.sync_problems to authenticated;
drop policy if exists sync_problems_select on public.sync_problems;
create policy sync_problems_select on public.sync_problems for select to authenticated
  using (public.is_admin_or_owner(tenant_id) or user_id = (select auth.uid()));
drop trigger if exists prevent_tenant_change on public.sync_problems;
create trigger prevent_tenant_change before update on public.sync_problems for each row execute function private.prevent_tenant_change();
drop trigger if exists audit_row_change on public.sync_problems;
create trigger audit_row_change after insert or update or delete on public.sync_problems for each row execute function private.audit_row_change();

create or replace function public.report_sync_problem(
  p_tenant_id uuid, p_kind text, p_at timestamptz, p_error text, p_client_event_id uuid default null
) returns void
language plpgsql security definer set search_path = '' as $$
declare v_emp uuid := private.require_self_employee(p_tenant_id);
begin
  insert into public.sync_problems (tenant_id, employee_id, user_id, action_kind, action_label, happened_at, error, client_event_id)
  values (p_tenant_id, v_emp, auth.uid(), coalesce(nullif(p_kind, ''), 'action'),
          case p_kind when 'in' then 'Clock in' when 'out' then 'Clock out' when 'break_start' then 'Break start'
                      when 'break_end' then 'Break end' when 'start_visit' then 'Start stop' when 'complete_visit' then 'Finish stop'
                      when 'report_problem' then 'Couldn''t do stop' else 'Action' end,
          least(coalesce(p_at, now()), now()), left(coalesce(p_error, 'refused'), 500), p_client_event_id)
  on conflict (tenant_id, client_event_id) where client_event_id is not null do nothing;
end $$;
revoke execute on function public.report_sync_problem(uuid, text, timestamptz, text, uuid) from public, anon;
grant execute on function public.report_sync_problem(uuid, text, timestamptz, text, uuid) to authenticated;

create or replace function public.resolve_sync_problem(p_id uuid, p_resolution text) returns void
language plpgsql security definer set search_path = '' as $$
declare sp public.sync_problems; v_note text := private.require_reason(p_resolution);
begin
  select * into sp from public.sync_problems where id = p_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(sp.tenant_id);
  update public.sync_problems set resolved_at = now(), resolved_by = auth.uid(), resolution = v_note where id = p_id and resolved_at is null;
end $$;
revoke execute on function public.resolve_sync_problem(uuid, text) from public, anon;
grant execute on function public.resolve_sync_problem(uuid, text) to authenticated;

CREATE OR REPLACE FUNCTION public.owner_attention(p_tenant_id uuid)
 RETURNS TABLE(kind text, severity integer, title text, detail text, ref_type text, ref_id uuid, ref_date date, client_id uuid, amount numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
AS $function$
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
    -- Crew couldn't do a stop
    select 'crew_skipped', 1, 'Not serviced: ' || coalesce(c.name, j.title), to_char(v.scheduled_date, 'Dy Mon FMDD') || ' · ' || substr(v.status_reason, 7),
           'visit', v.id, v.scheduled_date, v.client_id, v.price
    from public.visits v join public.jobs j on j.id = v.job_id left join public.clients c on c.id = v.client_id
    where v.tenant_id = p_tenant_id and v.status = 'skipped' and v.status_reason like 'Crew: %'
      and v.scheduled_date between v_today - 3 and v_today
    union all
    -- Something a crew phone saved couldn't be recorded (e.g. a punch too old to accept)
    select 'sync_problem', 1, 'Phone couldn''t record: ' || coalesce(e.display_name, 'someone'),
           sp.action_label || ' at ' || to_char(sp.happened_at at time zone tz.timezone, 'Dy FMHH12:MI am') || ' · ' || left(sp.error, 80),
           'time_entry', sp.id, private.tenant_date(p_tenant_id, sp.happened_at), null, null
    from public.sync_problems sp
    left join public.employees e on e.id = sp.employee_id
    cross join (select t.timezone from public.tenants t where t.id = p_tenant_id) tz
    where sp.tenant_id = p_tenant_id and sp.resolved_at is null
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
end $function$;
