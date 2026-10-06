-- Field: crew can report a stop they couldn't do (locked gate, dog, rain).
-- It's marked not serviced, written to customer history, and lands on the
-- owner's attention list so the office can reschedule.

create or replace function public.report_visit_problem(p_visit_id uuid, p_reason text, p_at timestamptz default null)
returns table (visit_id uuid, status text)
language plpgsql security definer set search_path = ''
as $$
#variable_conflict use_column
declare v public.visits; v_reason text := private.require_reason(p_reason);
begin
  select * into v from public.visits where id = p_visit_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  if not (public.is_admin_or_owner(v.tenant_id) or private.is_assigned_to_visit(v.id)) then
    raise exception 'forbidden' using errcode = '42501';
  end if;
  if v.status = 'skipped' then return query select v.id, v.status; return; end if;   -- offline retry
  if v.status not in ('scheduled','in_progress') then raise exception 'visit_not_open' using errcode = '22023'; end if;
  perform private.check_punch_time(p_at);
  update public.visits set status = 'skipped', status_reason = 'Crew: ' || v_reason where id = v.id returning * into v;
  if v.client_id is not null then
    perform private.log_activity(v.tenant_id, v.client_id, v.property_id, 'visit_not_serviced',
      format('%s on %s not serviced: %s', private.visit_label(v), to_char(v.scheduled_date, 'Mon FMDD'), v_reason),
      jsonb_build_object('visit_id', v.id));
  end if;
  return query select v.id, v.status;
end $$;
revoke execute on function public.report_visit_problem(uuid, text, timestamptz) from public, anon;
grant execute on function public.report_visit_problem(uuid, text, timestamptz) to authenticated, service_role;

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
    -- Crew couldn't do a stop
    select 'crew_skipped', 1, 'Not serviced: ' || coalesce(c.name, j.title), to_char(v.scheduled_date, 'Dy Mon FMDD') || ' · ' || substr(v.status_reason, 7),
           'visit', v.id, v.scheduled_date, v.client_id, v.price
    from public.visits v join public.jobs j on j.id = v.job_id left join public.clients c on c.id = v.client_id
    where v.tenant_id = p_tenant_id and v.status = 'skipped' and v.status_reason like 'Crew: %'
      and v.scheduled_date between v_today - 3 and v_today
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

-- Crew membership saved in one step (a failed save never leaves a crew empty).
create or replace function public.set_crew_members(p_crew_id uuid, p_employee_ids uuid[], p_lead_id uuid default null)
returns integer
language plpgsql security definer set search_path = '' as $$
declare c public.crews; n integer;
begin
  select * into c from public.crews where id = p_crew_id;
  if not found then raise exception 'crew_not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(c.tenant_id);
  if exists (select 1 from unnest(coalesce(p_employee_ids, '{}')) e(id)
             where not exists (select 1 from public.employees x where x.id = e.id and x.tenant_id = c.tenant_id and x.status = 'active')) then
    raise exception 'employee_not_found' using errcode = 'P0002';
  end if;
  if p_lead_id is not null and not (p_lead_id = any (coalesce(p_employee_ids, '{}'))) then
    raise exception 'lead_not_on_crew' using errcode = '22023';
  end if;
  delete from public.crew_members where crew_id = c.id and not (employee_id = any (coalesce(p_employee_ids, '{}')));
  insert into public.crew_members (tenant_id, crew_id, employee_id, is_lead)
  select c.tenant_id, c.id, e.id, e.id = p_lead_id from unnest(coalesce(p_employee_ids, '{}')) e(id)
  on conflict (crew_id, employee_id) do update set is_lead = excluded.is_lead;
  get diagnostics n = row_count;
  return n;
end $$;
revoke execute on function public.set_crew_members(uuid, uuid[], uuid) from public, anon;
grant execute on function public.set_crew_members(uuid, uuid[], uuid) to authenticated, service_role;
