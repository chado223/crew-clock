-- Owner overview: the numbers and to-dos for the office's Today page, in one
-- call. Read-only, managers only, company-local dates.

create or replace function public.owner_overview(p_tenant_id uuid) returns jsonb
language plpgsql stable security definer set search_path = '' as $$
declare
  v_today date;
  v_week date;
  v_month date;
  v_tz text;
  r jsonb;
begin
  perform private.require_manager(p_tenant_id);
  select coalesce(timezone, 'America/New_York') into v_tz from public.tenants where id = p_tenant_id;
  v_today := private.tenant_today(p_tenant_id);
  -- Week starts on the company's week_start_day (ISO: 1 = Monday ... 7 = Sunday).
  v_week := v_today - ((extract(isodow from v_today)::int
            - coalesce((select week_start_day from public.tenants where id = p_tenant_id), 1) + 7) % 7);
  v_month := date_trunc('month', v_today)::date;

  select jsonb_build_object(
    'today', v_today,
    'week_start', v_week,
    'visits_today', (
      select jsonb_build_object(
        'total', count(*) filter (where status <> 'canceled'),
        'done', count(*) filter (where status = 'completed'),
        'working', count(*) filter (where status = 'in_progress'),
        'left', count(*) filter (where status = 'scheduled'),
        'skipped', count(*) filter (where status = 'skipped'),
        'value_done', coalesce(sum(price) filter (where status = 'completed'), 0))
      from public.visits where tenant_id = p_tenant_id and scheduled_date = v_today),
    'crews_today', coalesce((
      select jsonb_agg(jsonb_build_object('crew_id', x.crew_id, 'crew', x.name, 'done', x.done, 'total', x.total) order by x.name)
      from (
        select v.crew_id, coalesce(c.name, 'No crew') as name,
               count(*) filter (where v.status in ('completed','skipped')) as done,
               count(*) filter (where v.status <> 'canceled') as total
        from public.visits v left join public.crews c on c.id = v.crew_id
        where v.tenant_id = p_tenant_id and v.scheduled_date = v_today
        group by v.crew_id, c.name
      ) x), '[]'::jsonb),
    'tomorrow_unassigned', (select count(*) from public.visits
      where tenant_id = p_tenant_id and scheduled_date = v_today + 1 and status = 'scheduled' and crew_id is null
        and not exists (select 1 from public.visit_assignments a where a.visit_id = visits.id)),
    'weather_alerts', (select count(*) from public.weather_alerts
      where tenant_id = p_tenant_id and status = 'open' and forecast_date >= v_today),
    'new_requests', (select count(*) from public.service_requests where tenant_id = p_tenant_id and status = 'new'),
    'leads', (select count(*) from public.clients where tenant_id = p_tenant_id and status = 'lead'),
    'estimates_waiting', (
      select jsonb_build_object('count', count(*), 'value', coalesce(sum(subtotal), 0),
                                'expiring_soon', count(*) filter (where valid_until between v_today and v_today + 7))
      from public.estimates where tenant_id = p_tenant_id and status = 'sent'),
    'estimates_approved_not_scheduled', (select count(*) from public.estimates
      where tenant_id = p_tenant_id and status = 'approved'),
    'receivables', (
      select jsonb_build_object(
        'open', coalesce(sum(total - amount_paid), 0),
        'open_count', count(*),
        'overdue', coalesce(sum(total - amount_paid) filter (where due_at < now()), 0),
        'overdue_count', count(*) filter (where due_at < now()))
      from public.invoices
      where tenant_id = p_tenant_id and status in ('sent','partial','overdue') and total - amount_paid > 0),
    'unbilled_visits', (
      select jsonb_build_object('count', count(*), 'value', coalesce(sum(v.price), 0))
      from public.visits v
      where v.tenant_id = p_tenant_id and v.status = 'completed'
        and not exists (select 1 from public.invoice_lines l where l.visit_id = v.id)),
    'collected', jsonb_build_object(
      'week', (select coalesce(sum(amount), 0) from public.payments where tenant_id = p_tenant_id and received_on >= v_week),
      'month', (select coalesce(sum(amount), 0) from public.payments where tenant_id = p_tenant_id and received_on >= v_month)),
    'work_done', jsonb_build_object(
      'week', (select coalesce(sum(price), 0) from public.visits where tenant_id = p_tenant_id and status = 'completed'
                 and scheduled_date between v_week and v_today),
      'month', (select coalesce(sum(price), 0) from public.visits where tenant_id = p_tenant_id and status = 'completed'
                 and scheduled_date between v_month and v_today)),
    'messages_problems', (select count(*) from public.messages
      where tenant_id = p_tenant_id and created_at > now() - interval '7 days'
        and (status = 'failed' or (status = 'suppressed' and suppressed_reason not in ('messaging_off')))),
    'open_shifts_over_12h', (select count(*) from public.time_entries
      where tenant_id = p_tenant_id and clock_out is null and voided_at is null and clock_in < now() - interval '12 hours')
  ) into r;
  return r;
end $$;

revoke execute on function public.owner_overview(uuid) from public, anon;
grant execute on function public.owner_overview(uuid) to authenticated, service_role;
