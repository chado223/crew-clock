-- Follow-up to 20261007000100_weather (already applied to staging; applied
-- migrations are never edited).
--  * The web app saves settings as an upsert, which PostgREST writes as
--    INSERT ... ON CONFLICT DO UPDATE SET tenant_id = ..., so tenant_id needs
--    the update grant. prevent_tenant_change still blocks any actual change.
--  * Office notes on weather alerts share the app-wide 2,000 character limit.

grant update (tenant_id) on public.weather_settings to authenticated;

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
  if p_note is not null and length(p_note) > 2000 then raise exception 'note_too_long' using errcode = '22023'; end if;

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
