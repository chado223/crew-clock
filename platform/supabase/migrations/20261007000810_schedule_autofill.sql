-- Keep every company's schedule filled ahead without anyone pressing "Fill".
-- Called by the background job (service_role) and by the office pages on load.

create or replace function public.automation_fill_schedules(p_days integer default 21) returns integer
language plpgsql security definer set search_path = '' as $$
declare t uuid; n integer := 0; v_today date;
begin
  if p_days not between 1 and 62 then raise exception 'invalid_date_range' using errcode = '22023'; end if;
  for t in select id from public.tenants where coalesce(status, 'active') = 'active' loop
    v_today := private.tenant_today(t);
    n := n + public.generate_visits(t, v_today, v_today + p_days);
  end loop;
  return n;
end $$;
revoke execute on function public.automation_fill_schedules(integer) from public, anon, authenticated;
grant execute on function public.automation_fill_schedules(integer) to service_role;
