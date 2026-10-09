-- Follow-ups from the fix verification:
-- * A message whose send keeps crashing the worker stops after 5 tries
--   (recovered claims count toward the same limit) instead of forever.
-- * Reopening a canceled visit can't create a second visit on a day that
--   already has one for the same job.
-- * One person can't flood the office's list with phone problem reports.

create or replace function public.messages_worker_claim(p_limit integer default 25)
returns setof public.messages
language plpgsql security definer set search_path = '' as $$
begin
  update public.messages set status = 'failed', error = coalesce(error, 'worker stopped mid-send ' || attempts || ' times; gave up')
   where status = 'sending' and updated_at < now() - interval '15 minutes' and attempts >= 5;
  update public.messages set status = 'queued', error = coalesce(error, 'worker stopped mid-send; retried')
   where status = 'sending' and updated_at < now() - interval '15 minutes';

  return query
  with c as (
    select m.id from public.messages m
    where m.status = 'queued' and m.send_after <= now()
    order by m.send_after
    limit least(greatest(p_limit, 1), 200)
    for update skip locked
  )
  update public.messages m set status = 'sending', attempts = m.attempts + 1, updated_at = now()
  from c where m.id = c.id
  returning m.*;
end $$;

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
  -- A canceled visit whose day was filled again (customer reactivated) would become a second visit that day.
  if v.status = 'canceled' and exists (
       select 1 from public.visits o where o.job_id = v.job_id and o.id <> v.id and o.status <> 'canceled'
         and o.scheduled_date = coalesce(p_date, v.scheduled_date)) then
    raise exception 'visit_already_scheduled' using errcode = '22023';
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

create or replace function public.report_sync_problem(
  p_tenant_id uuid, p_kind text, p_at timestamptz, p_error text, p_client_event_id uuid default null
) returns void
language plpgsql security definer set search_path = '' as $$
declare v_emp uuid := private.require_self_employee(p_tenant_id);
begin
  -- A phone reports refusals one by one; more than 50 open from one person is not a real backlog.
  if (select count(*) from public.sync_problems where employee_id = v_emp and resolved_at is null) >= 50 then
    raise exception 'too_many_open_problems' using errcode = '22023';
  end if;
  insert into public.sync_problems (tenant_id, employee_id, user_id, action_kind, action_label, happened_at, error, client_event_id)
  values (p_tenant_id, v_emp, auth.uid(), coalesce(nullif(p_kind, ''), 'action'),
          case p_kind when 'in' then 'Clock in' when 'out' then 'Clock out' when 'break_start' then 'Break start'
                      when 'break_end' then 'Break end' when 'start_visit' then 'Start stop' when 'complete_visit' then 'Finish stop'
                      when 'report_problem' then 'Couldn''t do stop' else 'Action' end,
          least(coalesce(p_at, now()), now()), left(coalesce(p_error, 'refused'), 500), p_client_event_id)
  on conflict (tenant_id, client_event_id) where client_event_id is not null do nothing;
end $$;
