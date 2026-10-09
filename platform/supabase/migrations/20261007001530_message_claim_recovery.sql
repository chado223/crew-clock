-- A worker that dies after claiming messages (status 'sending') used to leave
-- them stuck forever. Claims now put back anything left in 'sending' for more
-- than 15 minutes. A resend can't double-deliver: providers get the message id
-- as an idempotency key.

create or replace function public.messages_worker_claim(p_limit integer default 25)
returns setof public.messages
language plpgsql security definer set search_path = '' as $$
begin
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

-- Marks which database is production. Test tools (end-to-end runs, staging
-- jobs) refuse to run where this is on. Turned on only in the production
-- project, as part of cutover.
insert into private.platform_flags (key, enabled, note)
values ('production', false, 'On only in the production project; test tools refuse to run where it is on.')
on conflict (key) do nothing;
