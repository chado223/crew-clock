-- Offline replays and conflicts: every field action can be re-sent safely, and
-- a replay that conflicts with what the office did meanwhile is refused cleanly
-- (the phone shows the reason and drops it) instead of changing data twice.

create temp table ev as select gen_random_uuid() as e1, gen_random_uuid() as e2, gen_random_uuid() as b1;
grant select on ev to authenticated;
update public.visits set scheduled_date = current_date where id = '7a500000-0000-0000-0000-000000000001';

select tests.login('a0000000-0000-0000-0000-000000000003');   -- Cy
set role authenticated;
-- Clock in made offline 40 minutes ago, sent twice (reply lost the first time)
select public.clock_in('aaaaaaaa-0000-0000-0000-000000000000', (select e1 from ev), now() - interval '40 minutes');
select public.clock_in('aaaaaaaa-0000-0000-0000-000000000000', (select e1 from ev), now() - interval '40 minutes');
select tests.is((select count(*) from public.time_entries where clock_out is null and employee_id = 'ea000000-0000-0000-0000-000000000003'), 1::bigint,
  'Clock-in replayed twice = one shift');
select tests.ok((select clock_in from public.time_entries where clock_out is null and employee_id = 'ea000000-0000-0000-0000-000000000003') < now() - interval '35 minutes',
  'The shift starts at the phone''s time, not when signal came back');
-- Break start replayed
select public.start_break('aaaaaaaa-0000-0000-0000-000000000000', (select b1 from ev), now() - interval '20 minutes');
select public.start_break('aaaaaaaa-0000-0000-0000-000000000000', (select b1 from ev), now() - interval '20 minutes');
select tests.is((select count(*) from public.time_entry_breaks where employee_id = 'ea000000-0000-0000-0000-000000000003'), 1::bigint, 'Break replayed = one break');
select public.end_break('aaaaaaaa-0000-0000-0000-000000000000', now() - interval '10 minutes');
select tests.throws($$select public.end_break('aaaaaaaa-0000-0000-0000-000000000000')$$, '%not_on_break%',
  'Ending a break twice is refused (the phone treats this reply as done)');
-- Visit: start + complete with notes, each replayed
select public.start_visit('7a500000-0000-0000-0000-000000000001', now() - interval '30 minutes');
select public.start_visit('7a500000-0000-0000-0000-000000000001', now() - interval '30 minutes');
select public.complete_visit('7a500000-0000-0000-0000-000000000001', 'Gate latch broken', now() - interval '5 minutes');
select public.complete_visit('7a500000-0000-0000-0000-000000000001', 'Gate latch broken', now() - interval '5 minutes');
reset role;
select tests.is((select count(*) from public.activity where kind = 'visit_completed' and summary like '%Gate latch broken%'), 1::bigint,
  'Completion replayed = one history entry, notes kept');
select tests.ok((select completed_at - started_at from public.visits where id = '7a500000-0000-0000-0000-000000000001') between interval '24 minutes' and interval '26 minutes',
  'On-site time comes from the phone''s taps');

-- Conflict: the office skipped a stop while the phone, offline, marked it done.
insert into public.visits (id, tenant_id, job_id, scheduled_date) values
  ('7a500000-0000-0000-0000-0000000000a9', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', current_date);
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select public.skip_visit('7a500000-0000-0000-0000-0000000000a9', 'Customer called to cancel');
reset role;
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.throws($$select public.complete_visit('7a500000-0000-0000-0000-0000000000a9', 'done', now() - interval '1 minute')$$, '%visit_not_open%',
  'Office decision wins: a late "done" on a canceled stop is refused, not applied');
-- Conflict the other way: the office finished it; the phone''s late problem report is refused.
reset role;
insert into public.visits (id, tenant_id, job_id, scheduled_date) values
  ('7a500000-0000-0000-0000-0000000000aa', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', current_date);
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select public.complete_visit('7a500000-0000-0000-0000-0000000000aa', 'Done by office');
reset role;
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.throws($$select * from public.report_visit_problem('7a500000-0000-0000-0000-0000000000aa', 'Gate locked')$$, '%visit_not_open%',
  'A late problem report on a finished stop is refused');
-- Too old to replay (phone off for days): refused, office adds it by hand
select tests.throws($$select public.clock_out('aaaaaaaa-0000-0000-0000-000000000000', gen_random_uuid(), now() - interval '4 days')$$, '%punch_too_old%',
  'Punches more than 3 days old are refused for a manager to enter');
-- Same event id can't be reused to touch someone else''s data
reset role;
select tests.login('a0000000-0000-0000-0000-000000000004');   -- Cam replays Cy's event id
set role authenticated;
select tests.throws($$select public.clock_in('aaaaaaaa-0000-0000-0000-000000000000', (select e1 from ev))$$, '%forbidden%',
  'Another worker''s event id is refused (never returns or changes Cy''s shift)');
reset role;
