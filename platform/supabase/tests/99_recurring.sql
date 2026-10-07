-- Recurring schedule generation: right dates, never duplicates, respects moves,
-- skips, pauses and end dates.

-- Every-2-weeks job on Mondays starting 2026-11-02 (a Monday)
insert into public.jobs (id, tenant_id, client_id, property_id, title, kind, status, interval_weeks, weekday, starts_on, ends_on, price, crew_id) values
  ('f1a00000-0000-0000-0000-0000000000c2', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001',
   'd1a00000-0000-0000-0000-000000000001', 'Biweekly', 'recurring', 'active', 2, 1, '2026-11-02', '2026-12-31', 40, 'ca000000-0000-0000-0000-000000000001');

select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-11-01', '2026-12-31');
select tests.is((select string_agg(scheduled_date::text, ',' order by scheduled_date) from public.visits where job_id = 'f1a00000-0000-0000-0000-0000000000c2'),
  '2026-11-02,2026-11-16,2026-11-30,2026-12-14,2026-12-28', 'Every other Monday from the start date, stopping at the end date');
select tests.is(public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-11-01', '2026-12-31'), 0, 'Running the same range again adds nothing');
select tests.is(public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-11-10', '2026-12-20'), 0, 'Overlapping range adds nothing');
select tests.is((select count(*) from (select scheduled_date from public.visits where job_id = 'f1a00000-0000-0000-0000-0000000000c2'
  group by scheduled_date having count(*) > 1) d), 0::bigint, 'No date has two visits');
select tests.is((select price || ':' || crew_id from public.visits where job_id = 'f1a00000-0000-0000-0000-0000000000c2' limit 1),
  '40.00:ca000000-0000-0000-0000-000000000001', 'Visits take the job''s price and crew');

-- A moved visit is not recreated on its original date; a skipped one stays skipped.
select public.reschedule_visit((select id from public.visits where job_id = 'f1a00000-0000-0000-0000-0000000000c2' and scheduled_date = '2026-11-16'), '2026-11-18');
select public.skip_visit((select id from public.visits where job_id = 'f1a00000-0000-0000-0000-0000000000c2' and scheduled_date = '2026-11-30'), 'Customer away');
select public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-11-01', '2026-12-31');
select tests.is((select count(*) from public.visits where job_id = 'f1a00000-0000-0000-0000-0000000000c2' and scheduled_date in ('2026-11-16','2026-11-30') and status = 'scheduled'),
  0::bigint, 'Moved and skipped visits are not regenerated');
select tests.is((select count(*) from public.visits where job_id = 'f1a00000-0000-0000-0000-0000000000c2'), 5::bigint, 'Still five visits in total');

-- A one-off extra visit on a scheduled day doesn't block or duplicate the regular one.
insert into public.visits (tenant_id, job_id, scheduled_date) values ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-0000000000c2', '2026-12-14');
select public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-12-01', '2026-12-31');
select tests.is((select count(*) from public.visits where job_id = 'f1a00000-0000-0000-0000-0000000000c2' and scheduled_date = '2026-12-14'), 2::bigint,
  'Extra visit and regular visit both exist, no third');
reset role;

-- Concurrent fills (two office tabs, or the background job at the same time) are blocked by the unique key.
select tests.throws($$insert into public.visits (tenant_id, job_id, scheduled_date, generated_for)
  values ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-0000000000c2', '2026-11-02', '2026-11-02')$$, '%duplicate key%',
  'The database itself refuses a second generated visit for the same date');

-- Isolation / permissions
select tests.login('b0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-11-01', '2026-11-30')$$, '%forbidden%', 'Other company cannot fill A''s schedule');
reset role;
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.throws($$select public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-11-01', '2026-11-30')$$, '%forbidden%', 'Crew cannot fill the schedule');
reset role;
