-- Editing recurring work: price, rhythm, pause/resume, end date; past visits untouched.

create temp table d as select private.tenant_today('aaaaaaaa-0000-0000-0000-000000000000') as t;
grant select on d to authenticated;
-- A past completed visit at the old price must never change.
insert into public.visits (id, tenant_id, job_id, scheduled_date, status, completed_at, price) values
  ('7a500000-0000-0000-0000-0000000000d1', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', (select t - 7 from d), 'completed', now() - interval '7 days', 45);

select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', (select t from d), (select t + 27 from d));
create temp table before_count as select count(*) n from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000001' and status = 'scheduled' and scheduled_date >= (select t from d);
select tests.ok((select n from before_count) >= 3, 'Four weeks of weekly visits on the schedule');

-- Price change flows to upcoming visits only
select public.update_job('f1a00000-0000-0000-0000-000000000001', p_price := 50);
select tests.is((select count(*) from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000001' and status = 'scheduled'
  and scheduled_date >= (select t from d) and price <> 50), 0::bigint, 'Upcoming visits take the new price');
select tests.is((select price from public.visits where id = '7a500000-0000-0000-0000-0000000000d1'), 45.00::numeric, 'Past work keeps its price');
select tests.ok((select summary from public.activity where kind = 'job_updated' order by seq desc limit 1) like 'A Mow: price $45.00 → $50%', 'Price change in customer history');

-- New weekday: old upcoming visits canceled (kept), new ones generated on the new day
select public.update_job('f1a00000-0000-0000-0000-000000000001', p_weekday := 4);
select tests.is((select count(*) from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000001' and status = 'scheduled'
  and scheduled_date >= (select t from d) and extract(isodow from scheduled_date) <> 4), 0::bigint, 'No upcoming visits left on the old day');
select tests.ok((select count(*) from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000001' and status = 'scheduled'
  and extract(isodow from scheduled_date) = 4) >= 5, 'Six weeks generated on the new day');
select tests.ok((select count(*) from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000001' and status = 'canceled' and status_reason = 'Schedule changed') >= 3,
  'Old visits kept as canceled with a reason');

-- Pause, then resume
select public.update_job('f1a00000-0000-0000-0000-000000000001', p_status := 'paused', p_reason := 'Winter');
select tests.is((select count(*) from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000001' and status = 'scheduled' and scheduled_date >= (select t from d)),
  0::bigint, 'Pausing clears the upcoming schedule');
select tests.is((select status from public.visits where id = '7a500000-0000-0000-0000-0000000000d1'), 'completed', 'History untouched by pause');
select tests.is(public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', (select t from d), (select t + 27 from d)), 0, 'Fill skips paused jobs');
select public.update_job('f1a00000-0000-0000-0000-000000000001', p_status := 'active');
select tests.ok((select count(*) from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000001' and status = 'scheduled' and scheduled_date >= (select t from d)) >= 5,
  'Resuming puts it back on the schedule');

-- End date
select public.update_job('f1a00000-0000-0000-0000-000000000001', p_ends_on := (select t + 10 from d));
select tests.is((select count(*) from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000001' and status = 'scheduled' and scheduled_date > (select t + 10 from d)),
  0::bigint, 'Nothing scheduled after the end date');
select tests.throws($$select public.update_job('f1a00000-0000-0000-0000-000000000001', p_interval_weeks := 0)$$, '%invalid_interval%', 'Bad frequency rejected');
select tests.throws($$select public.update_job('f1a00000-0000-0000-0000-000000000001', p_crew_id := 'cb000000-0000-0000-0000-000000000001')$$, '%crew_not_found%',
  'Cannot assign another company''s crew');
select tests.throws($$update public.properties set status = 'inactive' where id = 'd1a00000-0000-0000-0000-000000000001'$$, '%property_has_active_jobs%',
  'A property with active work cannot be archived');
reset role;

select tests.login('a0000000-0000-0000-0000-000000000003');   -- crew
set role authenticated;
select tests.throws($$select public.update_job('f1a00000-0000-0000-0000-000000000001', p_price := 1)$$, '%forbidden%', 'Crew cannot edit jobs');
reset role;
select tests.login('b0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select public.update_job('f1a00000-0000-0000-0000-000000000001', p_price := 1)$$, '%forbidden%', 'Other company cannot edit jobs');
reset role;

-- Background fill keeps schedules ahead for every company (service only)
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select public.automation_fill_schedules()$$, '%permission denied%', 'Only the background job can fill every company');
reset role;
select tests.login(null);
set role service_role;
select tests.ok(public.automation_fill_schedules(21) >= 1, 'Background fill adds upcoming visits (company B''s weekly job)');
select tests.is(public.automation_fill_schedules(21), 0, 'Running it again adds nothing');
reset role;
