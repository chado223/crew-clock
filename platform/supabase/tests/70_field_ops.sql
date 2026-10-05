-- Field operations: recurring jobs -> visits -> assignment -> crew workflow -> customer history.
-- Company A job f1a…: weekly on Tuesday (ISO 2) from 2026-09-01, crew "Crew 1" (Cy Crew a…03).
-- Visit 7a5… on 2026-09-29: crew 1 + Cam Crew (a…04) directly assigned.

-- ------------------------------------------------------------ generation
select tests.login('a0000000-0000-0000-0000-000000000002');  -- admin
set role authenticated;
select tests.is(public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-10-01', '2026-10-31'), 4,
  'Weekly Tuesday job generates 4 October visits');
select tests.is(public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-10-01', '2026-10-31'), 0,
  'Generating again creates no duplicates');
select tests.is((select array_agg(scheduled_date order by scheduled_date)::text from public.visits
                 where job_id = 'f1a00000-0000-0000-0000-000000000001' and scheduled_date >= '2026-10-01'),
  '{2026-10-06,2026-10-13,2026-10-20,2026-10-27}', 'Visits land on Tuesdays');
select tests.is((select price from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000001' and scheduled_date = '2026-10-06'),
  45.00::numeric, 'Visit takes the job price');
select tests.is((select property_id from public.visits where scheduled_date = '2026-10-06' and tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000'),
  'd1a00000-0000-0000-0000-000000000001'::uuid, 'Visit is tied to the job''s property');
select tests.throws($$select public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-10-01', '2027-01-31')$$,
  '%invalid_date_range%', 'Range limited to about two months');
reset role;

-- Every-2-weeks job starting mid-week
insert into public.jobs (id, tenant_id, client_id, property_id, title, kind, interval_weeks, weekday, starts_on, price)
values ('f1a00000-0000-0000-0000-000000000002', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001',
        'd1a00000-0000-0000-0000-000000000001', 'Shrub trim', 'recurring', 2, 5, '2026-10-07', 80);
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-10-01', '2026-10-31');
select tests.is((select array_agg(scheduled_date order by scheduled_date)::text from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000002'),
  '{2026-10-09,2026-10-23}', 'Every-other-Friday starts on the first Friday after the start date');
reset role;
select tests.throws($$insert into public.jobs (tenant_id, title, kind) values ('aaaaaaaa-0000-0000-0000-000000000000', 'x', 'recurring')$$,
  '%jobs_recurrence_chk%', 'Recurring job needs a schedule');

-- ------------------------------------------------------------ crew visibility
select tests.login('a0000000-0000-0000-0000-000000000003');  -- Cy, on Crew 1
set role authenticated;
select tests.is((select count(*) from public.schedule('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-10-31')), 5::bigint,
  'Crew member sees visits of their crew');
select tests.is(tests.count($$select 1 from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000002'$$), 0::bigint,
  'Crew member does not see visits they are not on');
select tests.throws($$select public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-10-01', '2026-10-31')$$, '%forbidden%',
  'Crew cannot generate the schedule');
select tests.throws($$select public.reschedule_visit('7a500000-0000-0000-0000-000000000001', '2026-09-30')$$, '%forbidden%',
  'Crew cannot reschedule');
select tests.is(tests.affected($$update public.visits set price = 0$$), 0::bigint, 'Crew cannot edit visits directly');
reset role;

select tests.login('a0000000-0000-0000-0000-000000000004');  -- Cam, directly assigned to one visit
set role authenticated;
select tests.is((select count(*) from public.schedule('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-10-31')), 1::bigint,
  'Directly assigned person sees just that visit');
reset role;

-- ------------------------------------------------------------ crew workflow + CRM history
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.is((public.start_visit('7a500000-0000-0000-0000-000000000001')).status, 'in_progress', 'Crew starts a visit');
select tests.is((public.complete_visit('7a500000-0000-0000-0000-000000000001', 'Gate latch is loose')).status, 'completed', 'Crew completes it');
select tests.is((public.complete_visit('7a500000-0000-0000-0000-000000000001')).status, 'completed', 'Completing twice is harmless (offline retries)');
reset role;
select tests.ok(exists (select 1 from public.activity where client_id = 'c1a00000-0000-0000-0000-000000000001' and kind = 'visit_completed'
                        and summary like 'A Mow at 1 A St completed: Gate latch is loose'),
  'Completion appears in the customer''s history with the crew note');
select tests.ok((select completed_by = 'a0000000-0000-0000-0000-000000000003' from public.visits where id = '7a500000-0000-0000-0000-000000000001'),
  'Records who completed it');

-- ------------------------------------------------------------ manager scheduling
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select id as oct6 from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000001' and scheduled_date = '2026-10-06' \gset
select tests.is((public.reschedule_visit(:'oct6', '2026-10-07', null, 'Rain')).scheduled_date, '2026-10-07'::date, 'Admin moves a visit for rain');
select tests.ok(exists (select 1 from public.activity where kind = 'visit_rescheduled' and summary like '%moved to Oct 7 (Rain)%'),
  'Reschedule appears in customer history with the reason');
select tests.is(public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-10-01', '2026-10-31'), 0,
  'A moved visit is not re-created by generation');
select tests.throws(format($$select public.skip_visit(%L, '')$$, :'oct6'), '%reason_required%', 'Skipping needs a reason');
select tests.is((public.skip_visit(:'oct6', 'Customer on vacation')).status, 'skipped', 'Admin skips a visit');
select tests.throws(format($$select public.reschedule_visit(%L, '2026-10-08')$$, :'oct6'), '%visit_not_scheduled%', 'Skipped visit cannot be moved');
select tests.throws($$select public.assign_visit('7a500000-0000-0000-0000-000000000001', null, '{eb000000-0000-0000-0000-000000000003}')$$,
  '%employee_not_found%', 'Cannot assign another company''s employee');
select tests.throws($$select public.assign_visit('7a500000-0000-0000-0000-000000000001', 'cb000000-0000-0000-0000-000000000001')$$,
  '%crew_not_found%', 'Cannot assign another company''s crew');
select tests.throws($$insert into public.visits (tenant_id, job_id, scheduled_date) values ('aaaaaaaa-0000-0000-0000-000000000000', 'f1b00000-0000-0000-0000-000000000001', '2026-10-01')$$,
  '%job_not_found%', 'Visit cannot point at another company''s job');
reset role;

-- ------------------------------------------------------------ cross-company
select tests.login('b0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is((select count(*) from public.schedule('aaaaaaaa-0000-0000-0000-000000000000', '2026-01-01', '2026-12-31')), 0::bigint,
  'Other company sees none of A''s schedule');
select tests.throws($$select public.complete_visit('7a500000-0000-0000-0000-000000000001')$$, '%forbidden%', 'Other company cannot complete A''s visit');
select tests.throws($$select public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', '2026-10-01', '2026-10-31')$$, '%forbidden%',
  'Other company cannot generate A''s visits');
reset role;

-- ------------------------------------------------------------ time linked to a visit
select tests.throws($$update public.time_entries set visit_id = '7b500000-0000-0000-0000-000000000001' where id = '7a000000-0000-0000-0000-000000000003'$$,
  '%foreign key%', 'A shift cannot be linked to another company''s visit');
