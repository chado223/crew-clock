-- Time clock behavior and the single hours calculation.
-- Includes the four bugs found in the Flask app (audit B1–B4).

select now() - interval '1 minute' as t0 \gset

-- ---------------------------------------------------------------- punches
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;

select (public.clock_in('aaaaaaaa-0000-0000-0000-000000000000', '00000000-0000-0000-0000-0000000000e1', :'t0'::timestamptz - interval '3 hours')).id as shift1 \gset
select tests.ok(:'shift1' is not null, 'Crew can clock in');
select tests.is((public.clock_in('aaaaaaaa-0000-0000-0000-000000000000', '00000000-0000-0000-0000-0000000000e1', :'t0'::timestamptz - interval '3 hours')).id, :'shift1'::uuid,
  'Re-sent clock-in (same event id) returns the same shift, no duplicate');
select tests.throws($$select public.clock_in('aaaaaaaa-0000-0000-0000-000000000000', '00000000-0000-0000-0000-0000000000e2')$$, '%already_clocked_in%',
  'B2: second clock-in while on shift is rejected');
select tests.is(tests.count($$select 1 from public.time_entries where clock_out is null$$), 1::bigint, 'Still exactly one open shift');

select tests.lives(format($$select public.start_break('aaaaaaaa-0000-0000-0000-000000000000', null, %L::timestamptz - interval '2 hours')$$, :'t0'), 'Crew can start a break');
select tests.throws($$select public.start_break('aaaaaaaa-0000-0000-0000-000000000000')$$, '%already_on_break%', 'Cannot start a second break');
select tests.lives(format($$select public.end_break('aaaaaaaa-0000-0000-0000-000000000000', %L::timestamptz - interval '90 minutes')$$, :'t0'), 'Crew can end a break');

select tests.is((public.clock_out('aaaaaaaa-0000-0000-0000-000000000000', '00000000-0000-0000-0000-0000000000f1', :'t0'::timestamptz)).id, :'shift1'::uuid, 'Crew can clock out');
select tests.is((public.clock_out('aaaaaaaa-0000-0000-0000-000000000000', '00000000-0000-0000-0000-0000000000f1', :'t0'::timestamptz)).id, :'shift1'::uuid,
  'Re-sent clock-out (same event id) is idempotent');
select tests.throws($$select public.clock_out('aaaaaaaa-0000-0000-0000-000000000000', '00000000-0000-0000-0000-0000000000f2')$$, '%not_clocked_in%', 'Clock-out without an open shift is rejected');

select tests.is((select worked_seconds from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', current_date - 3, current_date + 1) where entry_id = :'shift1'),
  9000::bigint, '3 h shift minus 30 min unpaid break = 2.5 h');
select tests.is((select break_seconds from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', current_date - 3, current_date + 1) where entry_id = :'shift1'),
  1800::bigint, 'Break time reported');

-- offline / clock-skew bounds
select tests.throws($$select public.clock_in('aaaaaaaa-0000-0000-0000-000000000000', null, now() + interval '1 hour')$$, '%punch_in_future%', 'Future punch rejected');
select tests.throws($$select public.clock_in('aaaaaaaa-0000-0000-0000-000000000000', null, now() - interval '4 days')$$, '%punch_too_old%', 'Punch older than 72 h needs a manager');
select tests.throws(format($$select public.clock_in('aaaaaaaa-0000-0000-0000-000000000000', null, %L::timestamptz - interval '1 hour')$$, :'t0'), '%overlaps_existing_shift%',
  'Offline punch that overlaps a recorded shift is rejected');

-- B1: forgotten clock-out stays open, is excluded from pay totals, and is visible as open
select (public.clock_in('aaaaaaaa-0000-0000-0000-000000000000', null, :'t0'::timestamptz + interval '30 seconds')).id as shift2 \gset
select tests.throws(format($$select public.clock_out('aaaaaaaa-0000-0000-0000-000000000000', null, %L)$$, :'t0'), '%clock_out_before_clock_in%', 'Clock-out before clock-in rejected');
select tests.is((select status from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', current_date - 3, current_date + 1) where entry_id = :'shift2'), 'open',
  'B1: forgotten clock-out shows as open');
select tests.ok((select worked_seconds is null from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', current_date - 3, current_date + 1) where entry_id = :'shift2'),
  'B1: open shift has no hours (no runaway 32-hour day)');
reset role;

-- ---------------------------------------------------------------- manager corrections
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select tests.throws(format($$select public.correct_time_entry(%L, %L::timestamptz + interval '30 seconds', %L::timestamptz + interval '2 hours', '')$$, :'shift2', :'t0', :'t0'),
  '%reason_required%', 'Correction without a reason is rejected');
select tests.throws(format($$select public.correct_time_entry(%L, %L::timestamptz - interval '1 hour', now(), 'overlap test')$$, :'shift2', :'t0'),
  '%overlaps_existing_shift%', 'Correction cannot create overlapping shifts');
select tests.lives(format($$select public.correct_time_entry(%L, %L::timestamptz + interval '30 seconds', now(), 'Forgot to clock out, confirmed with crew lead')$$, :'shift2', :'t0'),
  'Admin closes a forgotten shift with a reason');
select tests.is((select status from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', current_date - 3, current_date + 1) where entry_id = :'shift2'), 'closed',
  'Corrected shift is closed');
select tests.ok(exists (select 1 from public.audit_log where entity_type = 'time_entries' and entity_id = :'shift2'
                       and action = 'update' and reason like 'correct_time_entry: Forgot to clock out%'
                       and before ->> 'clock_out' is null and after ->> 'clock_out' is not null
                       and actor_user_id = 'a0000000-0000-0000-0000-000000000002'),
  'Correction is in the audit log with reason, actor, before and after');
select tests.ok(exists (select 1 from public.audit_log where entity_id = :'shift1' and action = 'insert' and reason = 'clock_in'
                       and actor_user_id = 'a0000000-0000-0000-0000-000000000003'),
  'Every punch is audited with who made it');

select tests.lives($$select public.add_time_entry('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000005',
  '2026-09-29 11:00+00', '2026-09-29 17:00+00', 'Paper timesheet, no phone')$$, 'Admin can add time for an employee with no login');
select tests.lives($$select public.void_time_entry('7a000000-0000-0000-0000-000000000004', 'Duplicate entry')$$, 'Admin can void a shift');
select tests.is(tests.count($$select 1 from public.time_entries where id = '7a000000-0000-0000-0000-000000000004'$$), 1::bigint, 'Voided shift is kept, not deleted');
select tests.is((select count(*) from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-28', '2026-09-28') where employee_id = 'ea000000-0000-0000-0000-000000000004'),
  0::bigint, 'Voided shift excluded from timesheet');
select tests.is((select count(*) from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-28', '2026-09-28', true) where status = 'voided'),
  1::bigint, 'Voided shift visible on request');
reset role;

-- ---------------------------------------------------------------- the calculation
-- Fixed historical/future instants inserted directly (calculation tests only).
insert into public.time_entries (tenant_id, employee_id, clock_in, clock_out, source) values
  -- B4: DST fall-back night. 00:30 EDT -> 03:30 EST is 4 real hours.
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000005', '2026-11-01 00:30 America/New_York', '2026-11-01 03:30 America/New_York', 'admin'),
  -- Overnight: counts toward the day it started
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000005', '2026-10-06 20:00 America/New_York', '2026-10-07 04:00 America/New_York', 'admin'),
  -- Overtime week (Mon 2026-10-12): 5 x 9 h = 45 h
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000005', '2026-10-12 07:00 America/New_York', '2026-10-12 16:00 America/New_York', 'admin'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000005', '2026-10-13 07:00 America/New_York', '2026-10-13 16:00 America/New_York', 'admin'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000005', '2026-10-14 07:00 America/New_York', '2026-10-14 16:00 America/New_York', 'admin'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000005', '2026-10-15 07:00 America/New_York', '2026-10-15 16:00 America/New_York', 'admin'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000005', '2026-10-16 07:00 America/New_York', '2026-10-16 16:00 America/New_York', 'admin'),
  -- Company B is in Chicago: 22:00 local on 9/28 is 9/29 in UTC and New York
  ('bbbbbbbb-0000-0000-0000-000000000000', 'eb000000-0000-0000-0000-000000000003', '2026-09-29 03:00+00', '2026-09-29 05:00+00', 'admin');

select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is((select worked_seconds from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', '2026-11-01', '2026-11-01')), 14400::bigint,
  'B4: DST fall-back night counts the real 4 hours');
select tests.is((select work_date from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', '2026-10-06', '2026-10-07') where worked_seconds = 28800), '2026-10-06'::date,
  'Overnight shift counts toward the start date');
select tests.is((select regular_seconds from public.weekly_hours('aaaaaaaa-0000-0000-0000-000000000000', '2026-10-12') where employee_name = 'Old Timer'), 144000::bigint,
  '45 h week: 40 h regular');
select tests.is((select overtime_seconds from public.weekly_hours('aaaaaaaa-0000-0000-0000-000000000000', '2026-10-12') where employee_name = 'Old Timer'), 18000::bigint,
  '45 h week: 5 h overtime');
select tests.is((select overtime_hours from public.weekly_hours('aaaaaaaa-0000-0000-0000-000000000000', '2026-10-12') where employee_name = 'Old Timer'), 5.00::numeric,
  'Overtime hours reported to 2 decimals');
select tests.throws($$select * from public.weekly_hours('aaaaaaaa-0000-0000-0000-000000000000', '2026-10-13')$$, '%week_start_mismatch%',
  'Week must start on the company''s week start day');
select tests.is((select sum(total_seconds) from public.weekly_hours('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-28')),
  (select sum(worked_seconds) from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-28', '2026-10-04') where status in ('closed','needs_review'))::numeric,
  'Weekly totals equal the sum of timesheet shifts (one calculation)');
reset role;

select tests.login('b0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is((select count(*) from public.timesheet('bbbbbbbb-0000-0000-0000-000000000000', '2026-09-28', '2026-09-28')), 2::bigint,
  'Work date uses the company''s own time zone (Chicago)');
reset role;

-- ---------------------------------------------------------------- database-level guarantees
select tests.throws($$insert into public.time_entries (tenant_id, employee_id, clock_in, clock_out)
  values ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000005', '2026-10-12 10:00 America/New_York', '2026-10-12 12:00 America/New_York')$$,
  '%time_entries_no_overlap%', 'Overlapping shifts blocked even for direct/service writes');
select tests.throws($$insert into public.time_entries (tenant_id, employee_id, clock_in, clock_out)
  values ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000005', '2026-12-01 10:00+00', '2026-12-01 09:00+00')$$,
  '%time_entries_out_after_in_chk%', 'Clock-out before clock-in blocked at the database');
