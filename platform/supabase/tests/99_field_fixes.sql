-- Second audit: reopen visits, inactive customers stop scheduling, costing by the day work was done.

-- ------------------------------------------------------------ reopen
insert into public.visits (id, tenant_id, job_id, scheduled_date, status, status_reason) values
  ('88888888-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', current_date - 1, 'skipped', 'Crew: gate locked');
insert into public.visits (id, tenant_id, job_id, scheduled_date, status, started_at, completed_at) values
  ('88888888-0000-0000-0000-000000000002', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', current_date - 2, 'completed', now() - interval '2 days', now() - interval '2 days'),
  ('88888888-0000-0000-0000-000000000003', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', current_date - 3, 'completed', now() - interval '3 days', now() - interval '3 days');
insert into public.invoices (id, tenant_id, client_id, total, status) values
  ('88888888-0000-0000-0000-0000000000a1', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 0, 'draft');
insert into public.invoice_lines (tenant_id, invoice_id, visit_id, description, quantity, unit_price)
values ('aaaaaaaa-0000-0000-0000-000000000000', '88888888-0000-0000-0000-0000000000a1', '88888888-0000-0000-0000-000000000003', 'Mow', 1, 50);

select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.throws($$select public.reopen_visit('88888888-0000-0000-0000-000000000001', current_date + 1, 'customer called')$$, '%forbidden%', 'Crew cannot reopen visits');
reset role;
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select tests.throws($$select public.reopen_visit('88888888-0000-0000-0000-000000000001', current_date + 1, '')$$, '%reason_required%', 'Reopening needs a reason');
select tests.is((public.reopen_visit('88888888-0000-0000-0000-000000000001', current_date + 1, 'gate code from customer')).status, 'scheduled',
  'A stop the crew couldn''t do goes back on the schedule');
select tests.is((public.reopen_visit('88888888-0000-0000-0000-000000000002', null, 'marked done by mistake')).completed_at::text, null,
  'A visit marked done by mistake (not billed) can be undone');
select tests.throws($$select public.reopen_visit('88888888-0000-0000-0000-000000000003', null, 'oops')$$, '%visit_billed%',
  'A billed visit cannot be reopened');
reset role;
select tests.is((select scheduled_date from public.visits where id = '88888888-0000-0000-0000-000000000001'), current_date + 1, 'Moved to the new date');
select tests.ok((select count(*) from public.audit_log where entity_type = 'visits' and reason like 'reopen_visit:%') >= 2, 'Reopens are audited');

-- ----------------------------------------------- inactive customers
update public.jobs set kind = 'recurring', status = 'active', starts_on = current_date, weekday = extract(isodow from current_date + 1)::int, interval_weeks = 1
 where id = 'f1a00000-0000-0000-0000-000000000001';
select public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', current_date, current_date + 21);
select count(*) as upcoming from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000001' and status = 'scheduled' and scheduled_date >= current_date \gset
select tests.ok(:upcoming >= 3, 'Recurring visits scheduled for an active customer');
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
update public.clients set status = 'inactive' where id = 'c1a00000-0000-0000-0000-000000000001';
reset role;
select tests.is((select count(*) from public.visits where client_id = 'c1a00000-0000-0000-0000-000000000001' and status = 'scheduled' and scheduled_date >= current_date),
  0::bigint, 'Marking the customer inactive cancels their upcoming visits');
select tests.ok((select count(*) from public.visits where client_id = 'c1a00000-0000-0000-0000-000000000001' and status = 'canceled' and status_reason = 'Customer inactive') >= 3,
  'Canceled visits are kept with the reason');
select tests.is(public.generate_visits('aaaaaaaa-0000-0000-0000-000000000000', current_date, current_date + 21), 0, 'No new visits for an inactive customer');
select tests.is((select count(*) from public.visits where client_id = 'c1b00000-0000-0000-0000-000000000001' and status = 'canceled'), 0::bigint,
  'Other company untouched');
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
update public.clients set status = 'active' where id = 'c1a00000-0000-0000-0000-000000000001';
reset role;
select tests.ok((select count(*) from public.visits where client_id = 'c1a00000-0000-0000-0000-000000000001' and status = 'scheduled' and scheduled_date >= current_date) >= 3,
  'Reactivating puts their recurring work back on the schedule');
select tests.is((select count(*) from (select scheduled_date from public.visits where job_id = 'f1a00000-0000-0000-0000-000000000001' and status = 'scheduled'
  group by scheduled_date having count(*) > 1) d), 0::bigint, 'No duplicate visits after reactivating');

-- ------------------------------------------ costing by actual work day
-- Scheduled Monday Sep 14 (rained out), done Tuesday 9-10am with a 9-10am shift; nothing worked Monday.
update public.crews set active = true where id = 'ca000000-0000-0000-0000-000000000001';
insert into public.visits (id, tenant_id, job_id, scheduled_date, crew_id, status, started_at, completed_at, price) values
  ('88888888-0000-0000-0000-000000000010', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', '2026-09-14',
   'ca000000-0000-0000-0000-000000000001', 'completed', '2026-09-15 13:00+00', '2026-09-15 14:00+00', 60);
insert into public.time_entries (tenant_id, user_id, employee_id, clock_in, clock_out, source) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000003', 'ea000000-0000-0000-0000-000000000003', '2026-09-15 13:00+00', '2026-09-15 14:00+00', 'app');
select tests.is((select onsite_minutes || '/' || overhead_minutes || '/' || labor_cost from public.visit_costing('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-14', '2026-09-15')
  where visit_id = '88888888-0000-0000-0000-000000000010'), '60/0/18.00', 'Work done a day late is costed once: 1 h on site, no extra overhead, $18');
