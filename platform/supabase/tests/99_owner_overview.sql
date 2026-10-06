-- Owner overview: right numbers, managers only, one company only.

create temp table d as select private.tenant_today('aaaaaaaa-0000-0000-0000-000000000000') as t;
-- Today for A: one done ($45), one working, one left; tomorrow: one with no crew.
insert into public.visits (tenant_id, job_id, scheduled_date, status, started_at, completed_at, price) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', (select t from d), 'completed', now() - interval '2 hours', now() - interval '1 hour', 45),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', (select t from d), 'in_progress', now(), null, 45),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', (select t from d), 'scheduled', null, null, 45);
update public.jobs set crew_id = null where id = 'f1a00000-0000-0000-0000-000000000001';
insert into public.visits (tenant_id, job_id, scheduled_date) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', (select t from d) + 1);
-- Money and CRM signals
update public.invoices set due_at = now() - interval '3 days' where id = '1a000000-0000-0000-0000-000000000001';
insert into public.payments (tenant_id, invoice_id, amount, method, received_on) values
  ('aaaaaaaa-0000-0000-0000-000000000000', '1a000000-0000-0000-0000-000000000001', 5, 'cash', (select t from d));
update public.estimates set status = 'sent', sent_at = now(), valid_until = (select t from d) + 3 where id = 'e5a00000-0000-0000-0000-000000000001';
update public.clients set status = 'lead' where id = 'c1a00000-0000-0000-0000-000000000001';
insert into public.service_requests (tenant_id, client_id, details) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'Please trim hedges');

select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
create temp table o as select public.owner_overview('aaaaaaaa-0000-0000-0000-000000000000') as j;
select tests.is((select j->'visits_today'->>'total' from o), '3', 'Visits today');
select tests.is((select (j->'visits_today'->>'done') || '/' || (j->'visits_today'->>'working') || '/' || (j->'visits_today'->>'left') from o), '1/1/1',
  'Done / working / left');
select tests.is((select (j->'visits_today'->>'value_done')::numeric from o), 45::numeric, 'Value of work done today');
select tests.is((select j->>'tomorrow_unassigned' from o), '1', 'Tomorrow''s visit with no crew is flagged');
select tests.is((select (j->'receivables'->>'overdue')::numeric from o), 20::numeric, 'Overdue balance (45 - 20 - 5)');
select tests.is((select (j->'collected'->>'week')::numeric from o), 5::numeric, 'Collected this week');
select tests.is((select j->'estimates_waiting'->>'expiring_soon' from o), '1', 'Estimate expiring soon');
select tests.is((select j->>'new_requests' from o), '1', 'New service request');
select tests.is((select j->>'leads' from o), '1', 'Open leads');
select tests.ok((select (j->'unbilled_visits'->>'count')::int from o) >= 1, 'Completed but unbilled visits counted');
reset role;

select tests.login('a0000000-0000-0000-0000-000000000003');   -- A crew
set role authenticated;
select tests.throws($$select public.owner_overview('aaaaaaaa-0000-0000-0000-000000000000')$$, '%forbidden%', 'Crew cannot see company numbers');
reset role;
select tests.login('b0000000-0000-0000-0000-000000000001');   -- B owner
set role authenticated;
select tests.throws($$select public.owner_overview('aaaaaaaa-0000-0000-0000-000000000000')$$, '%forbidden%', 'Other company cannot see A''s numbers');
select tests.is((select public.owner_overview('bbbbbbbb-0000-0000-0000-000000000000')->'visits_today'->>'total'), '0', 'B''s numbers do not include A''s visits');
reset role;
select tests.login(null);
set role anon;
select tests.throws($$select public.owner_overview('aaaaaaaa-0000-0000-0000-000000000000')$$, '%permission denied%', 'Anonymous refused');
reset role;
