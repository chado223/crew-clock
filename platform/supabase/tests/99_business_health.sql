-- Owner command center + business health: every number agrees with the
-- records behind it and with the other screens; managers of that company only.

create temp table d as select private.tenant_today('aaaaaaaa-0000-0000-0000-000000000000') as t;
grant select on d to authenticated;

-- September: the fixture visit (Sep 29) done for $45 inside Cy's shift that day; one more done for $60 on Sep 28.
insert into public.time_entries (tenant_id, employee_id, user_id, clock_in, clock_out, source) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000003', 'a0000000-0000-0000-0000-000000000003', '2026-09-29 12:00+00', '2026-09-29 16:00+00', 'app');
update public.visits set status = 'completed', started_at = '2026-09-29 13:00+00', completed_at = '2026-09-29 14:00+00', price = 45
  where id = '7a500000-0000-0000-0000-000000000001';
insert into public.visits (id, tenant_id, job_id, scheduled_date, status, started_at, completed_at, price) values
  ('7a500000-0000-0000-0000-0000000000b1', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', '2026-09-28', 'completed',
   '2026-09-28 12:00+00', '2026-09-28 13:00+00', 60);
insert into public.expenses (tenant_id, category, amount, spent_at, visit_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'Materials', 7, '2026-09-28', '7a500000-0000-0000-0000-0000000000b1');
delete from public.expenses where tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000' and visit_id is null;

-- A drifting customer: one-off work in the summer, nothing since.
insert into public.clients (id, tenant_id, name) values ('c1a00000-0000-0000-0000-0000000000b2', 'aaaaaaaa-0000-0000-0000-000000000000', 'Drifting Dave');
insert into public.properties (id, tenant_id, client_id, address_line1) values
  ('d1a00000-0000-0000-0000-0000000000b2', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000b2', '5 Quiet Ct');
insert into public.jobs (id, tenant_id, client_id, property_id, title) values
  ('f1a00000-0000-0000-0000-0000000000b2', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000b2', 'd1a00000-0000-0000-0000-0000000000b2', 'Cleanup');
insert into public.visits (tenant_id, job_id, scheduled_date, status, completed_at, price) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-0000000000b2', (select t - 70 from d), 'completed', now() - interval '70 days', 300);

-- Attention signals
update public.invoices set due_at = now() - interval '40 days', sent_at = '2026-09-01' where id = '1a000000-0000-0000-0000-000000000001';
insert into public.visits (id, tenant_id, job_id, scheduled_date) values
  ('7a500000-0000-0000-0000-0000000000b3', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', (select t - 2 from d));
insert into public.clients (id, tenant_id, name) values ('c1a00000-0000-0000-0000-0000000000b4', 'aaaaaaaa-0000-0000-0000-000000000000', 'New Nina');
insert into public.properties (id, tenant_id, client_id, address_line1) values
  ('d1a00000-0000-0000-0000-0000000000b4', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000b4', '9 New St');
insert into public.jobs (id, tenant_id, client_id, property_id, title) values
  ('f1a00000-0000-0000-0000-0000000000b4', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000b4', 'd1a00000-0000-0000-0000-0000000000b4', 'First mow');
insert into public.visits (id, tenant_id, job_id, scheduled_date) values
  ('7a500000-0000-0000-0000-0000000000b4', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-0000000000b4', (select t + 1 from d));
insert into public.time_entries (id, tenant_id, employee_id, user_id, clock_in, source) values
  ('7a000000-0000-0000-0000-0000000000b5', 'aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000004', 'a0000000-0000-0000-0000-000000000004', now() - interval '13 hours', 'app');
insert into public.service_requests (tenant_id, client_id, details) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'Hedges please');
update public.estimates set status = 'sent', sent_at = now() - interval '8 days', valid_until = (select t + 20 from d)
  where id = 'e5a00000-0000-0000-0000-000000000001';
insert into public.weather_alerts (tenant_id, visit_id, forecast_date, reasons, precip_pct) values
  ('aaaaaaaa-0000-0000-0000-000000000000', '7a500000-0000-0000-0000-0000000000b4', (select t + 1 from d), '{rain}', 80);
insert into public.clients (id, tenant_id, name, status, created_at) values
  ('c1a00000-0000-0000-0000-0000000000b6', 'aaaaaaaa-0000-0000-0000-000000000000', 'Quiet Lead', 'lead', now() - interval '20 days');
update public.activity set occurred_at = now() - interval '20 days' where client_id = 'c1a00000-0000-0000-0000-0000000000b6';
insert into public.messages (tenant_id, client_id, template_key, channel, mode, to_address, body, status, error) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'invoice_sent', 'email', 'test', 'x@y.test', 'b', 'failed', 'bad address');
-- B data that must never appear in A's numbers
update public.visits set status = 'completed', completed_at = '2026-09-30 15:00+00', price = 999 where id = '7b500000-0000-0000-0000-000000000001';

select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;

-- ===================================================== attention: one row per record, pointing at it
create temp table att as select * from public.owner_attention('aaaaaaaa-0000-0000-0000-000000000000');
select tests.is((select ref_type || ':' || ref_id from att where kind = 'invoice_overdue'), 'invoice:1a000000-0000-0000-0000-000000000001',
  'Overdue invoice links to that invoice');
select tests.is((select amount from att where kind = 'invoice_overdue'), 25.00::numeric, 'With its balance');
select tests.is((select severity from att where kind = 'invoice_overdue'), 1, 'Over 30 days overdue is urgent');
select tests.is((select ref_type || ':' || ref_id from att where kind = 'weather'), 'visit:7a500000-0000-0000-0000-0000000000b4', 'Weather risk links to the visit');
select tests.is((select ref_id::text from att where kind = 'missed_visit'), '7a500000-0000-0000-0000-0000000000b3', 'Missed visit (still scheduled, day passed)');
select tests.is((select ref_id::text from att where kind = 'unassigned'), '7a500000-0000-0000-0000-0000000000b4', 'Tomorrow''s visit with no crew');
select tests.is((select ref_type || ':' || ref_id from att where kind = 'long_shift'), 'time_entry:7a000000-0000-0000-0000-0000000000b5', 'Forgotten clock-out');
select tests.is((select ref_type || ':' || ref_id from att where kind = 'request'), 'client:c1a00000-0000-0000-0000-000000000001', 'Service request links to the customer');
select tests.is((select ref_type || ':' || ref_id from att where kind = 'estimate_followup'), 'estimate:e5a00000-0000-0000-0000-000000000001', 'Quiet estimate to follow up');
select tests.ok((select count(*) from att where kind = 'unbilled' and ref_id = 'c1a00000-0000-0000-0000-000000000001') = 1, 'Unbilled work grouped per customer');
select tests.is((select ref_id::text from att where kind = 'lead_followup'), 'c1a00000-0000-0000-0000-0000000000b6', 'Untouched lead');
select tests.is((select count(*) from att where kind = 'message_problem'), 1::bigint, 'Failed message');
select tests.is((select min(severity) from att), 1, 'Urgent items first');
select tests.is((select count(*) from att where title like '%B Client%' or ref_id = '7b500000-0000-0000-0000-000000000001'), 0::bigint, 'Nothing from another company');

-- ===================================================== one set of numbers
create temp table h as select public.business_health('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30') as j;
select tests.is((select (j->'money'->>'work_done')::numeric from h), 105::numeric, 'Work done = completed visits in the period (45 + 60)');
select tests.is((select (j->'money'->>'work_done')::numeric from h),
  (select sum(revenue) from public.visit_costing('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30')),
  'Same as the Profit page (visit_costing)');
select tests.is((select (j->'money'->>'work_done')::numeric from h),
  (select sum(revenue) from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'customer')),
  'Same as profitability by customer');
select tests.is((select (j->'money'->>'job_labor')::numeric from h),
  (select sum(labor_cost) from public.visit_costing('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30')), 'Job labor = job costing');
select tests.is((select (j->'money'->>'paid_hours')::numeric from h), 16.0, 'Paid hours from the one hours calculation (8 + 4 + 4)');
select tests.is((select (j->'money'->>'payroll')::numeric from h), 216.00::numeric, 'Payroll = Cy 12h x $18 (Cam has no rate)');
select tests.ok((select (j->'money'->>'missing_rates')::int from h) >= 1, 'Missing pay rates are flagged, not hidden');
select tests.is((select (j->'money'->>'expenses')::numeric from h), 7::numeric, 'Expenses in the period');
select tests.is((select (j->'money'->>'gross_profit')::numeric from h), (105 - 216 - 7)::numeric, 'Gross profit = work done - payroll - expenses');
select tests.is((select (j->'money'->>'collected')::numeric from h), 20::numeric, 'Collected = payments received in the period');
select tests.is((select (j->'receivables'->>'d31_60')::numeric from h), 25::numeric, 'Aging bucket');
select tests.is((select (j->'receivables'->>'total')::numeric from h), (select sum(balance) from public.receivables('aaaaaaaa-0000-0000-0000-000000000000')),
  'Receivables total = the invoice list');
select tests.is((select (j->'customers'->>'at_risk')::int from h), 1, 'One customer drifting away');
select tests.is((select name from public.at_risk_customers('aaaaaaaa-0000-0000-0000-000000000000')), 'Drifting Dave', '...and who');
select tests.is((select (j->'recurring'->>'monthly_value')::numeric from h), round(45 * 52.0 / 12, 2), 'Recurring monthly value (weekly $45)');
select tests.is((select jsonb_array_length(j->'workload') from h), 14, 'Two weeks of upcoming workload');
select tests.is((select (w->>'unassigned')::int from h, jsonb_array_elements(j->'workload') w where w->>'date' = ((select t + 1 from d))::text), 1,
  'Workload shows unassigned visits');
select tests.ok((select (c->>'utilization_pct') is not null from h, jsonb_array_elements(j->'crews') c where c->>'crew' = 'Crew 1'), 'Crew utilization');

-- Profitability groups
select tests.is((select count(*) from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'service')), 1::bigint, 'By service');
select tests.is((select expenses from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'job')), 7::numeric,
  'Visit expenses land on the job');
select tests.throws($$select * from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'zodiac')$$, '%invalid_group%', 'Unknown grouping');
reset role;

-- ===================================================== who can see it
select tests.login('a0000000-0000-0000-0000-000000000003');   -- crew
set role authenticated;
select tests.throws($$select public.business_health('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30')$$, '%forbidden%', 'Crew cannot see business numbers');
select tests.throws($$select * from public.owner_attention('aaaaaaaa-0000-0000-0000-000000000000')$$, '%forbidden%', 'Crew cannot see the owner to-do list');
select tests.throws($$select * from public.receivables('aaaaaaaa-0000-0000-0000-000000000000')$$, '%forbidden%', 'Crew cannot see receivables');
reset role;
select tests.login('b0000000-0000-0000-0000-000000000001');   -- B owner
set role authenticated;
select tests.throws($$select * from public.at_risk_customers('aaaaaaaa-0000-0000-0000-000000000000')$$, '%forbidden%', 'Other company refused');
select tests.throws($$select * from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'customer')$$, '%forbidden%', 'Other company refused (profit)');
select tests.is((select (public.business_health('bbbbbbbb-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30')->'money'->>'work_done')::numeric), 999::numeric,
  'B sees only its own work');
reset role;
select tests.login(null);
set role anon;
select tests.throws($$select public.business_health('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30')$$, '%permission denied%', 'Anonymous refused');
reset role;
