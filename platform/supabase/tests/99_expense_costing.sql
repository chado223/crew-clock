-- Expenses tied to visits, jobs and customers flow into profitability by every
-- grouping; totals reconcile with Business health; isolation and permissions.

update public.visits set status = 'completed', started_at = '2026-09-29 13:00+00', completed_at = '2026-09-29 14:00+00', price = 45
  where id = '7a500000-0000-0000-0000-000000000001';
delete from public.expenses where tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';

select tests.login('a0000000-0000-0000-0000-000000000002');   -- admin
set role authenticated;
insert into public.expenses (tenant_id, category, amount, spent_at, visit_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'Materials', 10, '2026-09-29', '7a500000-0000-0000-0000-000000000001');
insert into public.expenses (tenant_id, category, amount, spent_at, job_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'Materials', 20, '2026-09-15', 'f1a00000-0000-0000-0000-000000000001');
insert into public.expenses (tenant_id, category, amount, spent_at, client_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'Permit', 5, '2026-09-10', 'c1a00000-0000-0000-0000-000000000001');
insert into public.expenses (tenant_id, category, amount, spent_at) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'Insurance', 100, '2026-09-01');
select tests.is((select job_id::text || '|' || client_id from public.expenses where amount = 10),
  'f1a00000-0000-0000-0000-000000000001|c1a00000-0000-0000-0000-000000000001', 'A visit expense knows its job and customer');
select tests.is((select client_id::text from public.expenses where amount = 20), 'c1a00000-0000-0000-0000-000000000001', 'A job expense knows its customer');
select tests.throws($$insert into public.expenses (tenant_id, category, amount, spent_at, job_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'x', 1, current_date, 'f1b00000-0000-0000-0000-000000000001')$$, '%', 'Cannot tie an expense to another company''s job');
select tests.throws($$insert into public.expenses (tenant_id, category, amount, spent_at) values ('aaaaaaaa-0000-0000-0000-000000000000', 'x', -5, current_date)$$,
  '%expenses_amount_positive%', 'Amounts must be positive');

select tests.is((select expenses from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'customer')
  where key = 'c1a00000-0000-0000-0000-000000000001'), 35.00::numeric, 'Customer carries visit + job + customer expenses');
select tests.is((select margin from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'customer')
  where key = 'c1a00000-0000-0000-0000-000000000001'),
  (select 45 - labor_cost - 35 from public.visit_costing('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30')
   where visit_id = '7a500000-0000-0000-0000-000000000001'), 'Margin = revenue - labor - expenses');
select tests.is((select expenses from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'job')
  where key = 'f1a00000-0000-0000-0000-000000000001'), 30.00::numeric, 'Job carries visit + job expenses');
select tests.is((select expenses from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'job')
  where key is null), 5.00::numeric, 'Customer-only expense shows as not tied to a job');
select tests.is((select expenses from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'property')
  where key = 'd1a00000-0000-0000-0000-000000000001'), 30.00::numeric, 'Property carries its job''s expenses');
select tests.is((select expenses from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'service')
  where key = '5a000000-0000-0000-0000-000000000001'), 30.00::numeric, 'Service carries its jobs'' expenses');
select tests.is((select expenses from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'crew')
  where key = 'ca000000-0000-0000-0000-000000000001'), 30.00::numeric, 'Crew carries its work''s expenses');
-- Reconciliation: tied expenses in every grouping + general overhead = Business health total
select tests.is((select sum(expenses) from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'customer')) + 100,
  (public.business_health('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30')->'money'->>'expenses')::numeric,
  'Tied + overhead = company total (customer view)');
select tests.is((select sum(expenses) from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'service')),
  (select sum(expenses) from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30', 'job')),
  'Every grouping adds to the same tied total');
-- Period rule: an expense dated outside the period is excluded
select tests.is((select coalesce(sum(expenses), 0) from public.profitability('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-20', '2026-09-30', 'job')),
  10.00::numeric, 'Only expenses dated in the period count');
reset role;

select tests.login('a0000000-0000-0000-0000-000000000003');   -- crew
set role authenticated;
select tests.throws($$insert into public.expenses (tenant_id, category, amount) values ('aaaaaaaa-0000-0000-0000-000000000000', 'x', 1)$$, '%row-level security%', 'Crew cannot add expenses');
select tests.is((select count(*) from public.expenses), 0::bigint, 'Crew cannot see expenses');
reset role;
