-- Audit fixes (2026-10-06): each finding has a test that fails if it comes back.

-- ===================================================== M2 crew sees only their stops, no prices
select tests.login('a0000000-0000-0000-0000-000000000003');   -- Cy, Crew 1
set role authenticated;
select tests.is((select count(*) from public.schedule('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-10-31')), 1::bigint, 'Crew schedule shows their stop');
select tests.is((select price from public.schedule('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-10-31')), null::numeric, 'No price for crew');
select tests.is((select client_name from public.schedule('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-10-31')), 'A Client', 'Crew gets what the stop needs');
select tests.is(tests.count('select 1 from public.profiles'), 1::bigint, 'Crew reads only their own profile (no other app billing fields)');
select tests.is((select count(*) from public.start_visit('7a500000-0000-0000-0000-000000000001') where to_jsonb(start_visit) ? 'price'), 0::bigint,
  'Starting a visit returns no price');
reset role;
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is((select price from public.schedule('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-10-31') where price is not null limit 1), 45.00::numeric,
  'Office still sees prices');
reset role;

-- ===================================================== offline times on visits
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select public.complete_visit('7a500000-0000-0000-0000-000000000001', 'done');
reset role;
insert into public.visits (id, tenant_id, job_id, scheduled_date) values
  ('7a500000-0000-0000-0000-0000000000c3', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', current_date);
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select public.start_visit('7a500000-0000-0000-0000-0000000000c3', now() - interval '3 hours');
select public.complete_visit('7a500000-0000-0000-0000-0000000000c3', 'done offline', now() - interval '2 hours');
reset role;
select tests.ok((select started_at < now() - interval '170 minutes' and completed_at < now() - interval '110 minutes' from public.visits
  where id = '7a500000-0000-0000-0000-0000000000c3'), 'A stop done without signal keeps the phone''s times');
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;

select tests.throws($$select public.start_visit('7a500000-0000-0000-0000-0000000000ff', now() - interval '10 days')$$, '%not_found%', 'Unknown visit');
reset role;

-- ===================================================== M1 photos and visits follow the current customer
insert into public.clients (id, tenant_id, name) values ('c1a00000-0000-0000-0000-0000000000e1', 'aaaaaaaa-0000-0000-0000-000000000000', 'Second Customer');
insert into public.properties (id, tenant_id, client_id, address_line1) values
  ('d1a00000-0000-0000-0000-0000000000e1', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000e1', '2 Private Ln');
insert into public.jobs (id, tenant_id, client_id, property_id, title) values
  ('f1a00000-0000-0000-0000-0000000000e1', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000e1', 'd1a00000-0000-0000-0000-0000000000e1', 'Second mow');
select tests.throws($$update public.jobs set property_id = 'd1a00000-0000-0000-0000-0000000000e1' where id = 'f1a00000-0000-0000-0000-000000000001'$$,
  '%jobs_property_belongs_to_client%', 'A job cannot point at another customer''s property');
select tests.throws($$insert into public.estimates (tenant_id, number, client_id, property_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'EST-X', 'c1a00000-0000-0000-0000-000000000001', 'd1a00000-0000-0000-0000-0000000000e1')$$,
  '%estimates_property_belongs_to_client%', 'An estimate cannot use another customer''s property');
select tests.throws($$update public.visits set job_id = 'f1a00000-0000-0000-0000-0000000000e1' where id = '7a500000-0000-0000-0000-000000000001'$$,
  '%visit_has_history%', 'Finished work cannot be moved to another customer''s job');
select tests.throws($$update public.jobs set client_id = 'c1a00000-0000-0000-0000-0000000000e1', property_id = 'd1a00000-0000-0000-0000-0000000000e1'
  where id = 'f1a00000-0000-0000-0000-000000000001'$$, '%job_has_history%', 'A job with finished visits keeps its customer');
-- An open job can change hands; its scheduled visits follow.
insert into public.visits (id, tenant_id, job_id, scheduled_date) values
  ('7a500000-0000-0000-0000-0000000000e2', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-0000000000e1', '2030-01-01');
update public.properties set client_id = 'c1a00000-0000-0000-0000-000000000001' where id = 'd1a00000-0000-0000-0000-0000000000e1'
  and false;  -- (properties stay put)
insert into public.properties (id, tenant_id, client_id, address_line1) values
  ('d1a00000-0000-0000-0000-0000000000e3', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', '3 New Owner Rd');
update public.jobs set client_id = 'c1a00000-0000-0000-0000-000000000001', property_id = 'd1a00000-0000-0000-0000-0000000000e3'
  where id = 'f1a00000-0000-0000-0000-0000000000e1';
select tests.is((select client_id::text || '|' || property_id from public.visits where id = '7a500000-0000-0000-0000-0000000000e2'),
  'c1a00000-0000-0000-0000-000000000001|d1a00000-0000-0000-0000-0000000000e3', 'Scheduled visits follow the job''s new customer and property');

-- ===================================================== M4 admins and money records
select tests.login('a0000000-0000-0000-0000-000000000002');   -- admin
set role authenticated;
select tests.throws($$insert into public.employee_pay_rates (tenant_id, employee_id, hourly_rate, effective_from) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000002', 99, current_date)$$, '%row-level security%', 'Admin cannot raise their own pay');
select tests.lives($$insert into public.employee_pay_rates (tenant_id, employee_id, hourly_rate, effective_from) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000004', 17, current_date)$$, 'Admin can set a crew member''s pay');
select tests.is(tests.affected($$update public.employees set status = 'inactive' where id = 'ea000000-0000-0000-0000-000000000001'$$), 0::bigint,
  'Admin cannot deactivate the owner');
select tests.throws($$update public.employees set user_id = null where id = 'ea000000-0000-0000-0000-000000000004'$$, '%permission denied%', 'Login links are system-managed');
select tests.is(tests.affected($$update public.employees set phone = '555-0100' where id = 'ea000000-0000-0000-0000-000000000004'$$), 1::bigint, 'Admin edits crew contact details');
select tests.throws($$delete from public.invoices where id = '1a000000-0000-0000-0000-000000000001'$$, '%permission denied%', 'Invoices cannot be deleted');
select tests.throws($$update public.estimates set subtotal = 1$$, '%permission denied%', 'Estimate totals cannot be edited directly');
reset role;
update public.estimates set status = 'sent', sent_at = now() where id = 'e5a00000-0000-0000-0000-000000000001';
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select tests.is(tests.affected($$delete from public.estimates where id = 'e5a00000-0000-0000-0000-000000000001'$$), 0::bigint, 'Sent estimates cannot be deleted');
-- void
select tests.throws($$select public.void_invoice('1a000000-0000-0000-0000-000000000001', 'mistake')$$, '%invoice_has_payments%', 'Invoice with payments must have them voided first');
select tests.throws($$select public.void_payment((select id from public.payments where invoice_id = '1a000000-0000-0000-0000-000000000001' limit 1), '')$$,
  '%reason_required%', 'Voiding needs a reason');
select public.void_payment((select id from public.payments where invoice_id = '1a000000-0000-0000-0000-000000000001' limit 1), 'Check bounced');
select tests.is((select amount_paid::text || ':' || status from public.invoices where id = '1a000000-0000-0000-0000-000000000001'), '0.00:sent', 'Voided payment comes off the invoice');
select tests.lives($$select public.void_invoice('1a000000-0000-0000-0000-000000000001', 'Billed the wrong customer')$$, 'Invoice voided');
select tests.is((select status from public.invoices where id = '1a000000-0000-0000-0000-000000000001'), 'void', 'Status void');
select tests.is((select count(*) from public.invoice_lines where invoice_id = '1a000000-0000-0000-0000-000000000001'), 1::bigint, 'Lines are kept');
reset role;
select tests.ok((select count(*) from public.activity where kind in ('invoice_voided','payment_voided')) = 2, 'Both voids are in customer history');
select tests.ok((select count(*) from public.audit_log where reason like 'void%') >= 2, 'Voids are audited with the reason');
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.throws($$select public.void_invoice('1a000000-0000-0000-0000-000000000001', 'x')$$, '%forbidden%', 'Crew cannot void');
reset role;

-- Voiding frees visits to be billed again
insert into public.invoices (id, tenant_id, client_id, number, status, total) values
  ('1a000000-0000-0000-0000-0000000000e9', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'INV-E9', 'draft', 0);
insert into public.invoice_lines (tenant_id, invoice_id, visit_id, description, unit_price) values
  ('aaaaaaaa-0000-0000-0000-000000000000', '1a000000-0000-0000-0000-0000000000e9', '7a500000-0000-0000-0000-000000000001', 'Mow', 45);
update public.invoices set status = 'sent' where id = '1a000000-0000-0000-0000-0000000000e9';
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select public.void_invoice('1a000000-0000-0000-0000-0000000000e9', 'Re-billing');
reset role;
select tests.is((select voided_visit_id::text from public.invoice_lines where invoice_id = '1a000000-0000-0000-0000-0000000000e9'),
  '7a500000-0000-0000-0000-000000000001', 'The voided line remembers which visit it billed');
select tests.throws($$update public.invoice_lines set unit_price = 1 where invoice_id = '1a000000-0000-0000-0000-0000000000e9'$$, '%document_not_draft%',
  'Lines of a voided invoice stay locked');

-- ===================================================== L2 paid breaks
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select public.clock_in('aaaaaaaa-0000-0000-0000-000000000000');
select tests.is((public.start_break('aaaaaaaa-0000-0000-0000-000000000000', null, null, true)).paid, false, 'Crew cannot mark their own break as paid');
reset role;

-- ===================================================== L5 sequences
select tests.is((select count(*) from information_schema.usage_privileges
  where object_type = 'SEQUENCE' and object_schema = 'public' and grantee in ('anon','authenticated')), 0::bigint, 'No sequence access for anon/authenticated');
