-- Batch invoicing with company defaults; company profile edits.

update public.visits set status = 'completed', completed_at = '2026-09-29 15:00+00', price = 45 where id = '7a500000-0000-0000-0000-000000000001';
insert into public.clients (id, tenant_id, name) values ('c1a00000-0000-0000-0000-0000000000b1', 'aaaaaaaa-0000-0000-0000-000000000000', 'Billy Batch');
insert into public.properties (id, tenant_id, client_id, address_line1) values
  ('d1a00000-0000-0000-0000-0000000000b1', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000b1', '8 Batch Rd');
insert into public.jobs (id, tenant_id, client_id, property_id, title, price) values
  ('f1a00000-0000-0000-0000-0000000000b1', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000b1', 'd1a00000-0000-0000-0000-0000000000b1', 'Mow', 60);
insert into public.visits (tenant_id, job_id, scheduled_date, status, completed_at) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-0000000000b1', '2026-09-10', 'completed', '2026-09-10 15:00+00'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-0000000000b1', '2026-09-17', 'completed', '2026-09-17 15:00+00'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-0000000000b1', '2026-10-17', 'completed', '2026-10-17 15:00+00');

select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.lives($$update public.tenants set default_tax_rate = 0.0975, payment_terms_days = 15, invoice_note = 'Thank you!', phone = '865-555-0100'
  where id = 'aaaaaaaa-0000-0000-0000-000000000000'$$, 'Owner sets company profile, tax and terms');
select tests.throws($$update public.tenants set default_tax_rate = 1.5 where id = 'aaaaaaaa-0000-0000-0000-000000000000'$$, '%check%', 'Tax rate must be under 100%');
create temp table run1 as select * from public.invoice_all_completed('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30');
select tests.is((select count(*) from run1), 2::bigint, 'One invoice per customer with unbilled work in September');
select tests.is((select total from run1 where client_name = 'Billy Batch'), 131.70::numeric, 'Two $60 visits + 9.75% tax');
select tests.is((select visits from run1 where client_name = 'Billy Batch'), 2, 'October work is not included');
select tests.is((select tax_rate::text || ':' || notes || ':' || (due_at::date - issued_at::date) from public.invoices where id = (select invoice_id from run1 where client_name = 'Billy Batch')),
  '0.0975:Thank you!:15', 'Company tax, note and terms applied');
select tests.is((select count(*) from public.invoice_all_completed('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30')), 0::bigint,
  'Running again creates nothing (each visit billed once)');
select tests.lives($$insert into public.invoice_lines (tenant_id, invoice_id, description, quantity, unit_price) values
  ('aaaaaaaa-0000-0000-0000-000000000000', (select invoice_id from run1 where client_name = 'Billy Batch'), 'Mulch (3 bags)', 3, 6.50)$$, 'Extra charge added to a draft');
select tests.is((select subtotal from public.invoices where id = (select invoice_id from run1 where client_name = 'Billy Batch')), 139.50::numeric, 'Totals follow');
reset role;

select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.throws($$select * from public.invoice_all_completed('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30')$$, '%forbidden%', 'Crew cannot invoice');
select tests.is(tests.affected($$update public.tenants set default_tax_rate = 0 where id = 'aaaaaaaa-0000-0000-0000-000000000000'$$), 0::bigint, 'Crew cannot change company settings');
reset role;
select tests.login('b0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select * from public.invoice_all_completed('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-30')$$, '%forbidden%', 'Other company cannot invoice A');
reset role;
