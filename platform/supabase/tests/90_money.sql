-- Money: estimate -> approval -> jobs; completed visits -> invoice -> payments; customer history.

select tests.is((select total from public.invoices where id = '1a000000-0000-0000-0000-000000000001'), 45.00::numeric, 'Invoice total comes from its lines');
select tests.is((select status from public.invoices where id = '1a000000-0000-0000-0000-000000000001'), 'partial', 'Partly paid invoice shows partial');

select tests.login('a0000000-0000-0000-0000-000000000002');  -- admin
set role authenticated;

-- Estimate
select (public.create_estimate('c1a00000-0000-0000-0000-000000000001', 'd1a00000-0000-0000-0000-000000000001')).id as est \gset
select tests.ok((select number from public.estimates where id = :'est') ~ '^EST-\d+$', 'Estimate gets a company number');
select tests.throws(format($$select public.set_estimate_status(%L, 'sent')$$, :'est'), '%estimate_empty%', 'Empty estimate cannot be sent');
insert into public.estimate_lines (tenant_id, estimate_id, description, quantity, unit_price, repeat_every_weeks, est_minutes)
values ('aaaaaaaa-0000-0000-0000-000000000000', :'est', 'Weekly mow', 1, 50, 1, 45);
insert into public.estimate_lines (tenant_id, estimate_id, description, quantity, unit_price)
values ('aaaaaaaa-0000-0000-0000-000000000000', :'est', 'Spring cleanup', 1, 225);
select tests.is((select subtotal from public.estimates where id = :'est'), 275.00::numeric, 'Estimate subtotal is calculated');
select tests.lives(format($$select public.set_estimate_status(%L, 'sent')$$, :'est'), 'Estimate marked sent');
select tests.throws(format($$insert into public.estimate_lines (tenant_id, estimate_id, description, unit_price) values ('aaaaaaaa-0000-0000-0000-000000000000', %L, 'Sneaky add-on', 99)$$, :'est'),
  '%document_not_draft%', 'Lines are locked once the estimate is sent');
select tests.throws(format($$select public.convert_estimate(%L, '2026-10-06')$$, :'est'), '%estimate_not_approved%', 'Only approved estimates become jobs');
select tests.lives(format($$select public.set_estimate_status(%L, 'approved')$$, :'est'), 'Customer approval recorded');
select tests.is(public.convert_estimate(:'est', '2026-10-06', 'ca000000-0000-0000-0000-000000000001'), 2, 'Approved estimate becomes 2 jobs');
select tests.is((select kind from public.jobs where estimate_id = :'est' and title = 'Weekly mow'), 'recurring', 'Repeating line becomes a recurring job');
select tests.is((select count(*) from public.visits v join public.jobs j on j.id = v.job_id where j.estimate_id = :'est' and j.title = 'Spring cleanup'),
  1::bigint, 'One-time line becomes one scheduled visit');
select tests.ok((select count(*) from public.visits v join public.jobs j on j.id = v.job_id where j.estimate_id = :'est' and j.title = 'Weekly mow') >= 5,
  'Recurring job is already on the schedule');
select tests.is((select status from public.estimates where id = :'est'), 'converted', 'Estimate marked converted');
select tests.throws(format($$select public.convert_estimate(%L, '2026-10-06')$$, :'est'), '%estimate_not_approved%', 'Cannot convert twice');
select tests.is((select string_agg(kind, ',' order by occurred_at, seq) from public.activity where data ->> 'estimate_id' = :'est'),
  'estimate_sent,estimate_approved,estimate_converted', 'Customer history shows sent, approved, converted');
reset role;

-- Complete two visits, then bill them
update public.visits set status = 'completed', started_at = '2026-10-06 12:00+00', completed_at = '2026-10-06 13:00+00'
 where scheduled_date = '2026-10-06' and job_id in (select id from public.jobs where estimate_id = :'est');

select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select tests.throws($$select public.invoice_completed_visits('c1a00000-0000-0000-0000-000000000001', '2026-11-01', '2026-11-30')$$,
  '%nothing_to_invoice%', 'Nothing to bill in an empty period');
select (public.invoice_completed_visits('c1a00000-0000-0000-0000-000000000001', '2026-10-01', '2026-10-31', 0.0925)).id as inv \gset
select tests.is((select count(*) from public.invoice_lines where invoice_id = :'inv'), 2::bigint, 'Both completed visits billed');
select tests.is((select subtotal from public.invoices where id = :'inv'), 275.00::numeric, 'Subtotal from visit prices');
select tests.is((select tax_amount from public.invoices where id = :'inv'), 25.44::numeric, 'Tax at 9.25% rounded to the cent');
select tests.is((select total from public.invoices where id = :'inv'), 300.44::numeric, 'Total = subtotal + tax');
select tests.throws($$select public.invoice_completed_visits('c1a00000-0000-0000-0000-000000000001', '2026-10-01', '2026-10-31')$$,
  '%nothing_to_invoice%', 'A visit is never billed twice');
select tests.throws(format($$update public.invoices set total = 1 where id = %L$$, :'inv'), '%permission denied%', 'Totals cannot be typed in');
select tests.throws(format($$update public.invoices set status = 'paid' where id = %L$$, :'inv'), '%permission denied%', 'Status cannot be set directly');
select tests.throws(format($$select public.record_payment(%L, 10, 'cash')$$, :'inv'), '%invoice_not_open%', 'No payments on a draft');
select tests.lives(format($$select public.mark_invoice_sent(%L)$$, :'inv'), 'Invoice marked sent');
select tests.throws(format($$select public.record_payment(%L, 500, 'check')$$, :'inv'), '%overpayment%', 'Overpayment rejected');
select tests.is((public.record_payment(:'inv', 100, 'check', '2026-10-20', '1042')).status, 'partial', 'Partial payment');
select tests.is((public.record_payment(:'inv', 200.44, 'card')).status, 'paid', 'Paid in full');
select tests.is((select amount_paid from public.invoices where id = :'inv'), 300.44::numeric, 'Amount paid adds up');
select tests.ok(exists (select 1 from public.activity where kind = 'payment_received' and summary like 'Payment of $200.44 received for INV-%'),
  'Payment appears in the customer''s history');
reset role;

-- Crew and other companies see none of it
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.is(tests.count('select 1 from public.estimates'), 0::bigint, 'Crew cannot see estimates');
select tests.is(tests.count('select 1 from public.payments'), 0::bigint, 'Crew cannot see payments');
select tests.throws($$select public.create_estimate('c1a00000-0000-0000-0000-000000000001')$$, '%forbidden%', 'Crew cannot create estimates');
reset role;
select tests.login('b0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws(format($$select public.record_payment(%L, 1, 'cash')$$, :'inv'), '%forbidden%', 'Other company cannot record payments on A''s invoice');
select tests.throws($$select public.invoice_completed_visits('c1a00000-0000-0000-0000-000000000001', '2026-10-01', '2026-10-31')$$, '%forbidden%',
  'Other company cannot bill A''s customer');
select tests.throws($$insert into public.estimate_lines (tenant_id, estimate_id, description, unit_price) values ('bbbbbbbb-0000-0000-0000-000000000000', 'e5a00000-0000-0000-0000-000000000001', 'x', 1)$$,
  '%foreign key%', 'Cannot attach a line to another company''s estimate');
reset role;
