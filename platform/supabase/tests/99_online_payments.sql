-- Online payments: off by default, service-only processor API, idempotent
-- settlement through the normal payments table, customers limited to their own invoices.

insert into auth.users (id, email) values ('c0570000-0000-0000-0000-0000000000a1', 'payer@customer.test'), ('c0570000-0000-0000-0000-0000000000a2', 'nosy@customer.test');
insert into public.clients (id, tenant_id, name) values ('c1a00000-0000-0000-0000-0000000000a2', 'aaaaaaaa-0000-0000-0000-000000000000', 'Nosy Neighbor');
insert into public.portal_access (tenant_id, client_id, user_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'c0570000-0000-0000-0000-0000000000a1'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000a2', 'c0570000-0000-0000-0000-0000000000a2');

-- ===================================================== off by default
select tests.login('c0570000-0000-0000-0000-0000000000a1');
set role authenticated;
select tests.is((select can_pay_online::text || ':' || reason || ':' || balance from public.portal_payment_options('c1a00000-0000-0000-0000-000000000001', '1a000000-0000-0000-0000-000000000001')),
  'false:not_available:25.00', 'Online payment is off until approved; customer sees the balance');
select tests.throws($$select public.payments_worker_settle('x', 'y', true)$$, '%permission denied%', 'Customers cannot settle payments');
reset role;
select tests.login('c0570000-0000-0000-0000-0000000000a2');
set role authenticated;
select tests.throws($$select * from public.portal_payment_options('c1a00000-0000-0000-0000-000000000001', '1a000000-0000-0000-0000-000000000001')$$, '%forbidden%',
  'Another customer cannot look at that invoice');
select tests.throws($$select * from public.portal_payment_options('c1a00000-0000-0000-0000-0000000000a2', '1a000000-0000-0000-0000-000000000001')$$, '%not_found%',
  'An invoice that isn''t yours does not exist for you');
reset role;
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select public.payments_worker_open('1a000000-0000-0000-0000-000000000001', 25, 'acme_pay', 'cs_1', 'https://pay.example/cs_1')$$,
  '%permission denied%', 'Even the owner cannot open processor payments from the browser');
select tests.throws($$insert into public.payment_settings (tenant_id, provider, enabled) values ('aaaaaaaa-0000-0000-0000-000000000000', 'acme_pay', true)$$,
  '%permission denied%', 'Processor connection is server-side only');
reset role;
set role service_role;
select tests.throws($$select public.payments_worker_open('1a000000-0000-0000-0000-000000000001', 25, 'acme_pay', 'cs_1', 'https://pay.example/cs_1')$$,
  '%online_payments_not_enabled%', 'Platform switch off: no checkout can be opened');
reset role;

-- ===================================================== simulated processor (switch on for this test only)
update private.platform_flags set enabled = true where key = 'online_payments';
insert into public.payment_settings (tenant_id, provider, enabled) values ('aaaaaaaa-0000-0000-0000-000000000000', 'acme_pay', true);
select tests.login('c0570000-0000-0000-0000-0000000000a1');
set role authenticated;
select tests.is((select can_pay_online from public.portal_payment_options('c1a00000-0000-0000-0000-000000000001', '1a000000-0000-0000-0000-000000000001')), true,
  'With both switches on, the customer can pay');
reset role;
set role service_role;
select tests.throws($$select public.payments_worker_open('1a000000-0000-0000-0000-000000000001', 30, 'acme_pay', 'cs_x', null)$$, '%invalid_amount%', 'Cannot charge more than the balance');
select public.payments_worker_open('1a000000-0000-0000-0000-000000000001', 25, 'acme_pay', 'cs_1', 'https://pay.example/cs_1');
select tests.is(public.payments_worker_settle('acme_pay', 'cs_1', true, 25), 'succeeded', 'Webhook settles the payment');
select tests.is(public.payments_worker_settle('acme_pay', 'cs_1', true, 25), 'succeeded', 'Duplicate webhook is harmless');
reset role;
select tests.is((select count(*) from public.payments where method = 'card' and reference = 'acme_pay:cs_1'), 1::bigint, 'Exactly one payment recorded');
select tests.is((select status || ':' || amount_paid from public.invoices where id = '1a000000-0000-0000-0000-000000000001'), 'paid:45.00',
  'Invoice updates like any other payment');
select tests.ok((select count(*) from public.activity where summary like 'Paid online: $25.00%') = 1, 'Customer history shows the online payment');

-- Mismatched amount is held for review, not guessed
update public.payments set voided_at = now(), void_reason = 'test reset' where reference = 'acme_pay:cs_1';
set role service_role;
select public.payments_worker_open('1a000000-0000-0000-0000-000000000001', 10, 'acme_pay', 'cs_2', null);
select tests.is(public.payments_worker_settle('acme_pay', 'cs_2', true, 12), 'needs_review', 'Amount mismatch held for review');
select public.payments_worker_open('1a000000-0000-0000-0000-000000000001', 10, 'acme_pay', 'cs_3', null);
select tests.is(public.payments_worker_settle('acme_pay', 'cs_3', false, null, 'card declined'), 'failed', 'Declined card recorded');
reset role;
select tests.is((select count(*) from public.payments where reference in ('acme_pay:cs_2', 'acme_pay:cs_3')), 0::bigint, 'No payment recorded for either');

select tests.login('a0000000-0000-0000-0000-000000000003');   -- crew
set role authenticated;
select tests.is((select count(*) from public.payment_requests) + (select count(*) from public.payment_settings), 0::bigint, 'Crew sees no payment data');
reset role;
select tests.login('b0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is((select count(*) from public.payment_requests) + (select count(*) from public.payment_settings), 0::bigint, 'Other company sees none');
reset role;
select tests.login('a0000000-0000-0000-0000-000000000002');   -- admin sees requests, not processor settings
set role authenticated;
select tests.is((select count(*) from public.payment_requests), 3::bigint, 'Office sees payment attempts');
select tests.is((select count(*) from public.payment_settings), 0::bigint, 'Only the owner sees processor settings');
reset role;
