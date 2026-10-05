-- Customer portal security. A customer may reach ONLY their own client
-- record's customer-facing data, in their own company, through portal_*.
--
-- Cast:
--   Carla  (c0570000-…01)  customer of A Client  (Company A)
--   Dan    (c0570000-…02)  customer of D Client  (Company A, a different customer)
--   Bella  (c0570000-…03)  customer of B Client  (Company B)

create or replace function tests.customer_sees_no_tables() returns text language plpgsql as $f$
declare t text; n bigint; leaks text := '';
begin
  for t in select c.relname from pg_class c join pg_namespace s on s.oid = c.relnamespace
           where s.nspname = 'public' and c.relkind = 'r' and c.relname <> 'scenarios'
             and has_table_privilege('authenticated', c.oid, 'select') loop
    n := tests.count(format('select 1 from public.%I', t));
    if t = 'profiles' then n := tests.count('select 1 from public.profiles where id <> auth.uid()'); end if;
    if n > 0 then leaks := leaks || t || '=' || n || ' '; end if;
  end loop;
  return nullif(leaks, '');
end $f$;

insert into auth.users (id, email) values
  ('c0570000-0000-0000-0000-000000000001', 'carla@customer.test'),
  ('c0570000-0000-0000-0000-000000000002', 'dan@customer.test'),
  ('c0570000-0000-0000-0000-000000000003', 'bella@customer.test'),
  ('c0570000-0000-0000-0000-000000000004', 'newcust@customer.test');

insert into public.clients (id, tenant_id, name, internal_notes) values
  ('c1a00000-0000-0000-0000-0000000000dd', 'aaaaaaaa-0000-0000-0000-000000000000', 'D Client', 'SECRET-INTERNAL-D');
insert into public.properties (id, tenant_id, client_id, address_line1) values
  ('d1a00000-0000-0000-0000-0000000000dd', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000dd', '9 D St');

-- Plant internal-only data on Carla's records; none of it may ever reach her.
update public.clients set internal_notes = 'SECRET-INTERNAL-NOTE', lead_source = 'SECRET-LEADSOURCE', tags = '{SECRET-TAG}',
  assigned_employee_id = 'ea000000-0000-0000-0000-000000000002'
  where id = 'c1a00000-0000-0000-0000-000000000001';
update public.properties set gate_code = 'SECRET-GATE', access_notes = 'SECRET-ACCESS-DOG-BITES', notes = 'SECRET-PROP-NOTE'
  where id = 'd1a00000-0000-0000-0000-000000000001';
update public.jobs set notes = 'SECRET-JOB-NOTE' where id = 'f1a00000-0000-0000-0000-000000000001';
update public.visits set status = 'completed', started_at = '2026-09-29 12:00+00', completed_at = '2026-09-29 13:00+00',
  completion_notes = 'SECRET-CREW-NOTE customer was rude', status_reason = 'SECRET-REASON'
  where id = '7a500000-0000-0000-0000-000000000001';
insert into public.visits (tenant_id, job_id, scheduled_date, status, status_reason) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', '2026-09-22', 'skipped', 'SECRET-SKIP-slow payer');
insert into public.activity (tenant_id, client_id, kind, summary) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'note', 'SECRET-OFFICE-NOTE owes us');
-- A draft estimate (not for customer eyes) and a sent one
update public.estimate_lines set description = 'SECRET-DRAFT-LINE' where estimate_id = 'e5a00000-0000-0000-0000-000000000001';
insert into public.estimates (id, tenant_id, number, client_id, property_id, status, sent_at, valid_until) values
  ('e5a00000-0000-0000-0000-0000000000a2', 'aaaaaaaa-0000-0000-0000-000000000000', 'EST-9002', 'c1a00000-0000-0000-0000-000000000001',
   'd1a00000-0000-0000-0000-000000000001', 'draft', null, '2030-01-01');
insert into public.estimate_lines (tenant_id, estimate_id, description, unit_price) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'e5a00000-0000-0000-0000-0000000000a2', 'Fall aeration', 140);
update public.estimates set status = 'sent', sent_at = now() where id = 'e5a00000-0000-0000-0000-0000000000a2';
insert into public.estimates (id, tenant_id, number, client_id, status, sent_at) values
  ('e5a00000-0000-0000-0000-0000000000d1', 'aaaaaaaa-0000-0000-0000-000000000000', 'EST-9003', 'c1a00000-0000-0000-0000-0000000000dd', 'sent', now());
update public.invoices set notes = 'SECRET-INVOICE-NOTE' where id = '1a000000-0000-0000-0000-000000000001';
update public.payments set reference = 'SECRET-PAYREF' where invoice_id = '1a000000-0000-0000-0000-000000000001';
insert into public.expenses (tenant_id, category, amount, note, visit_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'Materials', 999, 'SECRET-EXPENSE', '7a500000-0000-0000-0000-000000000001');

insert into public.portal_access (tenant_id, client_id, user_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'c0570000-0000-0000-0000-000000000001'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000dd', 'c0570000-0000-0000-0000-000000000002'),
  ('bbbbbbbb-0000-0000-0000-000000000000', 'c1b00000-0000-0000-0000-000000000001', 'c0570000-0000-0000-0000-000000000003');

-- ===================================================== Carla: what she CAN see
select tests.login('c0570000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is((select string_agg(client_name || '@' || company_name, ',') from public.portal_accounts()), 'A Client@Acme Lawn',
  'Customer sees only her own account');
select tests.is((select count(*) from public.portal_properties('c1a00000-0000-0000-0000-000000000001')), 1::bigint, 'Sees her property');
select tests.is((select count(*) from public.portal_services('c1a00000-0000-0000-0000-000000000001')), 1::bigint, 'Sees her recurring service');
select tests.is((select status from public.portal_visits('c1a00000-0000-0000-0000-000000000001', '2026-09-01', '2026-10-31') where visit_date = '2026-09-29'),
  'completed', 'Sees her completed visit');
select tests.is((select status from public.portal_visits('c1a00000-0000-0000-0000-000000000001', '2026-09-01', '2026-10-31') where visit_date = '2026-09-22'),
  'not_serviced', 'A skipped visit shows as not serviced, without the internal reason');
select tests.is((select string_agg(number, ',' order by number) from public.portal_estimates('c1a00000-0000-0000-0000-000000000001')), 'EST-9002',
  'Sees sent estimates only, never drafts');
select tests.is((select balance from public.portal_invoices('c1a00000-0000-0000-0000-000000000001') where number = 'INV-9001'), 25.00::numeric,
  'Sees invoice balance');

-- ===================================================== no internal data, anywhere
create temp table carla_sees as
  select 'accounts' src, to_jsonb(x)::text body from public.portal_accounts() x
  union all select 'profile', to_jsonb(x)::text from public.portal_profile('c1a00000-0000-0000-0000-000000000001') x
  union all select 'properties', to_jsonb(x)::text from public.portal_properties('c1a00000-0000-0000-0000-000000000001') x
  union all select 'services', to_jsonb(x)::text from public.portal_services('c1a00000-0000-0000-0000-000000000001') x
  union all select 'visits', to_jsonb(x)::text from public.portal_visits('c1a00000-0000-0000-0000-000000000001', '2026-01-01', '2026-12-31') x
  union all select 'estimates', to_jsonb(x)::text from public.portal_estimates('c1a00000-0000-0000-0000-000000000001') x
  union all select 'invoices', to_jsonb(x)::text from public.portal_invoices('c1a00000-0000-0000-0000-000000000001') x
  union all select 'requests', to_jsonb(x)::text from public.portal_requests('c1a00000-0000-0000-0000-000000000001') x;
select tests.ok((select count(*) from carla_sees) >= 7, 'Portal returned data to inspect');
select tests.is((select string_agg(src || ': ' || substring(body from 'SECRET[-A-Z]*'), '; ') from carla_sees where body like '%SECRET%'), null,
  'No internal notes, crew notes, gate codes, reasons, tags, lead source, payment refs or drafts in anything the portal returns');
select tests.is((select count(*) from carla_sees where body ~* '(crew|employee|assignee|hourly|rate|cost|margin|labor|actor|audit)'), 0::bigint,
  'No staff, pay, cost or audit fields in portal output');

-- ===================================================== what she CANNOT reach
select tests.is(tests.customer_sees_no_tables(), null, 'Customer reads ZERO rows from every table directly (all data only via portal)');
select tests.is((select count(*) from public.my_companies()), 0::bigint, 'Customer is not a member of any company');
select tests.is((select count(*) from public.schedule('aaaaaaaa-0000-0000-0000-000000000000', '2026-01-01', '2026-12-31')), 0::bigint, 'No dispatch schedule');
select tests.is((select count(*) from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', '2026-01-01', '2026-12-31')), 0::bigint, 'No timesheets');
select tests.is((select count(*) from public.weekly_hours('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-28')), 0::bigint, 'No payroll hours');
select tests.throws($$select * from public.visit_costing('aaaaaaaa-0000-0000-0000-000000000000', '2026-01-01', '2026-12-31')$$, '%forbidden%', 'No job costing / margins');
select tests.throws($$select public.add_client_note('c1a00000-0000-0000-0000-000000000001', 'note', 'hi')$$, '%forbidden%', 'Cannot write internal CRM notes');
select tests.throws($$select public.set_estimate_status('e5a00000-0000-0000-0000-0000000000a2', 'approved')$$, '%forbidden%', 'Cannot use the staff estimate tool');
select tests.throws($$select public.invoice_completed_visits('c1a00000-0000-0000-0000-000000000001', '2026-01-01', '2026-12-31')$$, '%forbidden%', 'Cannot create invoices');
select tests.throws($$select public.record_payment('1a000000-0000-0000-0000-000000000001', 25, 'cash')$$, '%forbidden%', 'Cannot mark her own invoice paid');
select tests.throws($$select public.invite_customer('c1a00000-0000-0000-0000-000000000001', 'friend@x.test')$$, '%forbidden%', 'Cannot invite others');
select tests.throws($$select public.clock_in('aaaaaaaa-0000-0000-0000-000000000000')$$, '%not_an_active_employee%', 'Cannot clock in');
select tests.throws($$select public.complete_visit('7a500000-0000-0000-0000-000000000001')$$, '%forbidden%', 'Cannot change visits');
select tests.is(tests.affected($$update public.clients set name = 'Hacked', internal_notes = 'x'$$), 0::bigint, 'Direct edits to client records change nothing');
select tests.is(tests.affected($$update public.estimates set status = 'approved'$$), 0::bigint, 'Direct edits to estimates change nothing');
select tests.is(tests.affected($$delete from public.service_requests$$), 0::bigint, 'Cannot delete requests');

-- other customers, same company
select tests.throws($$select * from public.portal_profile('c1a00000-0000-0000-0000-0000000000dd')$$, '%forbidden%', 'Cannot open another customer''s profile');
select tests.throws($$select * from public.portal_invoices('c1a00000-0000-0000-0000-0000000000dd')$$, '%forbidden%', 'Cannot open another customer''s invoices');
select tests.throws($$select * from public.portal_visits('c1a00000-0000-0000-0000-0000000000dd', '2026-01-01', '2026-12-31')$$, '%forbidden%', 'Cannot open another customer''s visits');
select tests.throws($$select public.portal_respond_estimate('e5a00000-0000-0000-0000-0000000000d1', 'approved')$$, '%forbidden%', 'Cannot approve another customer''s estimate');
select tests.throws($$select public.portal_submit_request('c1a00000-0000-0000-0000-0000000000dd', 'mow please')$$, '%forbidden%', 'Cannot file requests for another customer');
select tests.throws($$select public.portal_submit_request('c1a00000-0000-0000-0000-000000000001', 'mow please', 'd1a00000-0000-0000-0000-0000000000dd')$$,
  '%forbidden%', 'Cannot attach a request to another customer''s property');
-- other companies
select tests.throws($$select * from public.portal_estimates('c1b00000-0000-0000-0000-000000000001')$$, '%forbidden%', 'Cannot open another company''s customer');
select tests.throws($$select public.portal_respond_estimate('e5b00000-0000-0000-0000-000000000001', 'approved')$$, '%forbidden%', 'Cannot touch another company''s estimate');
select tests.throws($$select public.portal_respond_estimate('00000000-0000-0000-0000-000000000000', 'approved')$$, '%forbidden%', 'Unknown ids look the same as forbidden ones');

-- ===================================================== her actions: audited, in history
select tests.throws($$select public.portal_respond_estimate('e5a00000-0000-0000-0000-000000000001', 'approved')$$, '%estimate_not_open%', 'Cannot approve a draft');
select tests.is(public.portal_respond_estimate('e5a00000-0000-0000-0000-0000000000a2', 'approved', 'Please start next week'), 'approved', 'Approves her estimate');
select tests.throws($$select public.portal_respond_estimate('e5a00000-0000-0000-0000-0000000000a2', 'declined')$$, '%estimate_not_open%', 'Cannot change her answer afterwards');
select tests.ok((select public.portal_submit_request('c1a00000-0000-0000-0000-000000000001', 'Leaves piling up in back', 'd1a00000-0000-0000-0000-000000000001', '2026-11-02')) is not null,
  'Submits a service request');
select tests.is((select status from public.portal_requests('c1a00000-0000-0000-0000-000000000001') limit 1), 'new', 'Sees her request as new');
select tests.lives($$select public.portal_update_contact('c1a00000-0000-0000-0000-000000000001', '(865) 555-0199', 'text')$$, 'Updates her phone and preference');
select tests.throws($$select public.portal_update_contact('c1a00000-0000-0000-0000-000000000001', 'call me maybe!!')$$, '%invalid_phone%', 'Phone is validated');
reset role;

select tests.ok(exists (select 1 from public.activity where kind = 'estimate_approved' and data ->> 'by' = 'customer'
                        and summary like 'Customer approved estimate EST-9002 in the portal%Please start next week%'),
  'Approval is in the customer history, with her note');
select tests.ok(exists (select 1 from public.audit_log where entity_type = 'estimates' and entity_id = 'e5a00000-0000-0000-0000-0000000000a2'
                        and actor_user_id = 'c0570000-0000-0000-0000-000000000001' and reason like 'customer approved in portal%'),
  'Approval is in the audit log with who did it');
select tests.ok(exists (select 1 from public.activity where kind = 'service_requested' and summary like 'Customer requested: Leaves piling up%'),
  'Request is in the customer history');
select tests.is((select phone from public.clients where id = 'c1a00000-0000-0000-0000-000000000001'), '(865) 555-0199', 'Phone saved');
select tests.is((select name from public.clients where id = 'c1a00000-0000-0000-0000-000000000001'), 'A Client', 'Name unchanged (customers cannot rename the account)');

-- Staff see and manage requests; customers' requests are invisible to other companies
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select tests.is(tests.count('select 1 from public.service_requests'), 1::bigint, 'Office sees the new request');
select tests.is(tests.affected($$update public.service_requests set status = 'acknowledged', staff_note = 'SECRET-STAFF-NOTE'$$), 1::bigint, 'Office acknowledges it');
reset role;
select tests.login('c0570000-0000-0000-0000-000000000001');
set role authenticated;
select tests.ok((select string_agg(to_jsonb(r)::text, '') from public.portal_requests('c1a00000-0000-0000-0000-000000000001') r) not like '%SECRET%',
  'Office note on a request stays internal');
reset role;
select tests.login('b0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is(tests.count('select 1 from public.service_requests'), 0::bigint, 'Other company cannot see the request');
reset role;

-- ===================================================== revocation and invitations
update public.portal_access set status = 'revoked' where user_id = 'c0570000-0000-0000-0000-000000000001';
select tests.login('c0570000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select * from public.portal_invoices('c1a00000-0000-0000-0000-000000000001')$$, '%forbidden%', 'Revoked access stops working immediately');
select tests.is((select count(*) from public.portal_accounts()), 0::bigint, 'Revoked account disappears from her list');
reset role;

select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select public.invite_customer('c1a00000-0000-0000-0000-0000000000dd', 'newcust@customer.test') as ptok \gset
select tests.is(tests.count(format('select 1 from public.portal_invitations where token_hash = %L', :'ptok')), 0::bigint, 'Portal invite token is never stored raw');
reset role;
select tests.login('c0570000-0000-0000-0000-000000000003');  -- wrong person
set role authenticated;
select tests.throws(format($$select public.accept_portal_invitation(%L)$$, :'ptok'), '%invitation_email_mismatch%', 'Portal invite only works for the invited email');
reset role;
select tests.login('c0570000-0000-0000-0000-000000000004');
set role authenticated;
select tests.is(public.accept_portal_invitation(:'ptok'), 'c1a00000-0000-0000-0000-0000000000dd'::uuid, 'Invited customer joins');
select tests.is((select client_name from public.portal_accounts()), 'D Client', 'And sees that account only');
select tests.is(tests.count('select 1 from public.portal_access'), 0::bigint, 'Customers cannot list portal access records');
reset role;
