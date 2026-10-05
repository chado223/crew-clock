-- Two unrelated companies plus an outsider. Loaded fresh before every test file.
--
--   Company A  aaaaaaaa-…  "Acme Lawn"     owner a…01  admin a…02  crew a…03  crew a…04
--              employee e-a…05 "Old Timer" (no login, legacy history)
--   Company B  bbbbbbbb-…  "Bravo Turf"    owner b…01  crew b…03
--   Outsider   99999999-…  signed up, belongs to no company

insert into auth.users (id, email, raw_user_meta_data) values
  ('a0000000-0000-0000-0000-000000000001', 'owner@acme.test',  '{"full_name":"Ann Owner"}'),
  ('a0000000-0000-0000-0000-000000000002', 'admin@acme.test',  '{"full_name":"Al Admin"}'),
  ('a0000000-0000-0000-0000-000000000003', 'crew@acme.test',   '{"full_name":"Cy Crew"}'),
  ('a0000000-0000-0000-0000-000000000004', 'crew2@acme.test',  '{"full_name":"Cam Crew"}'),
  ('b0000000-0000-0000-0000-000000000001', 'owner@bravo.test', '{"full_name":"Bea Owner"}'),
  ('b0000000-0000-0000-0000-000000000003', 'crew@bravo.test',  '{"full_name":"Bo Crew"}'),
  ('99999999-9999-9999-9999-999999999999', 'outsider@else.test', '{"full_name":"Out Sider"}');

insert into public.tenants (id, name, timezone) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'Acme Lawn', 'America/New_York'),
  ('bbbbbbbb-0000-0000-0000-000000000000', 'Bravo Turf', 'America/Chicago');

insert into public.memberships (tenant_id, user_id, role) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000001', 'owner'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000002', 'admin'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000003', 'crew'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000004', 'crew'),
  ('bbbbbbbb-0000-0000-0000-000000000000', 'b0000000-0000-0000-0000-000000000001', 'owner'),
  ('bbbbbbbb-0000-0000-0000-000000000000', 'b0000000-0000-0000-0000-000000000003', 'crew');

insert into public.employees (id, tenant_id, user_id, display_name) values
  ('ea000000-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000001', 'Ann Owner'),
  ('ea000000-0000-0000-0000-000000000002', 'aaaaaaaa-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000002', 'Al Admin'),
  ('ea000000-0000-0000-0000-000000000003', 'aaaaaaaa-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000003', 'Cy Crew'),
  ('ea000000-0000-0000-0000-000000000004', 'aaaaaaaa-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000004', 'Cam Crew'),
  ('ea000000-0000-0000-0000-000000000005', 'aaaaaaaa-0000-0000-0000-000000000000', null, 'Old Timer'),
  ('eb000000-0000-0000-0000-000000000001', 'bbbbbbbb-0000-0000-0000-000000000000', 'b0000000-0000-0000-0000-000000000001', 'Bea Owner'),
  ('eb000000-0000-0000-0000-000000000003', 'bbbbbbbb-0000-0000-0000-000000000000', 'b0000000-0000-0000-0000-000000000003', 'Bo Crew');

insert into public.employee_pay_rates (tenant_id, employee_id, hourly_rate, effective_from) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000003', 18.00, '2026-01-01'),
  ('bbbbbbbb-0000-0000-0000-000000000000', 'eb000000-0000-0000-0000-000000000003', 19.50, '2026-01-01');

insert into public.crews (id, tenant_id, name) values
  ('ca000000-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000000', 'Crew 1'),
  ('cb000000-0000-0000-0000-000000000001', 'bbbbbbbb-0000-0000-0000-000000000000', 'Crew 1');
insert into public.crew_members (tenant_id, crew_id, employee_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ca000000-0000-0000-0000-000000000001', 'ea000000-0000-0000-0000-000000000003'),
  ('bbbbbbbb-0000-0000-0000-000000000000', 'cb000000-0000-0000-0000-000000000001', 'eb000000-0000-0000-0000-000000000003');

insert into public.clients (id, tenant_id, name) values
  ('c1a00000-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000000', 'A Client'),
  ('c1b00000-0000-0000-0000-000000000001', 'bbbbbbbb-0000-0000-0000-000000000000', 'B Client');
insert into public.properties (id, tenant_id, client_id, address_line1) values
  ('d1a00000-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', '1 A St'),
  ('d1b00000-0000-0000-0000-000000000001', 'bbbbbbbb-0000-0000-0000-000000000000', 'c1b00000-0000-0000-0000-000000000001', '1 B St');
insert into public.jobs (id, tenant_id, client_id, property_id, title) values
  ('f1a00000-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'd1a00000-0000-0000-0000-000000000001', 'A Mow'),
  ('f1b00000-0000-0000-0000-000000000001', 'bbbbbbbb-0000-0000-0000-000000000000', 'c1b00000-0000-0000-0000-000000000001', 'd1b00000-0000-0000-0000-000000000001', 'B Mow');
insert into public.invoices (tenant_id, client_id, total, status) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 100, 'sent'),
  ('bbbbbbbb-0000-0000-0000-000000000000', 'c1b00000-0000-0000-0000-000000000001', 200, 'sent');
insert into public.expenses (tenant_id, category, amount) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'Fuel', 10),
  ('bbbbbbbb-0000-0000-0000-000000000000', 'Fuel', 20);
insert into public.activity (tenant_id, client_id, kind, summary) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'note', 'A note'),
  ('bbbbbbbb-0000-0000-0000-000000000000', 'c1b00000-0000-0000-0000-000000000001', 'note', 'B note');

-- One closed shift each for A crew, A crew2, B crew (last week, fixed times)
insert into public.time_entries (id, tenant_id, employee_id, user_id, clock_in, clock_out, source) values
  ('7a000000-0000-0000-0000-000000000003', 'aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000003', 'a0000000-0000-0000-0000-000000000003', '2026-09-28 11:00+00', '2026-09-28 19:00+00', 'app'),
  ('7a000000-0000-0000-0000-000000000004', 'aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000004', 'a0000000-0000-0000-0000-000000000004', '2026-09-28 11:00+00', '2026-09-28 15:00+00', 'app'),
  ('7b000000-0000-0000-0000-000000000003', 'bbbbbbbb-0000-0000-0000-000000000000', 'eb000000-0000-0000-0000-000000000003', 'b0000000-0000-0000-0000-000000000003', '2026-09-28 12:00+00', '2026-09-28 20:00+00', 'app');

truncate public.audit_log;   -- tests start with a clean audit trail
