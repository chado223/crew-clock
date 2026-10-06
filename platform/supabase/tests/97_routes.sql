-- Routes: ordered stops per crew per day, plan metadata, permissions, isolation.

-- Two more A stops for Crew 1 on the fixture visit's day (2026-09-29).
insert into public.properties (id, tenant_id, client_id, address_line1, latitude, longitude) values
  ('d1a00000-0000-0000-0000-0000000000e2', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', '2 Route Rd', 35.90, -83.70),
  ('d1a00000-0000-0000-0000-0000000000e3', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', '3 Route Rd', 35.95, -83.60);
insert into public.jobs (id, tenant_id, client_id, property_id, title, crew_id) values
  ('f1a00000-0000-0000-0000-0000000000e2', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'd1a00000-0000-0000-0000-0000000000e2', 'Stop 2', 'ca000000-0000-0000-0000-000000000001'),
  ('f1a00000-0000-0000-0000-0000000000e3', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'd1a00000-0000-0000-0000-0000000000e3', 'Stop 3', 'ca000000-0000-0000-0000-000000000001');
insert into public.visits (id, tenant_id, job_id, scheduled_date) values
  ('7a500000-0000-0000-0000-0000000000e2', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-0000000000e2', '2026-09-29'),
  ('7a500000-0000-0000-0000-0000000000e3', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-0000000000e3', '2026-09-29');

select tests.ok((select sort_order from public.visits where id = '7a500000-0000-0000-0000-0000000000e3')
              > (select sort_order from public.visits where id = '7a500000-0000-0000-0000-0000000000e2'),
  'New visits are added to the end of the crew''s day');

-- ===================================================== yards
select tests.login('a0000000-0000-0000-0000-000000000002');   -- A admin
set role authenticated;
select tests.lives($$insert into public.yards (tenant_id, name, address_line1, latitude, longitude, is_default)
  values ('aaaaaaaa-0000-0000-0000-000000000000', 'Main yard', '100 Shop Rd', 35.89, -83.77, true)$$, 'Admin adds the company yard');
select tests.throws($$insert into public.yards (tenant_id, name, is_default) values ('aaaaaaaa-0000-0000-0000-000000000000', 'Second', true)$$,
  '%duplicate key%', 'Only one default yard');
select tests.throws($$insert into public.yards (tenant_id, name) values ('bbbbbbbb-0000-0000-0000-000000000000', 'Sneaky')$$,
  '%row-level security%', 'Cannot add a yard to another company');
reset role;

-- ===================================================== set order
select tests.login('a0000000-0000-0000-0000-000000000001');   -- A owner
set role authenticated;
select tests.lives($$select public.set_route_order('aaaaaaaa-0000-0000-0000-000000000000', 'ca000000-0000-0000-0000-000000000001', '2026-09-29',
  array['7a500000-0000-0000-0000-0000000000e3','7a500000-0000-0000-0000-000000000001','7a500000-0000-0000-0000-0000000000e2']::uuid[],
  'straight_line', false, 42, 18.25, (select id from public.yards where is_default))$$, 'Owner sets the day''s order');
select tests.is((select string_agg(job_title, ',' order by sort_order) from public.schedule('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-29', '2026-09-29')),
  'Stop 3,A Mow,Stop 2', 'Schedule (what crews see) follows the route order');
select tests.is((select provider || ':' || est_drive_minutes || ':' || est_drive_miles || ':' || stop_count from public.route_plans),
  'straight_line:42:18.3:3', 'Route plan records who planned it and the estimate');
select tests.throws($$select public.set_route_order('aaaaaaaa-0000-0000-0000-000000000000', 'ca000000-0000-0000-0000-000000000001', '2026-09-29',
  array['7a500000-0000-0000-0000-0000000000e3','7a500000-0000-0000-0000-000000000001']::uuid[])$$, '%route_mismatch%', 'Leaving a stop out is refused');
select tests.throws($$select public.set_route_order('aaaaaaaa-0000-0000-0000-000000000000', 'ca000000-0000-0000-0000-000000000001', '2026-09-29',
  array['7a500000-0000-0000-0000-0000000000e3','7a500000-0000-0000-0000-0000000000e3','7a500000-0000-0000-0000-000000000001']::uuid[])$$, '%route_mismatch%', 'Duplicates are refused');
select tests.throws($$select public.set_route_order('aaaaaaaa-0000-0000-0000-000000000000', 'ca000000-0000-0000-0000-000000000001', '2026-09-29',
  array['7a500000-0000-0000-0000-0000000000e3','7a500000-0000-0000-0000-000000000001','7b500000-0000-0000-0000-000000000001']::uuid[])$$, '%route_mismatch%',
  'Another company''s visit cannot be slipped into a route');
select tests.throws($$select public.set_route_order('aaaaaaaa-0000-0000-0000-000000000000', 'cb000000-0000-0000-0000-000000000001', '2026-09-30',
  array['7b500000-0000-0000-0000-000000000001']::uuid[])$$, '%crew_not_found%', 'Cannot order another company''s crew');
select tests.throws($$select public.set_route_order('bbbbbbbb-0000-0000-0000-000000000000', 'cb000000-0000-0000-0000-000000000001', '2026-09-30',
  array['7b500000-0000-0000-0000-000000000001']::uuid[])$$, '%forbidden%', 'Cannot order routes in another company');
select tests.throws($$select public.set_route_order('aaaaaaaa-0000-0000-0000-000000000000', 'ca000000-0000-0000-0000-000000000001', '2026-09-29',
  array['7a500000-0000-0000-0000-0000000000e3','7a500000-0000-0000-0000-000000000001','7a500000-0000-0000-0000-0000000000e2']::uuid[], 'Bad Name!')$$,
  '%check%', 'Provider names are validated');
reset role;
select tests.ok((select count(*) from public.audit_log where entity_type = 'visits' and reason like 'route order%') >= 2, 'Order changes are audited');

-- Moving a stop to another day drops the stale drive estimate.
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select public.reschedule_visit('7a500000-0000-0000-0000-0000000000e2', '2026-09-30');
select tests.is((select est_drive_miles from public.route_plans where route_date = '2026-09-29'), null::numeric, 'Estimate cleared when a stop leaves the route');
select tests.lives($$select public.set_route_order('aaaaaaaa-0000-0000-0000-000000000000', 'ca000000-0000-0000-0000-000000000001', '2026-09-29',
  array['7a500000-0000-0000-0000-000000000001','7a500000-0000-0000-0000-0000000000e3']::uuid[])$$, 'Order can be set again by hand');
select tests.is((select provider from public.route_plans where route_date = '2026-09-29'), 'manual', 'Default provider is manual');
reset role;

-- ===================================================== crew
select tests.login('a0000000-0000-0000-0000-000000000003');   -- A crew, on Crew 1
set role authenticated;
select tests.is((select count(*) from public.route_plans), 1::bigint, 'Crew member sees their crew''s route plan');
select tests.is((select count(*) from public.yards), 1::bigint, 'Crew sees the yard (start point)');
select tests.throws($$select public.set_route_order('aaaaaaaa-0000-0000-0000-000000000000', 'ca000000-0000-0000-0000-000000000001', '2026-09-29',
  array['7a500000-0000-0000-0000-0000000000e3','7a500000-0000-0000-0000-000000000001']::uuid[])$$, '%forbidden%', 'Crew cannot reorder the route');
select tests.is(tests.affected($$update public.yards set name = 'mine'$$), 0::bigint, 'Crew cannot edit yards');
reset role;
select tests.login('a0000000-0000-0000-0000-000000000004');   -- A crew2, not on Crew 1
set role authenticated;
select tests.is((select count(*) from public.route_plans), 0::bigint, 'Crew member not on that crew does not see its plan');
reset role;
select tests.login('b0000000-0000-0000-0000-000000000001');   -- B owner
set role authenticated;
select tests.is((select count(*) from public.route_plans) + (select count(*) from public.yards), 0::bigint, 'Other company sees no routes or yards');
reset role;

-- Customer sees nothing
insert into auth.users (id, email) values ('c0570000-0000-0000-0000-0000000000e1', 'rcust@customer.test');
insert into public.portal_access (tenant_id, client_id, user_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'c0570000-0000-0000-0000-0000000000e1');
select tests.login('c0570000-0000-0000-0000-0000000000e1');
set role authenticated;
select tests.is((select count(*) from public.route_plans) + (select count(*) from public.yards), 0::bigint, 'Customer sees no routes or yards');
select tests.throws($$select public.set_route_order('aaaaaaaa-0000-0000-0000-000000000000', 'ca000000-0000-0000-0000-000000000001', '2026-09-29', '{}')$$,
  '%forbidden%', 'Customer cannot touch routes');
reset role;
