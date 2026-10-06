-- Crew reports a stop they couldn't do.
update public.visits set scheduled_date = private.tenant_today('aaaaaaaa-0000-0000-0000-000000000000') where id = '7a500000-0000-0000-0000-000000000001';

select tests.login('a0000000-0000-0000-0000-000000000004');   -- Cam (assigned directly)
set role authenticated;
select tests.throws($$select * from public.report_visit_problem('7a500000-0000-0000-0000-000000000001', '')$$, '%reason_required%', 'A reason is required');
select tests.is((select status from public.report_visit_problem('7a500000-0000-0000-0000-000000000001', 'Gate locked, dog loose')), 'skipped', 'Crew marks the stop not serviced');
select tests.is((select status from public.report_visit_problem('7a500000-0000-0000-0000-000000000001', 'Gate locked, dog loose')), 'skipped', 'Retry from a phone with no signal is harmless');
reset role;
select tests.ok((select count(*) from public.activity where kind = 'visit_not_serviced' and summary like '%Gate locked%') = 1, 'In customer history once');

create temp table expect as select to_char(private.tenant_today('aaaaaaaa-0000-0000-0000-000000000000'), 'Dy Mon FMDD') || ' · Gate locked, dog loose' as d;
grant select on expect to authenticated;
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is((select detail from public.owner_attention('aaaaaaaa-0000-0000-0000-000000000000') where kind = 'crew_skipped'),
  (select d from expect), 'Owner sees it to reschedule');
reset role;

select tests.login('b0000000-0000-0000-0000-000000000003');   -- other company's crew
set role authenticated;
select tests.throws($$select * from public.report_visit_problem('7a500000-0000-0000-0000-0000000000ff', 'x y z')$$, '%not_found%', 'Unknown visit');
reset role;
insert into auth.users (id, email) values ('c0570000-0000-0000-0000-0000000000f9', 'skipcust@customer.test');
insert into public.portal_access (tenant_id, client_id, user_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'c0570000-0000-0000-0000-0000000000f9');
select tests.login('c0570000-0000-0000-0000-0000000000f9');
set role authenticated;
select tests.is((select count(*) from public.portal_visits('c1a00000-0000-0000-0000-000000000001', current_date - 30, current_date + 30) v
  where to_jsonb(v)::text like '%Gate locked%'), 0::bigint, 'The customer never sees the crew''s reason');
reset role;

-- Crew membership in one step
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select public.set_crew_members('ca000000-0000-0000-0000-000000000001', array['ea000000-0000-0000-0000-000000000003','ea000000-0000-0000-0000-000000000004']::uuid[], 'ea000000-0000-0000-0000-000000000004');
reset role;
select tests.is((select string_agg(employee_id::text || ':' || is_lead, ',' order by employee_id) from public.crew_members where crew_id = 'ca000000-0000-0000-0000-000000000001'),
  'ea000000-0000-0000-0000-000000000003:false,ea000000-0000-0000-0000-000000000004:true', 'Members and lead saved together');
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select tests.throws($$select public.set_crew_members('ca000000-0000-0000-0000-000000000001', array['eb000000-0000-0000-0000-000000000003']::uuid[])$$, '%employee_not_found%',
  'Cannot add another company''s worker');
reset role;
select tests.is((select count(*) from public.crew_members where crew_id = 'ca000000-0000-0000-0000-000000000001'), 2::bigint, 'A failed save leaves the crew as it was');
