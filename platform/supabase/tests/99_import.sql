-- CSV import: preview changes nothing, real import creates customers/properties/jobs,
-- duplicates and bad rows are reported, managers of that company only.

update public.clients set email = 'existing@example.test' where id = 'c1a00000-0000-0000-0000-000000000001';
create temp table src as select '[
  {"name":"Ann Able","email":"ann@example.test","phone":"(865) 555-0001","address_line1":"10 Oak St","city":"Seymour","region":"tn","postal_code":"37865",
   "service":"Mow & edge","price":"$45.00","frequency":"weekly","day":"Tue","start_date":"2026-04-07","crew":"crew 1","tags":"corner lot, HOA","lawn_sqft":"8,000"},
  {"name":"Bob Bee","address_line1":"20 Elm St","service":"Spring cleanup","price":"250","frequency":"once","start_date":"2030-03-01","status":"lead"},
  {"name":"Cara Cee","email":"cara@example.test","address_line1":"30 Pine St","frequency":"every 2 weeks"},
  {"name":"Dup Ann","email":"ANN@example.test","address_line1":"11 Oak St"},
  {"name":"Existing","email":"existing@example.test","address_line1":"1 A St"},
  {"email":"noname@example.test"},
  {"name":"Bad Price","address_line1":"5 X St","price":"forty"},
  {"name":"Bad Freq","address_line1":"6 X St","frequency":"whenever"},
  {"name":"No Crew","address_line1":"7 X St","service":"Mow","crew":"Night Shift"},
  {"name":"Work No Address","service":"Mow"}
]'::jsonb as j;
grant select on src to authenticated;

select tests.login('a0000000-0000-0000-0000-000000000002');   -- admin
set role authenticated;
create temp table preview as select public.import_customers('aaaaaaaa-0000-0000-0000-000000000000', (select j from src), true) as r;
select tests.is((select (r->>'clients')::int from preview), 3, 'Preview: 3 customers would be created');
select tests.is((select (r->>'jobs')::int from preview), 3, 'Preview: 3 jobs');
select tests.is((select jsonb_array_length(r->'skipped') from preview), 1, 'Preview: 1 duplicate skipped (already a customer)');
select tests.is((select jsonb_array_length(r->'errors') from preview), 5, 'Preview: 5 rows with problems');
select tests.ok((select r->'errors' from preview)::text like '%Price isn''t a number: forty%', 'Errors say what is wrong');
select tests.is((select count(*) from public.clients where name in ('Ann Able','Bob Bee','Cara Cee')), 0::bigint, 'Preview creates nothing');

create temp table done as select public.import_customers('aaaaaaaa-0000-0000-0000-000000000000', (select j from src), false) as r;
select tests.is((select (r->>'clients')::int || '/' || (r->>'properties') || '/' || (r->>'jobs') from done), '3/4/3', 'Import created 3 customers, 4 properties (Ann has two), 3 jobs');
select tests.is((select count(*) from public.properties p join public.clients c on c.id = p.client_id where c.name = 'Ann Able'), 2::bigint,
  'Same customer on two rows with different addresses: one customer, two properties');
select tests.is((select region || ':' || lawn_sqft from public.properties where address_line1 = '10 Oak St'), 'TN:8000', 'Address cleaned up');
select tests.is((select interval_weeks || ':' || weekday || ':' || price from public.jobs where title = 'Mow & edge' and client_id =
  (select id from public.clients where name = 'Ann Able')), '1:2:45.00', 'Weekly on Tuesday at $45');
select tests.is((select c.name from public.jobs j join public.crews c on c.id = j.crew_id where j.title = 'Mow & edge'), 'Crew 1', 'Crew matched by name');
select tests.is((select array_to_string(tags, '|') from public.clients where name = 'Ann Able'), 'corner lot|HOA', 'Tags split');
select tests.is((select status from public.clients where name = 'Bob Bee'), 'lead', 'Lead status kept');
select tests.is((select count(*) from public.visits v join public.jobs j on j.id = v.job_id where j.title = 'Spring cleanup' and v.scheduled_date = '2030-03-01'),
  1::bigint, 'One-time job gets its visit');
select tests.ok((select count(*) from public.visits v join public.jobs j on j.id = v.job_id
  join public.clients c on c.id = j.client_id where c.name = 'Ann Able' and v.scheduled_date >= current_date) >= 2, 'Recurring work is put on the schedule');
select tests.is((select interval_weeks from public.jobs j join public.clients c on c.id = j.client_id where c.name = 'Cara Cee')::int, 2, 'Every 2 weeks');
select tests.is((public.import_customers('aaaaaaaa-0000-0000-0000-000000000000', (select j from src), false)->>'clients')::int, 0,
  'Importing the same file twice creates nothing new');
reset role;
select tests.ok((select count(*) from public.activity where kind = 'import') = 1, 'Import recorded in history');
select tests.ok((select count(*) from public.audit_log where reason = 'CSV import' and entity_type = 'clients') >= 3, 'Imported rows are audited');

select tests.login('a0000000-0000-0000-0000-000000000003');   -- crew
set role authenticated;
select tests.throws($$select public.import_customers('aaaaaaaa-0000-0000-0000-000000000000', '[]', true)$$, '%forbidden%', 'Crew cannot import');
reset role;
select tests.login('b0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select public.import_customers('aaaaaaaa-0000-0000-0000-000000000000', '[]', false)$$, '%forbidden%', 'Other company cannot import into A');
select tests.throws($$select public.import_customers('bbbbbbbb-0000-0000-0000-000000000000', (select jsonb_agg(x) from generate_series(1, 2001) x), true)$$,
  '%import_too_large%', 'Size limit');
reset role;
