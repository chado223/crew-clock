-- CRM core: client record as hub, timeline written by the system, role limits.

select tests.login('a0000000-0000-0000-0000-000000000002');  -- A admin
set role authenticated;
select tests.lives($$insert into public.clients (id, tenant_id, name, kind, status, lead_source, tags)
  values ('c1a00000-0000-0000-0000-0000000000aa', 'aaaaaaaa-0000-0000-0000-000000000000', 'Maple Ridge HOA', 'commercial', 'lead', 'referral', '{hoa,mowing}')$$,
  'Admin adds a lead with tags and source');
select tests.is((select kind from public.activity where client_id = 'c1a00000-0000-0000-0000-0000000000aa' order by occurred_at limit 1), 'lead_created',
  'Adding a lead writes a timeline entry');
select tests.lives($$update public.clients set status = 'active' where id = 'c1a00000-0000-0000-0000-0000000000aa'$$, 'Admin converts lead to customer');
select tests.is((select data ->> 'to' from public.activity where client_id = 'c1a00000-0000-0000-0000-0000000000aa' and kind = 'status_changed'), 'active',
  'Status change is on the timeline');
select tests.lives($$insert into public.properties (tenant_id, client_id, address_line1) values ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000aa', '200 Maple Ridge Rd')$$,
  'Admin adds a property');
select tests.is((select count(*) from public.activity where client_id = 'c1a00000-0000-0000-0000-0000000000aa' and kind = 'property_added'), 1::bigint,
  'New property is on the timeline');
select tests.lives($$select public.add_client_note('c1a00000-0000-0000-0000-0000000000aa', 'call', 'Asked about fall aeration')$$, 'Admin logs a call');
select tests.throws($$select public.add_client_note('c1a00000-0000-0000-0000-0000000000aa', 'bogus', 'x')$$, '%invalid_activity_kind%', 'Unknown entry kind rejected');
select tests.throws($$select public.add_client_note('c1a00000-0000-0000-0000-0000000000aa', 'note', '  ')$$, '%summary_required%', 'Empty note rejected');
select tests.throws($$insert into public.activity (tenant_id, client_id, kind, summary) values ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000aa', 'payment_received', 'fake')$$,
  '%permission denied%', 'Timeline cannot be written directly (no fake payments)');
select tests.throws($$update public.clients set status = 'vip' where id = 'c1a00000-0000-0000-0000-0000000000aa'$$, '%clients_status_chk%', 'Invalid status rejected');
select tests.throws($$update public.clients set assigned_employee_id = 'eb000000-0000-0000-0000-000000000001' where id = 'c1a00000-0000-0000-0000-0000000000aa'$$,
  '%foreign key%', 'Cannot assign a client to another company''s employee');
select tests.is((select count(*) from public.activity where client_id = 'c1a00000-0000-0000-0000-0000000000aa'), 4::bigint, 'Timeline has 4 entries');
reset role;

-- Crew cannot write CRM data or read the timeline
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.throws($$select public.add_client_note('c1a00000-0000-0000-0000-0000000000aa', 'note', 'hi')$$, '%forbidden%', 'Crew cannot add notes');
select tests.is(tests.count($$select 1 from public.activity$$), 0::bigint, 'Crew cannot read the timeline');
reset role;

-- Other company cannot see or touch it
select tests.login('b0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select public.add_client_note('c1a00000-0000-0000-0000-0000000000aa', 'note', 'hi')$$, '%forbidden%', 'Other company cannot add notes');
select tests.is(tests.count($$select 1 from public.clients where id = 'c1a00000-0000-0000-0000-0000000000aa'$$), 0::bigint, 'Other company cannot see the lead');
select tests.is(tests.count($$select 1 from public.activity where client_id = 'c1a00000-0000-0000-0000-0000000000aa'$$), 0::bigint, 'Other company cannot see the timeline');
reset role;
