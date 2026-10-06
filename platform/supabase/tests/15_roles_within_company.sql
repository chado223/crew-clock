-- Inside one company: crew see only what they need; admins can't take over.

-- Crew: no financial data, no pay, no audit, only their own time
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.is(tests.count('select 1 from public.invoices'), 0::bigint, 'Crew cannot see invoices');
select tests.is(tests.count('select 1 from public.expenses'), 0::bigint, 'Crew cannot see expenses');
select tests.is(tests.count('select 1 from public.employee_pay_rates'), 0::bigint, 'Crew cannot see pay rates (even their own)');
select tests.is(tests.count('select 1 from public.audit_log'), 0::bigint, 'Crew cannot see audit log');
select tests.is(tests.count('select 1 from public.invitations'), 0::bigint, 'Crew cannot see invitations');
select tests.is(tests.count('select 1 from public.activity'), 0::bigint, 'Crew cannot see CRM activity');
select tests.is(tests.count('select 1 from public.time_entries'), 1::bigint, 'Crew sees only their own time entry');
select tests.is(tests.count($$select 1 from public.time_entries where id = '7a000000-0000-0000-0000-000000000004'$$), 0::bigint, 'Crew cannot see coworker time');
select tests.is((select count(*) from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-10-31')), 1::bigint, 'Crew timesheet shows only their own shift');
select tests.is(tests.count('select 1 from public.employees'), 1::bigint, 'Crew sees only their own employee record (no coworker HR data)');
select tests.is(tests.count('select 1 from public.clients') + tests.count('select 1 from public.jobs') + tests.count('select 1 from public.properties')
  + tests.count('select 1 from public.services') + tests.count('select 1 from public.visits'), 0::bigint,
  'Crew read no customer, job, price or visit tables directly (stops come through schedule())');

select tests.throws($$insert into public.clients (tenant_id, name) values ('aaaaaaaa-0000-0000-0000-000000000000', 'x')$$, '%row-level security%', 'Crew cannot create clients');
select tests.is(tests.affected($$update public.clients set name = 'x' where tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000'$$), 0::bigint, 'Crew cannot edit clients');
select tests.is(tests.affected($$delete from public.jobs where tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000'$$), 0::bigint, 'Crew cannot delete jobs');
select tests.throws($$insert into public.time_entries (tenant_id, employee_id, clock_in) values ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000003', now())$$, '%permission denied%', 'Crew cannot insert time directly');
select tests.throws($$update public.time_entries set clock_out = clock_out + interval '5 hours'$$, '%permission denied%', 'Crew cannot edit their own hours');
select tests.throws($$delete from public.time_entries$$, '%permission denied%', 'Crew cannot delete time');
select tests.throws($$select public.correct_time_entry('7a000000-0000-0000-0000-000000000003', '2026-09-28 11:00+00', '2026-09-28 23:00+00', 'I worked more')$$, '%forbidden%', 'Crew cannot correct time');
select tests.throws($$update public.memberships set role = 'owner'$$, '%permission denied%', 'Crew cannot change memberships directly');
select tests.throws($$insert into public.memberships (tenant_id, user_id, role) values ('aaaaaaaa-0000-0000-0000-000000000000', auth.uid(), 'owner')$$, '%permission denied%', 'Crew cannot insert memberships');
select tests.throws($$select public.invite_member('aaaaaaaa-0000-0000-0000-000000000000', 'friend@x.test', 'crew')$$, '%forbidden%', 'Crew cannot invite');
select tests.throws($$update public.tenants set plan = 'enterprise'$$, '%permission denied%', 'Nobody can change plan from the client');
reset role;

-- Admin: runs operations, cannot escalate or touch owners
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select tests.is(tests.count('select 1 from public.invoices'), 2::bigint, 'Admin sees the company''s invoices');
select tests.is(tests.count('select 1 from public.time_entries'), 2::bigint, 'Admin sees all company time');
select tests.throws($$select public.set_member_role('aaaaaaaa-0000-0000-0000-000000000000', auth.uid(), 'owner')$$, '%only_owner%', 'Admin cannot promote self to owner');
select tests.throws($$select public.invite_member('aaaaaaaa-0000-0000-0000-000000000000', 'boss@x.test', 'owner')$$, '%only_owner%', 'Admin cannot invite an owner');
select tests.throws($$select public.invite_member('aaaaaaaa-0000-0000-0000-000000000000', 'mgr@x.test', 'admin')$$, '%only_owner%', 'Admin cannot invite an admin');
select tests.throws($$select public.remove_member('aaaaaaaa-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000001')$$, '%only_owner%', 'Admin cannot remove the owner');
select tests.is(tests.affected($$update public.tenants set name = 'Admin Co'$$), 0::bigint, 'Admin cannot edit company settings');
select tests.lives($$insert into public.clients (tenant_id, name) values ('aaaaaaaa-0000-0000-0000-000000000000', 'New Client')$$, 'Admin can create clients');
reset role;

-- Owner
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is(tests.affected($$update public.tenants set name = 'Acme Lawn & Landscape' where id = 'aaaaaaaa-0000-0000-0000-000000000000'$$), 1::bigint, 'Owner can rename company');
select tests.throws($$update public.tenants set timezone = 'Mars/Olympus' where id = 'aaaaaaaa-0000-0000-0000-000000000000'$$, '%invalid_timezone%', 'Invalid timezone rejected');
select tests.throws($$update public.tenants set status = 'active'$$, '%permission denied%', 'Owner cannot change account status from the client');
reset role;
