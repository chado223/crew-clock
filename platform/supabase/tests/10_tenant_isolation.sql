-- RELEASE BLOCKER: no user can read or change another company's data.
-- Runs every check for every role in Company A against Company B's rows,
-- then the reverse direction, then anonymous and outsider access.

-- Generic read/update/delete matrix over every tenant-scoped table.
create or replace function tests.cross_tenant_matrix(p_user uuid, p_label text, p_other uuid)
returns void language plpgsql as $$
declare
  t text;
  n bigint;
begin
  perform tests.login(p_user);
  execute 'set role authenticated';
  foreach t in array array['memberships','employees','employee_pay_rates','crews','crew_members','clients',
                           'properties','jobs','time_entries','time_entry_breaks','invoices','expenses',
                           'invitations','audit_log','activity'] loop
    n := tests.count(format('select 1 from public.%I where tenant_id = %L', t, p_other));
    perform tests.is(n, 0::bigint, format('%s cannot SELECT other company %s', p_label, t));
    n := tests.affected(format('update public.%I set tenant_id = tenant_id where tenant_id = %L', t, p_other));
    perform tests.is(n, 0::bigint, format('%s cannot UPDATE other company %s', p_label, t));
    n := tests.affected(format('delete from public.%I where tenant_id = %L', t, p_other));
    perform tests.is(n, 0::bigint, format('%s cannot DELETE other company %s', p_label, t));
  end loop;
  n := tests.count(format('select 1 from public.tenants where id = %L', p_other));
  perform tests.is(n, 0::bigint, format('%s cannot SELECT other company tenant row', p_label));
  n := tests.affected(format('update public.tenants set name = name where id = %L', p_other));
  perform tests.is(n, 0::bigint, format('%s cannot UPDATE other company tenant row', p_label));
  execute 'reset role';
end $$;

select tests.cross_tenant_matrix('a0000000-0000-0000-0000-000000000001', 'A owner', 'bbbbbbbb-0000-0000-0000-000000000000');
select tests.cross_tenant_matrix('a0000000-0000-0000-0000-000000000002', 'A admin', 'bbbbbbbb-0000-0000-0000-000000000000');
select tests.cross_tenant_matrix('a0000000-0000-0000-0000-000000000003', 'A crew',  'bbbbbbbb-0000-0000-0000-000000000000');
select tests.cross_tenant_matrix('b0000000-0000-0000-0000-000000000001', 'B owner', 'aaaaaaaa-0000-0000-0000-000000000000');
select tests.cross_tenant_matrix('b0000000-0000-0000-0000-000000000003', 'B crew',  'aaaaaaaa-0000-0000-0000-000000000000');

-- Inserting rows into another company (as A owner, the most privileged A role)
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$insert into public.clients (tenant_id, name) values ('bbbbbbbb-0000-0000-0000-000000000000', 'x')$$, '%row-level security%', 'A owner cannot INSERT client into B');
select tests.throws($$insert into public.properties (tenant_id, client_id, address_line1) values ('bbbbbbbb-0000-0000-0000-000000000000', 'c1b00000-0000-0000-0000-000000000001', 'x')$$, '%row-level security%', 'A owner cannot INSERT property into B');
select tests.throws($$insert into public.jobs (tenant_id, title) values ('bbbbbbbb-0000-0000-0000-000000000000', 'x')$$, '%row-level security%', 'A owner cannot INSERT job into B');
select tests.throws($$insert into public.invoices (tenant_id, total) values ('bbbbbbbb-0000-0000-0000-000000000000', 1)$$, '%row-level security%', 'A owner cannot INSERT invoice into B');
select tests.throws($$insert into public.expenses (tenant_id, category, amount) values ('bbbbbbbb-0000-0000-0000-000000000000', 'x', 1)$$, '%row-level security%', 'A owner cannot INSERT expense into B');
select tests.throws($$insert into public.employees (tenant_id, display_name) values ('bbbbbbbb-0000-0000-0000-000000000000', 'x')$$, '%row-level security%', 'A owner cannot INSERT employee into B');
select tests.throws($$insert into public.crews (tenant_id, name) values ('bbbbbbbb-0000-0000-0000-000000000000', 'x')$$, '%row-level security%', 'A owner cannot INSERT crew into B');
select tests.throws($$insert into public.activity (tenant_id, kind, summary) values ('bbbbbbbb-0000-0000-0000-000000000000', 'note', 'x')$$, '%row-level security%', 'A owner cannot INSERT activity into B');
select tests.throws($$insert into public.employee_pay_rates (tenant_id, employee_id, hourly_rate, effective_from) values ('bbbbbbbb-0000-0000-0000-000000000000', 'eb000000-0000-0000-0000-000000000003', 1, '2026-02-01')$$, '%row-level security%', 'A owner cannot INSERT pay rate into B');

-- Cross-company references: a row in A pointing at B's records
select tests.throws($$insert into public.jobs (tenant_id, client_id, title) values ('aaaaaaaa-0000-0000-0000-000000000000', 'c1b00000-0000-0000-0000-000000000001', 'x')$$, '%foreign key%', 'A job cannot reference B client');
select tests.throws($$insert into public.properties (tenant_id, client_id, address_line1) values ('aaaaaaaa-0000-0000-0000-000000000000', 'c1b00000-0000-0000-0000-000000000001', 'x')$$, '%foreign key%', 'A property cannot reference B client');
select tests.throws($$insert into public.invoices (tenant_id, client_id, total) values ('aaaaaaaa-0000-0000-0000-000000000000', 'c1b00000-0000-0000-0000-000000000001', 1)$$, '%foreign key%', 'A invoice cannot reference B client');
select tests.throws($$insert into public.crew_members (tenant_id, crew_id, employee_id) values ('aaaaaaaa-0000-0000-0000-000000000000', 'ca000000-0000-0000-0000-000000000001', 'eb000000-0000-0000-0000-000000000003')$$, '%foreign key%', 'A crew cannot include B employee');

-- Moving a row to another company (even one you also belong to) is blocked
reset role;
insert into public.memberships (tenant_id, user_id, role) values ('bbbbbbbb-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000001', 'owner');
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$update public.clients set tenant_id = 'bbbbbbbb-0000-0000-0000-000000000000' where id = 'c1a00000-0000-0000-0000-000000000001'$$, '%tenant_id_immutable%', 'Owner of both companies cannot move a client between them');
reset role;
delete from public.memberships where tenant_id = 'bbbbbbbb-0000-0000-0000-000000000000' and user_id = 'a0000000-0000-0000-0000-000000000001';

-- Cross-company function calls
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select public.correct_time_entry('7b000000-0000-0000-0000-000000000003', now() - interval '3 hours', now(), 'test reason')$$, '%forbidden%', 'A owner cannot correct B time entry');
select tests.throws($$select public.void_time_entry('7b000000-0000-0000-0000-000000000003', 'test reason')$$, '%forbidden%', 'A owner cannot void B time entry');
select tests.throws($$select public.add_time_entry('bbbbbbbb-0000-0000-0000-000000000000', 'eb000000-0000-0000-0000-000000000003', now() - interval '3 hours', now() - interval '1 hour', 'test reason')$$, '%forbidden%', 'A owner cannot add time in B');
select tests.throws($$select public.add_time_entry('aaaaaaaa-0000-0000-0000-000000000000', 'eb000000-0000-0000-0000-000000000003', now() - interval '3 hours', now() - interval '1 hour', 'test reason')$$, '%employee_not_found%', 'A owner cannot add A time for a B employee');
select tests.throws($$select public.clock_in('bbbbbbbb-0000-0000-0000-000000000000')$$, '%not_an_active_employee%', 'A owner cannot clock in at B');
select tests.throws($$select public.invite_member('bbbbbbbb-0000-0000-0000-000000000000', 'x@y.test', 'crew')$$, '%forbidden%', 'A owner cannot invite into B');
select tests.throws($$select public.remove_member('bbbbbbbb-0000-0000-0000-000000000000', 'b0000000-0000-0000-0000-000000000003')$$, '%forbidden%', 'A owner cannot remove B member');
select tests.throws($$select public.set_member_role('bbbbbbbb-0000-0000-0000-000000000000', 'b0000000-0000-0000-0000-000000000003', 'admin')$$, '%only_owner%', 'A owner cannot change B roles');
select tests.is((select count(*) from public.timesheet('bbbbbbbb-0000-0000-0000-000000000000', '2026-09-01', '2026-10-31')), 0::bigint, 'A owner timesheet for B returns nothing');
select tests.is((select count(*) from public.weekly_hours('bbbbbbbb-0000-0000-0000-000000000000', '2026-09-28')), 0::bigint, 'A owner weekly_hours for B returns nothing');
select tests.is((select count(*) from public.my_companies()), 1::bigint, 'A owner my_companies lists only A');
reset role;

-- Outsider (signed in, no company) sees nothing anywhere
select tests.login('99999999-9999-9999-9999-999999999999');
set role authenticated;
select tests.is(tests.count('select 1 from public.tenants'), 0::bigint, 'Outsider sees no companies');
select tests.is(tests.count('select 1 from public.clients'), 0::bigint, 'Outsider sees no clients');
select tests.is(tests.count('select 1 from public.time_entries'), 0::bigint, 'Outsider sees no time entries');
select tests.is(tests.count('select 1 from public.employees'), 0::bigint, 'Outsider sees no employees');
select tests.is(tests.count('select 1 from public.profiles where id <> auth.uid()'), 0::bigint, 'Outsider sees no other profiles');
reset role;

-- Anonymous (not signed in) has no access at all
select tests.login(null);
set role anon;
select tests.throws('select * from public.clients', '%permission denied%', 'Anon cannot read clients');
select tests.throws('select * from public.tenants', '%permission denied%', 'Anon cannot read tenants');
select tests.throws('select * from public.time_entries', '%permission denied%', 'Anon cannot read time entries');
select tests.throws($$select public.clock_in('aaaaaaaa-0000-0000-0000-000000000000')$$, '%permission denied%', 'Anon cannot call clock_in');
select tests.throws($$select public.create_tenant('Anon Co')$$, '%permission denied%', 'Anon cannot create a company');
reset role;

-- Profiles: you see yourself and people you work with, nobody else
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.is(tests.count($$select 1 from public.profiles where id = 'b0000000-0000-0000-0000-000000000001'$$), 0::bigint, 'A crew cannot see B owner profile');
select tests.is(tests.count($$select 1 from public.profiles where id = 'a0000000-0000-0000-0000-000000000001'$$), 1::bigint, 'A crew can see A owner profile');
select tests.is(tests.affected($$update public.profiles set full_name = 'hacked' where id = 'a0000000-0000-0000-0000-000000000001'$$), 0::bigint, 'A crew cannot edit A owner profile');
reset role;
