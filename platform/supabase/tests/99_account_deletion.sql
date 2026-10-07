-- Delete my account: sign-in and access go, history stays, guards hold.

insert into public.time_entries (tenant_id, user_id, employee_id, clock_in, clock_out, source)
values ('aaaaaaaa-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000003', 'ea000000-0000-0000-0000-000000000003',
        '2026-09-01 12:00+00', '2026-09-01 20:00+00', 'app'),
       ('aaaaaaaa-0000-0000-0000-000000000000', 'a0000000-0000-0000-0000-000000000004', 'ea000000-0000-0000-0000-000000000004',
        now() - interval '1 hour', null, 'app');
create temp table b_before as select count(*) n from public.memberships where tenant_id = 'bbbbbbbb-0000-0000-0000-000000000000';
grant select on b_before to authenticated;

select tests.login('a0000000-0000-0000-0000-000000000003');   -- crew with history
set role authenticated;
select tests.throws($$select public.delete_my_account('yes')$$, '%confirm_required%', 'Must type DELETE');
select public.delete_my_account('DELETE');
reset role;
select tests.is((select count(*) from auth.users where id = 'a0000000-0000-0000-0000-000000000003'), 0::bigint, 'Sign-in removed');
select tests.is((select count(*) from public.memberships where user_id = 'a0000000-0000-0000-0000-000000000003'), 0::bigint, 'Access removed');
select tests.is((select count(*) from public.time_entries where employee_id = 'ea000000-0000-0000-0000-000000000003'
                 and clock_out = '2026-09-01 20:00+00' and user_id is null), 1::bigint, 'Hours kept, just unlinked from the login');
select tests.is((select status || ':' || display_name || ':' || coalesce(user_id::text, 'none') from public.employees
                 where id = 'ea000000-0000-0000-0000-000000000003'), 'inactive:Cy Crew:none', 'Employee record kept, marked inactive');
select tests.is((select count(*) from public.activity where kind = 'account_deleted'
                 and tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000'), 1::bigint, 'Company history notes it');
select tests.is((select count(*) from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-01', '2026-09-01')
                 where employee_id = 'ea000000-0000-0000-0000-000000000003'), 1::bigint, 'Still counted in the timesheet');

select tests.login('a0000000-0000-0000-0000-000000000004');   -- crew clocked in
set role authenticated;
select tests.throws($$select public.delete_my_account('DELETE')$$, '%still_clocked_in%', 'Must clock out first');
reset role;

select tests.login('a0000000-0000-0000-0000-000000000001');   -- the only owner
set role authenticated;
select tests.throws($$select public.delete_my_account('DELETE')$$, '%last_owner%', 'A company cannot be left without an owner');
reset role;
select tests.is((select count(*) from auth.users where id = 'a0000000-0000-0000-0000-000000000001'), 1::bigint, 'Owner untouched after refusal');

-- With a second owner, the first may leave.
insert into public.memberships (tenant_id, user_id, role) values ('aaaaaaaa-0000-0000-0000-000000000000', '99999999-9999-9999-9999-999999999999', 'owner');
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select public.delete_my_account('DELETE');
reset role;
select tests.is((select n from b_before), (select count(*) from public.memberships where tenant_id = 'bbbbbbbb-0000-0000-0000-000000000000'),
  'Other companies unaffected');
select tests.is((select count(*) from public.tenants where id = 'aaaaaaaa-0000-0000-0000-000000000000'), 1::bigint, 'Company and its data remain');

-- The other app's data is never removed by Crew Clock.
do $$ begin
  if to_regclass('public.scenarios') is not null then
    insert into public.scenarios (user_id, name) values ('a0000000-0000-0000-0000-000000000002', 'theirs');
  end if;
end $$;
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select case when to_regclass('public.scenarios') is not null
  then tests.throws($$select public.delete_my_account('DELETE')$$, '%other_app_data%', 'Refuses when the login has the other app''s data')
  else tests.ok(true, 'No other app in this database') end;
reset role;

set role anon;
select tests.throws($$select public.delete_my_account('DELETE')$$, '%permission denied%', 'Not callable signed out');
reset role;
