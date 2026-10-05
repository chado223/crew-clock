-- Upgrade scenario only: the migrations ran on top of a copy of the current
-- production shape with its existing rows. Nothing may be lost or altered.

select tests.is(
  (select string_agg(b.t || ' ' || b.n || '->' || coalesce(a.n, 0), ', ')
   from tests.before_counts b
   left join (
     select 'clients' t, count(*) n from public.clients where tenant_id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e'
     union all select 'jobs', count(*) from public.jobs where tenant_id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e'
     union all select 'time_entries', count(*) from public.time_entries where tenant_id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e'
     union all select 'invoices', count(*) from public.invoices where tenant_id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e'
     union all select 'expenses', count(*) from public.expenses where tenant_id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e'
     union all select 'memberships', count(*) from public.memberships where tenant_id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e'
     union all select 'tenants', count(*) from public.tenants where id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e'
     union all select 'profiles', count(*) from public.profiles where id = '11111111-1111-1111-1111-111111111111'
   ) a on a.t = b.t
   where a.n is distinct from b.n),
  null, 'Every pre-existing row is still there (counts unchanged)');

select tests.is(
  (select (clock_in, clock_out, notes)::text from public.time_entries where tenant_id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e'),
  ('2026-10-01 12:00:00+00'::timestamptz, '2026-10-01 14:00:00+00'::timestamptz, 'seeded shift')::text,
  'Existing time entry times and notes unchanged');
select tests.is((select source from public.time_entries where tenant_id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e'), 'pre_migration',
  'Existing time entry labeled pre_migration');
select tests.is((select total from public.invoices where tenant_id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e'), 350.00::numeric(12,2),
  'Existing invoice amount unchanged');

select tests.login('11111111-1111-1111-1111-111111111111');
set role authenticated;
select tests.is((select role from public.my_companies()), 'owner', 'Chad is still owner of Chad Washam Lawncare');
select tests.ok((select employee_id is not null from public.my_companies()), 'Chad has an employee record');
select tests.is((select worked_seconds from public.timesheet('055bdb3c-c8d0-47d4-aa70-a77739054d7e', '2026-10-01', '2026-10-01')), 7200::bigint,
  'Seeded 2-hour shift computes as 2 hours');
select tests.is(tests.count('select 1 from public.invoices'), 1::bigint, 'Chad (owner) still sees the seeded invoice');
select tests.is(tests.count($$select 1 from public.clients where name = 'Cool Springs HOA'$$), 1::bigint, 'Cool Springs HOA still visible');
reset role;
