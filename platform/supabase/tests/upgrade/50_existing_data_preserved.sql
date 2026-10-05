-- Upgrade scenario only: migrations ran on an exact copy of production
-- (schema, policies, rows). No existing row may be lost or changed. New columns
-- are allowed, so each old row must still exist with every original field equal.

select tests.is(
  (select string_agg(b.t || ':' || coalesce(b.r ->> 'id', b.r ->> 'user_id'), ', ')
   from tests.before_rows b
   where not exists (
     select 1 from (
       select 'tenants' t, to_jsonb(x) r from public.tenants x union all select 'clients', to_jsonb(x) from public.clients x
       union all select 'jobs', to_jsonb(x) from public.jobs x union all select 'invoices', to_jsonb(x) from public.invoices x
       union all select 'expenses', to_jsonb(x) from public.expenses x union all select 'time_entries', to_jsonb(x) from public.time_entries x
       union all select 'memberships', to_jsonb(x) from public.memberships x union all select 'profiles', to_jsonb(x) from public.profiles x
       union all select 'scenarios', to_jsonb(x) from public.scenarios x
     ) now_rows
     where now_rows.t = b.t and now_rows.r @> b.r)),
  null, 'Every production row still exists with all original values');

select tests.is((select count(*) from tests.before_rows), 9::bigint, 'Snapshot covered all 9 production rows (+1 other-app row)');
select tests.is((select total from public.invoices where id = '5efa660b-bf18-4273-ba9f-8de0ac321074'), 350.00::numeric, '$350 invoice unchanged');
select tests.is((select count(*) from public.expenses where tenant_id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e'), 4::bigint, 'All 4 expenses kept');

-- Profiles are backfilled for existing logins (additive)
select tests.is((select count(*) from public.profiles where id in ('31d54ab5-62a5-4aa8-b1d4-08c4772e96db','56942ef9-b819-4343-9b35-dedcd4c854bd')),
  2::bigint, 'Existing logins get profiles');

-- Production has no memberships, so nobody can see the company until an owner is linked.
select tests.login('56942ef9-b819-4343-9b35-dedcd4c854bd');
set role authenticated;
select tests.is(tests.count('select 1 from public.clients'), 0::bigint, 'Unlinked login sees no company data');
-- The other app keeps working for its users
select tests.is(tests.count($$select 1 from public.scenarios where id = '5ce00000-0000-0000-0000-000000000001'$$), 1::bigint, 'Other app: user still reads own scenario');
select tests.is(tests.count('select is_pro from public.profiles where id = auth.uid()'), 1::bigint, 'Other app: user still reads own profile');
select tests.throws($$update public.profiles set is_pro = true where id = auth.uid()$$, '%permission denied%', 'User cannot grant themselves is_pro');
select tests.throws($$update public.profiles set stripe_customer_id = 'cus_x' where id = auth.uid()$$, '%permission denied%', 'User cannot change stripe_customer_id');
select tests.is(tests.affected($$update public.profiles set full_name = 'Chad' where id = auth.uid()$$), 1::bigint, 'User can set own name');
reset role;

-- Cascades removed: deleting a login no longer erases time history; deleting a client no longer erases invoices.
select tests.throws($$delete from public.clients where id = 'cc1588c7-0a3d-4188-bd36-2a5dbd109163'$$, '%foreign key%', 'Deleting a client with invoices is blocked');
select tests.throws($$delete from public.tenants where id = '055bdb3c-c8d0-47d4-aa70-a77739054d7e'$$, '%foreign key%', 'Deleting a company does not cascade through its records');
