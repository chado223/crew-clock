-- Structural checks that catch mistakes in FUTURE migrations:
-- a new table without RLS, a table left open to anon, a SECURITY DEFINER
-- function without a fixed search_path, a function callable by anon.

select tests.is(
  (select string_agg(c.relname, ', ' order by c.relname) from pg_class c join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public' and c.relkind = 'r' and not c.relrowsecurity),
  null, 'Every public table has RLS enabled');

select tests.is(
  (select string_agg(table_name || ':' || privilege_type, ', ') from information_schema.role_table_grants
    where table_schema = 'public' and grantee in ('anon', 'PUBLIC')
      and table_name <> 'scenarios'),  -- belongs to another app sharing the project
  null, 'anon has no privileges on platform tables');

select tests.is(
  (select string_agg(p.oid::regprocedure::text, ', ') from pg_proc p join pg_namespace n on n.oid = p.pronamespace
    where n.nspname in ('public','private') and p.prosecdef
      and not exists (select 1 from unnest(coalesce(p.proconfig, '{}')) c where c like 'search_path=%')),
  null, 'Every SECURITY DEFINER function pins search_path');

select tests.is(
  (select string_agg(p.oid::regprocedure::text, ', ') from pg_proc p join pg_namespace n on n.oid = p.pronamespace
    where n.nspname in ('public','private')
      and (has_function_privilege('anon', p.oid, 'execute'))),
  null, 'anon cannot execute any app function');

select tests.is(
  (select string_agg(c.relname, ', ') from pg_class c join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public' and c.relkind = 'r'
      and exists (select 1 from pg_attribute a where a.attrelid = c.oid and a.attname = 'tenant_id' and not a.attisdropped)
      and c.relname not in ('audit_log')
      and not exists (select 1 from pg_trigger t where t.tgrelid = c.oid and t.tgname = 'prevent_tenant_change')),
  null, 'Every tenant-scoped table blocks moving rows between companies');

select tests.is(
  (select string_agg(tablename, ', ') from (
    select c.relname as tablename from pg_class c join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public' and c.relkind = 'r'
      and has_table_privilege('authenticated', c.oid, 'select')
      and not exists (select 1 from pg_policies p where p.schemaname = 'public' and p.tablename = c.relname and p.cmd in ('SELECT','ALL'))
  ) x),
  null, 'Every table readable by users has a SELECT policy');

select tests.is(
  (select count(*) from pg_policies where schemaname = 'public' and cmd = 'UPDATE' and with_check is null),
  0::bigint, 'Every UPDATE policy has WITH CHECK');

select tests.ok(not has_table_privilege('authenticated', 'public.audit_log', 'insert,update,delete'),
  'Audit log is append-only for users');
