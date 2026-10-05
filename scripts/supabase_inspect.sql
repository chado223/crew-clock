-- Crew Clock: Supabase read-only inspection
-- ------------------------------------------------------------------
-- WHAT IT DOES: reads schema metadata only. It does not read business
-- data (no client names, no times), and it changes nothing.
--
-- HOW TO RUN: Supabase dashboard → SQL Editor → New query → paste → Run.
-- The result is ONE row with ONE cell of JSON. Click the cell, copy it,
-- and paste it back to Claude.
-- ------------------------------------------------------------------

with
tbls as (
  select c.relname as table_name,
         c.relrowsecurity as rls_enabled,
         c.relforcerowsecurity as rls_forced,
         (select count(*) from pg_policy p where p.polrelid = c.oid) as policy_count
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public' and c.relkind = 'r'
),
cols as (
  select table_name, column_name, data_type, is_nullable, column_default
  from information_schema.columns
  where table_schema = 'public'
),
cons as (
  select conrelid::regclass::text as table_name, conname, pg_get_constraintdef(oid) as def
  from pg_constraint
  where connamespace = 'public'::regnamespace
),
idx as (
  select tablename as table_name, indexname, indexdef
  from pg_indexes where schemaname = 'public'
),
pols as (
  select tablename as table_name, policyname, cmd, roles::text as roles,
         permissive, qual as using_expr, with_check as with_check_expr
  from pg_policies where schemaname = 'public'
),
funcs as (
  select p.proname as name,
         pg_get_function_identity_arguments(p.oid) as args,
         p.prosecdef as security_definer,
         p.proconfig as config,           -- shows search_path if set
         p.provolatile as volatility,
         pg_get_functiondef(p.oid) as definition,
         array(select grantee::regrole::text
               from aclexplode(coalesce(p.proacl, acldefault('f', p.proowner)))
               where privilege_type = 'EXECUTE' and grantee <> 0) as execute_grantees,
         exists(select 1 from aclexplode(coalesce(p.proacl, acldefault('f', p.proowner)))
                where privilege_type = 'EXECUTE' and grantee = 0) as execute_public
  from pg_proc p
  where p.pronamespace = 'public'::regnamespace and p.prokind = 'f'
),
trg as (
  select event_object_table as table_name, trigger_name, action_timing, event_manipulation, action_statement
  from information_schema.triggers where trigger_schema = 'public'
),
grants as (
  select table_name, grantee, string_agg(privilege_type, ',' order by privilege_type) as privs
  from information_schema.role_table_grants
  where table_schema = 'public' and grantee in ('anon','authenticated')
  group by table_name, grantee
),
counts as (
  -- row counts only (no content), from planner stats
  select relname as table_name, n_live_tup as approx_rows
  from pg_stat_user_tables where schemaname = 'public'
),
storage as (
  select id, name, public from storage.buckets
),
ext as (
  select extname, extversion from pg_extension
)
select jsonb_pretty(jsonb_build_object(
  'generated_at', now(),
  'postgres_version', version(),
  'tables',      (select jsonb_agg(to_jsonb(t) order by table_name) from tbls t),
  'columns',     (select jsonb_agg(to_jsonb(c) order by table_name, column_name) from cols c),
  'constraints', (select jsonb_agg(to_jsonb(c) order by table_name, conname) from cons c),
  'indexes',     (select jsonb_agg(to_jsonb(i) order by table_name, indexname) from idx i),
  'policies',    (select jsonb_agg(to_jsonb(p) order by table_name, policyname) from pols p),
  'functions',   (select jsonb_agg(to_jsonb(f) order by name) from funcs f),
  'triggers',    (select jsonb_agg(to_jsonb(t) order by table_name, trigger_name) from trg t),
  'grants',      (select jsonb_agg(to_jsonb(g) order by table_name, grantee) from grants g),
  'row_counts',  (select jsonb_agg(to_jsonb(r) order by table_name) from counts r),
  'storage_buckets', (select jsonb_agg(to_jsonb(s)) from storage s),
  'extensions',  (select jsonb_agg(to_jsonb(e) order by extname) from ext e)
)) as inspection;
