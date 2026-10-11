-- READ-ONLY preflight for the dedicated production project, run right before
-- cutover (and again after). Every row is one check: ok = true or the cutover stops.
--   psql "$NEW_DB_URL" -X -v expected_latest=<newest migration version> -v start=clean|import -f preflight.sql
--   start=clean  (default): fresh company made in the app; no money carried over.
--   start=import: the old project's data was imported; money must match the backup.
-- (or paste into the Supabase SQL runner with the version filled in).
\if :{?start}
\else
\set start clean
\endif
begin transaction read only;

with checks(check_name, ok, detail) as (
  -- Identity: this database calls itself production, and nothing else lives here.
  select 'marked as production', coalesce((select enabled from private.platform_flags where key = 'production'), false),
         'private.platform_flags.production must be on in the production project only'
  union all
  select 'no other app data', not exists (select 1 from public.scenarios) and not exists (
           select 1 from public.profiles where is_pro or stripe_customer_id is not null),
         'the other app''s table and columns must be empty here'
  union all
  -- Migration target: fully migrated, every file recorded with its fingerprint.
  select 'migrations complete', exists (select 1 from supabase_migrations.schema_migrations where version = :'expected_latest'),
         'newest migration ' || :'expected_latest' || ' recorded'
  union all
  select 'migration fingerprints', not exists (select 1 from supabase_migrations.schema_migrations where statements[1] not like 'md5:%'),
         'every applied migration has its fingerprint (applied by the checked script)'
  union all
  -- Customer communications stay off.
  select 'live messaging off', not coalesce((select enabled from private.platform_flags where key = 'live_messaging'), false),
         'platform-wide switch for real customer/employee messages'
  union all
  select 'online payments off', not coalesce((select enabled from private.platform_flags where key = 'online_payments'), false), ''
  union all
  select 'every company in test mode', not exists (select 1 from public.communication_settings where delivery_mode <> 'test'),
         'no company may deliver to real recipients'
  union all
  select 'no live messages queued or sent', not exists (select 1 from public.messages where mode = 'live'), ''
  union all
  -- Security posture: no table open to anonymous visitors; row security everywhere it's needed.
  select 'anon has no table access', not exists (
           select 1 from information_schema.role_table_grants where grantee = 'anon' and table_schema = 'public'),
         'anonymous visitors can read or write nothing directly'
  union all
  select 'row security on every company table', not exists (
           select 1 from pg_class c join pg_namespace n on n.oid = c.relnamespace
           where n.nspname = 'public' and c.relkind = 'r' and not c.relrowsecurity),
         (select coalesce(string_agg(c.relname, ', '), 'all on') from pg_class c join pg_namespace n on n.oid = c.relnamespace
           where n.nspname = 'public' and c.relkind = 'r' and not c.relrowsecurity)
  union all
  select 'photos bucket private', coalesce((select not public from storage.buckets where id = 'visit-photos'), false), ''
  union all
  -- Data: exactly the company the import brought, owned by the right person.
  select 'one company', (select count(*) from public.tenants) = 1, (select string_agg(name, ', ') from public.tenants)
  union all
  select 'owner is chadwasham64@gmail.com', exists (
           select 1 from public.memberships m join auth.users u on u.id = m.user_id
           where m.role = 'owner' and lower(u.email) = 'chadwasham64@gmail.com'),
         'clean start: Chad creates the company in the app (owner automatically); import: ops/0001 after first sign-in'
  union all
  select case when :'start' = 'import' then 'money matches the backup' else 'clean start: nothing carried over' end,
         case when :'start' = 'import'
              then (select coalesce(sum(total), 0) from public.invoices) = 350.00
                   and (select coalesce(sum(amount), 0) from public.expenses where voided_at is null) = 258.50
              else not exists (select 1 from public.clients) and not exists (select 1 from public.invoices)
                   and not exists (select 1 from public.expenses) and not exists (select 1 from public.time_entries)
         end,
         case when :'start' = 'import' then 'invoices $350.00, expenses $258.50 as exported from the old project'
              else 'no customers, invoices, expenses or time from the old system (checked before any real use)' end
)
select ok, check_name, detail from checks order by ok, check_name;

rollback;
