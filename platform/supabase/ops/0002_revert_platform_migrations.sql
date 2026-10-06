-- EMERGENCY REVERT of the platform migrations on PRODUCTION.
--
-- Use ONLY right after the first production migration, if something is wrong
-- and fixing forward isn't possible. Run it only with the owner's approval.
-- It returns the database to the exact
-- baseline schema (tables, columns, constraints, policies, grants, helper
-- functions) that production had on 2026-10-05, and keeps every row that
-- existed then. Anything created in the new platform after the migration
-- (customers, visits, time, invoices...) is DELETED by this script, so take a
-- fresh pg_dump first.
--
-- Tested: platform/supabase/tests/revert/check_revert.sh builds production's
-- starting point, applies every migration, runs this script, and requires the
-- schema dump to match the starting point exactly and the original rows to be intact.
--
-- Usage (psql, direct connection, one transaction):
--   psql "$PROD_DB_URL" -v ON_ERROR_STOP=1 -v migration_started="'2026-10-20 06:00:00+00'" -f 0002_revert_platform_migrations.sql
-- migration_started = when the migration batch began; profile rows the
-- migration created after that time (and that the other app never touched)
-- are removed, because production had none.

\set ON_ERROR_STOP on
begin;
set local lock_timeout = '5s';

-- ---------------------------------------------------------------------------
-- 0. Safety: refuse if the other app's data looks different than expected
-- ---------------------------------------------------------------------------
do $$ begin
  if to_regclass('public.scenarios') is null then raise exception 'scenarios table missing: stop and investigate'; end if;
  if exists (select 1 from storage.objects where bucket_id = 'visit-photos') then
    raise exception 'visit photos exist in storage: export them first, then remove them in the dashboard and rerun';
  end if;
end $$;

-- ---------------------------------------------------------------------------
-- 1. Hooks outside public
-- ---------------------------------------------------------------------------
drop trigger if exists zz_cc_on_auth_user_created on auth.users;
drop policy if exists visit_photos_upload on storage.objects;
drop policy if exists visit_photos_read on storage.objects;
delete from storage.buckets where id = 'visit-photos';

-- ---------------------------------------------------------------------------
-- 2. Tables the platform added (and everything hanging off them)
-- ---------------------------------------------------------------------------
do $$
declare t text;
begin
  for t in
    select c.relname from pg_class c
    where c.relnamespace = 'public'::regnamespace and c.relkind = 'r'
      and c.relname <> all (array['tenants','profiles','scenarios','memberships','clients','jobs','time_entries','invoices','expenses'])
  loop
    execute format('drop table if exists public.%I cascade', t);
  end loop;
end $$;

-- ---------------------------------------------------------------------------
-- 3. Functions: drop the platform's, restore the three original helpers
-- ---------------------------------------------------------------------------
do $$
declare f record;
begin
  for f in
    select p.oid::regprocedure as sig from pg_proc p
    where p.pronamespace = 'public'::regnamespace
      and p.proname <> all (array['in_tenant','is_admin_or_owner','is_owner'])
  loop
    execute format('drop function if exists %s cascade', f.sig);
  end loop;
end $$;

-- ---------------------------------------------------------------------------
-- 4. Policies on the original tables: drop all but the other app's, restore originals
-- ---------------------------------------------------------------------------
do $$
declare p record;
begin
  for p in select schemaname, tablename, policyname from pg_policies
           where schemaname = 'public' and tablename <> 'scenarios' loop
    execute format('drop policy if exists %I on %I.%I', p.policyname, p.schemaname, p.tablename);
  end loop;
end $$;

-- ---------------------------------------------------------------------------
-- 5. Triggers on the original tables
-- ---------------------------------------------------------------------------
do $$
declare t record;
begin
  for t in select tgname, tgrelid::regclass as rel from pg_trigger
           where not tgisinternal and tgrelid in (select oid from pg_class where relnamespace = 'public'::regnamespace) loop
    execute format('drop trigger if exists %I on %s', t.tgname, t.rel);
  end loop;
end $$;

-- ---------------------------------------------------------------------------
-- 6. Constraints and indexes the platform added to the original tables
-- ---------------------------------------------------------------------------
do $$
declare c record; i record;
begin
  for c in
    select conrelid::regclass as rel, conname from pg_constraint
    where connamespace = 'public'::regnamespace and conrelid::regclass::text <> 'scenarios'
      and conname <> all (array[
        'clients_pkey','clients_tenant_id_fkey','expenses_pkey','expenses_tenant_id_fkey','invoices_client_id_fkey',
        'invoices_pkey','invoices_tenant_id_fkey','jobs_client_id_fkey','jobs_pkey','jobs_tenant_id_fkey',
        'memberships_pkey','memberships_tenant_id_fkey','memberships_user_id_fkey','profiles_id_fkey','profiles_pkey',
        'tenants_pkey','time_entries_job_id_fkey','time_entries_pkey','time_entries_tenant_id_fkey','time_entries_user_id_fkey'])
    order by contype = 'p'   -- foreign keys / uniques / checks before any primary key
  loop
    execute format('alter table %s drop constraint if exists %I cascade', c.rel, c.conname);
  end loop;
  for i in
    select schemaname, indexname from pg_indexes
    where schemaname = 'public' and tablename <> 'scenarios' and indexname not like '%\_pkey'
  loop
    execute format('drop index if exists %I.%I', i.schemaname, i.indexname);
  end loop;
end $$;

-- Original foreign keys, with their original delete behavior
alter table public.clients drop constraint if exists clients_tenant_id_fkey,
  add constraint clients_tenant_id_fkey foreign key (tenant_id) references public.tenants (id) on delete cascade;
alter table public.expenses drop constraint if exists expenses_tenant_id_fkey,
  add constraint expenses_tenant_id_fkey foreign key (tenant_id) references public.tenants (id) on delete cascade;
alter table public.invoices drop constraint if exists invoices_client_id_fkey,
  add constraint invoices_client_id_fkey foreign key (client_id) references public.clients (id) on delete cascade;
alter table public.invoices drop constraint if exists invoices_tenant_id_fkey,
  add constraint invoices_tenant_id_fkey foreign key (tenant_id) references public.tenants (id) on delete cascade;
alter table public.jobs drop constraint if exists jobs_client_id_fkey,
  add constraint jobs_client_id_fkey foreign key (client_id) references public.clients (id) on delete set null;
alter table public.jobs drop constraint if exists jobs_tenant_id_fkey,
  add constraint jobs_tenant_id_fkey foreign key (tenant_id) references public.tenants (id) on delete cascade;
alter table public.memberships drop constraint if exists memberships_tenant_id_fkey,
  add constraint memberships_tenant_id_fkey foreign key (tenant_id) references public.tenants (id) on delete cascade;
alter table public.memberships drop constraint if exists memberships_user_id_fkey,
  add constraint memberships_user_id_fkey foreign key (user_id) references auth.users (id) on delete cascade;
alter table public.profiles drop constraint if exists profiles_id_fkey,
  add constraint profiles_id_fkey foreign key (id) references auth.users (id) on delete cascade;
alter table public.time_entries drop constraint if exists time_entries_job_id_fkey,
  add constraint time_entries_job_id_fkey foreign key (job_id) references public.jobs (id) on delete set null;
alter table public.time_entries drop constraint if exists time_entries_tenant_id_fkey,
  add constraint time_entries_tenant_id_fkey foreign key (tenant_id) references public.tenants (id) on delete cascade;

-- ---------------------------------------------------------------------------
-- 7. Rows the platform created (production had no memberships, time or profiles)
-- ---------------------------------------------------------------------------
delete from public.time_entries;
delete from public.memberships;
delete from public.invoices where id <> '5efa660b-bf18-4273-ba9f-8de0ac321074';
delete from public.expenses where id <> all (array['a9c47778-6ab2-4933-ad18-5a000f444cfe','81634274-4d0f-435e-94a4-96e9fbd131e7',
  '93439a89-8800-43cc-b863-63e73f3f2202','33153a35-5338-4e6b-bb61-88077b51ba20']::uuid[]);
delete from public.jobs where id <> 'bf955a8f-206d-493d-bfca-11b9e887d3a1';
delete from public.clients where id <> 'cc1588c7-0a3d-4188-bd36-2a5dbd109163';
delete from public.tenants where id <> '055bdb3c-c8d0-47d4-aa70-a77739054d7e';
-- Profiles the migration created (not ones the other app has written to)
delete from public.profiles p
where p.created_at >= :migration_started
  and coalesce(p.is_pro, false) = false and p.stripe_customer_id is null;

alter table public.time_entries drop constraint if exists time_entries_user_id_fkey,
  add constraint time_entries_user_id_fkey foreign key (user_id) references auth.users (id) on delete cascade;

-- ---------------------------------------------------------------------------
-- 8. Columns the platform added to the original tables
-- ---------------------------------------------------------------------------
do $$
declare c record;
  keep jsonb := '{
    "clients": ["id","tenant_id","name","email","phone","address","created_at"],
    "expenses": ["id","tenant_id","category","amount","spent_at","note"],
    "invoices": ["id","tenant_id","client_id","total","status","pdf_url","issued_at","due_at"],
    "jobs": ["id","tenant_id","client_id","title","schedule","crew_id","status","created_at"],
    "memberships": ["tenant_id","user_id","role","created_at"],
    "profiles": ["id","email","stripe_customer_id","is_pro","created_at"],
    "tenants": ["id","name","plan","created_at"],
    "time_entries": ["id","tenant_id","user_id","job_id","clock_in","clock_out","notes","created_at"]
  }';
begin
  for c in select table_name, column_name from information_schema.columns
           where table_schema = 'public' and keep ? table_name
             and not (keep -> table_name) ? column_name loop
    execute format('alter table public.%I drop column if exists %I cascade', c.table_name, c.column_name);
  end loop;
end $$;

-- Original column rules
alter table public.time_entries alter column user_id set not null;
alter table public.jobs alter column status set default 'scheduled';
alter table public.invoices alter column status set default 'draft';
alter table public.invoices alter column total set default 0;

-- ---------------------------------------------------------------------------
-- 9. Original helper functions (production's exact definitions)
-- ---------------------------------------------------------------------------
create or replace function public.in_tenant(tid uuid) returns boolean language sql stable as $$
  SELECT EXISTS (SELECT 1 FROM public.memberships m WHERE m.tenant_id = tid AND m.user_id = auth.uid());
$$;
create or replace function public.is_admin_or_owner(tid uuid) returns boolean language sql stable as $$
  SELECT EXISTS (SELECT 1 FROM public.memberships m WHERE m.tenant_id = tid AND m.user_id = auth.uid() AND m.role IN ('admin','owner'));
$$;
create or replace function public.is_owner(tid uuid) returns boolean language sql stable as $$
  SELECT EXISTS (SELECT 1 FROM public.memberships m WHERE m.tenant_id = tid AND m.user_id = auth.uid() AND m.role = 'owner');
$$;
alter function public.in_tenant(uuid) security invoker reset all;
alter function public.is_admin_or_owner(uuid) security invoker reset all;
alter function public.is_owner(uuid) security invoker reset all;
grant execute on function public.in_tenant(uuid), public.is_admin_or_owner(uuid), public.is_owner(uuid)
  to public, anon, authenticated, service_role;

-- ---------------------------------------------------------------------------
-- 10. Original policies (production, 2026-10-05)
-- ---------------------------------------------------------------------------
create policy clients_delete on public.clients for delete using (is_admin_or_owner(tenant_id));
create policy clients_insert on public.clients for insert with check (is_admin_or_owner(tenant_id));
create policy clients_select on public.clients for select using (in_tenant(tenant_id));
create policy clients_update on public.clients for update using (is_admin_or_owner(tenant_id)) with check (is_admin_or_owner(tenant_id));
create policy expenses_delete on public.expenses for delete using (is_admin_or_owner(tenant_id));
create policy expenses_insert on public.expenses for insert with check (is_admin_or_owner(tenant_id));
create policy expenses_select on public.expenses for select using (in_tenant(tenant_id));
create policy expenses_update on public.expenses for update using (is_admin_or_owner(tenant_id)) with check (is_admin_or_owner(tenant_id));
create policy invoices_delete on public.invoices for delete using (is_admin_or_owner(tenant_id));
create policy invoices_insert on public.invoices for insert with check (is_admin_or_owner(tenant_id));
create policy invoices_select on public.invoices for select using (in_tenant(tenant_id));
create policy invoices_update on public.invoices for update using (is_admin_or_owner(tenant_id)) with check (is_admin_or_owner(tenant_id));
create policy jobs_delete on public.jobs for delete using (is_admin_or_owner(tenant_id));
create policy jobs_insert on public.jobs for insert with check (is_admin_or_owner(tenant_id));
create policy jobs_select on public.jobs for select using (in_tenant(tenant_id));
create policy jobs_update on public.jobs for update using (is_admin_or_owner(tenant_id)) with check (is_admin_or_owner(tenant_id));
create policy memberships_delete on public.memberships for delete using (is_owner(tenant_id));
create policy memberships_insert on public.memberships for insert with check (is_owner(tenant_id));
create policy memberships_select on public.memberships for select using ((user_id = auth.uid()) or is_admin_or_owner(tenant_id));
create policy memberships_update on public.memberships for update using (is_owner(tenant_id)) with check (is_owner(tenant_id));
create policy "profiles insert self" on public.profiles for insert with check (auth.uid() = id);
create policy "profiles self" on public.profiles for select using (auth.uid() = id);
create policy tenants_insert on public.tenants for insert to authenticated with check (true);
create policy tenants_select on public.tenants for select using (in_tenant(id));

-- ---------------------------------------------------------------------------
-- 11. Original grants (Supabase defaults) and schemas
-- ---------------------------------------------------------------------------
do $$
declare t text;
begin
  foreach t in array array['tenants','profiles','memberships','clients','jobs','time_entries','invoices','expenses'] loop
    execute format('revoke all on public.%I from anon, authenticated, service_role', t);
    execute format('grant all on public.%I to anon, authenticated, service_role', t);
  end loop;
end $$;
drop schema if exists private cascade;
do $$ begin
  if to_regclass('supabase_migrations.schema_migrations') is not null then
    delete from supabase_migrations.schema_migrations where version > '20261005000000';
  end if;
end $$;

-- The platform's extension stays installed (harmless, used by nothing):
-- btree_gist. pgcrypto was already there.

commit;
