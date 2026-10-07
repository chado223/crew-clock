-- Load the ops/0003 export into the NEW dedicated production project.
-- DO NOT RUN WITHOUT CHAD'S APPROVAL.
--
-- Prerequisites: the new project has every migration applied (empty, fresh).
--   psql "$NEW_DB_URL" -X -v ON_ERROR_STOP=1 -v export_file=crew-clock-export.json \
--        -v expected_latest=<newest migration version> -f 0004_import_into_dedicated_project.sql
--
-- * Keeps every original id, date and amount (only columns present in the
--   export are written; new columns get their normal defaults).
-- * All or nothing: one transaction; any mismatch with the export's manifest
--   (row counts, invoice and expense totals) aborts and nothing is kept.
-- * Safe to re-run: rows already present are left alone, never overwritten.
-- * Refuses to run anywhere that isn't a fully migrated Crew Clock project
--   (the old shared project has no migration history, so it is refused), and
--   refuses any exported column the new schema doesn't have, instead of
--   silently dropping it.
-- * Two mappings for columns the old schema never had, both from existing data:
--   an invoice already marked sent/paid gets sent_at = its issued_at, and the
--   "Added as a customer" history line is dated when the customer was created.

\set ON_ERROR_STOP 1
\set export_json `cat :export_file`

begin;

create temp table move_export (doc jsonb, expected_latest text) on commit drop;
insert into move_export values (:'export_json'::jsonb, :'expected_latest');

do $$
declare
  d jsonb := (select doc from move_export);
  tbl text;
  cols text;
  n_before bigint;
  n_new bigint;
begin
  if d->>'format' is distinct from 'crew-clock-export/1' then raise exception 'Not a Crew Clock export file'; end if;
  if to_regclass('supabase_migrations.schema_migrations') is null
     or not exists (select 1 from supabase_migrations.schema_migrations
                    where version = (select nullif(expected_latest, '') from move_export)) then
    raise exception 'Target is not a fully migrated Crew Clock project (migration % missing). Aborting.',
      coalesce((select expected_latest from move_export), '(none given)');
  end if;
  if exists (select 1 from public.tenants t where not exists (
       select 1 from jsonb_array_elements(d->'tenants') r where (r->>'id')::uuid = t.id)) then
    raise exception 'Target already has other companies in it; expected a new, empty project. Aborting.';
  end if;
  if exists (select 1 from information_schema.tables where table_schema = 'public' and table_name = 'scenarios')
     and exists (select 1 from public.scenarios) then
    raise exception 'This looks like the old shared project (it has the other app''s data). Aborting.';
  end if;

  perform set_config('app.audit_reason', 'Moved from the shared project (approved by Chad)', true);

  -- Parents before children.
  foreach tbl in array array['tenants','clients','jobs','invoices','expenses','time_entries'] loop
    if jsonb_array_length(d->tbl) = 0 then continue; end if;
    -- Every exported column must exist in the new table: nothing is dropped silently.
    select string_agg(k, ', ' order by k) into cols
    from (select distinct jsonb_object_keys(r) k from jsonb_array_elements(d->tbl) r) keys
    where not exists (select 1 from information_schema.columns
                      where table_schema = 'public' and table_name = tbl and column_name = keys.k and is_generated = 'NEVER');
    if cols is not null then
      raise exception 'Export has %.% column(s) the new project lacks: %. Add a migration or an explicit mapping first.', 'public', tbl, cols;
    end if;
    -- Columns that exist in both the export and the new table.
    select string_agg(quote_ident(k), ', ' order by k) into cols
    from (select distinct jsonb_object_keys(r) k from jsonb_array_elements(d->tbl) r) keys
    where exists (select 1 from information_schema.columns
                  where table_schema = 'public' and table_name = tbl and column_name = keys.k
                    and is_generated = 'NEVER');
    execute format('select count(*) from public.%I', tbl) into n_before;
    execute format(
      'insert into public.%1$I (%2$s) select %2$s from jsonb_populate_recordset(null::public.%1$I, $1) on conflict (id) do nothing',
      tbl, cols) using d->tbl;
    execute format('select count(*) from public.%I', tbl) into n_new;
    raise notice '%: % rows added (% in export)', tbl, n_new - n_before, jsonb_array_length(d->tbl);
  end loop;

  -- Mappings from existing data for columns the old schema didn't have.
  update public.invoices i set sent_at = i.issued_at
   where i.sent_at is null and i.status in ('sent','partial','paid','overdue') and i.issued_at is not null
     and i.id in (select (r->>'id')::uuid from jsonb_array_elements(d->'invoices') r);
  update public.activity a set occurred_at = c.created_at, created_at = c.created_at
    from public.clients c
   where a.client_id = c.id and a.kind in ('client_created', 'lead_created') and c.created_at < a.occurred_at
     and c.id in (select (r->>'id')::uuid from jsonb_array_elements(d->'clients') r);

  -- Verify against the manifest: every exported row is here with the same money.
  if (select count(*) from public.tenants t where t.id in (select (r->>'id')::uuid from jsonb_array_elements(d->'tenants') r)) <> (d->'manifest'->>'tenants')::int
  or (select count(*) from public.clients x where x.id in (select (r->>'id')::uuid from jsonb_array_elements(d->'clients') r)) <> (d->'manifest'->>'clients')::int
  or (select count(*) from public.jobs x where x.id in (select (r->>'id')::uuid from jsonb_array_elements(d->'jobs') r)) <> (d->'manifest'->>'jobs')::int
  or (select count(*) from public.time_entries x where x.id in (select (r->>'id')::uuid from jsonb_array_elements(d->'time_entries') r)) <> (d->'manifest'->>'time_entries')::int
  or (select coalesce(sum(total), 0) from public.invoices x where x.id in (select (r->>'id')::uuid from jsonb_array_elements(d->'invoices') r)) <> (d->'manifest'->>'invoice_total')::numeric
  or (select count(*) from public.invoices x where x.id in (select (r->>'id')::uuid from jsonb_array_elements(d->'invoices') r)) <> (d->'manifest'->>'invoices')::int
  or (select coalesce(sum(amount), 0) from public.expenses x where x.id in (select (r->>'id')::uuid from jsonb_array_elements(d->'expenses') r)) <> (d->'manifest'->>'expense_total')::numeric
  or (select count(*) from public.expenses x where x.id in (select (r->>'id')::uuid from jsonb_array_elements(d->'expenses') r)) <> (d->'manifest'->>'expenses')::int
  then
    raise exception 'Verification failed: the new project does not match the export. Nothing was kept.';
  end if;
  raise notice 'Verified: counts and money totals match the export (%).', d->'manifest';
end $$;

commit;
