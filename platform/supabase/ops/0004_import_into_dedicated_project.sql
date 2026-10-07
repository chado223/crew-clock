-- Load the ops/0003 export into the NEW dedicated production project.
-- DO NOT RUN WITHOUT CHAD'S APPROVAL.
--
-- Prerequisites: the new project has every migration applied (empty, fresh).
--   psql "$NEW_DB_URL" -X -v ON_ERROR_STOP=1 -v export_file=crew-clock-export.json -f 0004_import_into_dedicated_project.sql
--
-- * Keeps every original id, date and amount (only columns present in the
--   export are written; new columns get their normal defaults).
-- * All or nothing: one transaction; any mismatch with the export's manifest
--   (row counts, invoice and expense totals) aborts and nothing is kept.
-- * Safe to re-run: rows already present are left alone, never overwritten.
-- * Refuses to run against the old shared project.

\set ON_ERROR_STOP 1
\set export_json `cat :export_file`

begin;

create temp table move_export (doc jsonb) on commit drop;
insert into move_export values (:'export_json'::jsonb);

do $$
declare
  d jsonb := (select doc from move_export);
  tbl text;
  cols text;
  n_before bigint;
  n_new bigint;
begin
  if d->>'format' is distinct from 'crew-clock-export/1' then raise exception 'Not a Crew Clock export file'; end if;
  if exists (select 1 from information_schema.tables where table_schema = 'public' and table_name = 'scenarios')
     and exists (select 1 from public.scenarios) then
    raise exception 'This looks like the old shared project (it has the other app''s data). Aborting.';
  end if;

  perform set_config('app.audit_reason', 'Moved from the shared project (approved by Chad)', true);

  -- Parents before children.
  foreach tbl in array array['tenants','clients','jobs','invoices','expenses','time_entries'] loop
    if jsonb_array_length(d->tbl) = 0 then continue; end if;
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
