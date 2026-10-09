#!/usr/bin/env bash
# Proves a backup of the CURRENT (shared, legacy) production project restores.
#   bash verify_legacy_restore.sh <backup-dir>
# <backup-dir> holds rows.json (read-only export: every public row, sign-in users
# and identities, money totals) and schema-live.json (the live schema catalog).
# Builds a throwaway local database exactly like production's schema (baseline +
# the policies production had), checks the schema matches the live catalog,
# loads the backup, and checks every row, count and money total comes back
# identical. Nothing here connects to production; the backup never enters the repo.
set -euo pipefail
B="$(cd "${1:?backup dir}" && pwd)"
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../.." && pwd)"
if [[ -z "${PGHOST:-}" ]]; then export PGHOST=/var/tmp/cc-pgtest PGPORT=5499 PGUSER=postgres; fi
export PGOPTIONS='-c client_min_messages=warning' PGTZ=UTC   # timestamps read back as production prints them
P=(psql -X -q -v ON_ERROR_STOP=1); DB=cc_legacy_restore
W="$(mktemp -d /var/tmp/cc-lr.XXXX)"; chmod 755 "$W"; cp -r "$ROOT" "$W/s"; cp "$B/rows.json" "$W/rows.json"; chmod -R a+rX "$W"; trap 'rm -rf "$W"' EXIT
"${P[@]}" -d postgres -c "drop database if exists $DB with (force)" -c "create database $DB"
"${P[@]}" -d $DB -f "$W/s/tests/fixtures/00_supabase_shim.sql" -f "$W/s/migrations/20261005000000_baseline.sql" -f "$W/s/tests/fixtures/10_legacy_existing_state.sql"
# The fixture's sample rows out; the real backup in.
"${P[@]}" -d $DB -c "delete from public.time_entries; delete from public.expenses; delete from public.invoices; delete from public.jobs;
  delete from public.clients; delete from public.memberships; delete from public.profiles; delete from public.scenarios; delete from public.tenants; delete from auth.users;"

catalog="select jsonb_build_object(
 'columns', (select jsonb_agg(table_name||'.'||column_name||':'||data_type||':'||is_nullable||':'||coalesce(column_default,'') order by table_name, column_name) from information_schema.columns where table_schema='public'),
 'constraints', (select jsonb_agg(conrelid::regclass::text||':'||conname||':'||pg_get_constraintdef(oid) order by conrelid::regclass::text, conname) from pg_constraint where connamespace='public'::regnamespace),
 'policies', (select jsonb_agg(tablename||':'||policyname||':'||cmd||':'||coalesce(qual,'')||':'||coalesce(with_check,'') order by tablename, policyname) from pg_policies where schemaname='public'),
 'functions', (select jsonb_agg(p.oid::regprocedure::text order by 1) from pg_proc p where p.pronamespace='public'::regnamespace),
 'types', (select jsonb_agg(t.typname||':'||(select string_agg(enumlabel, ',' order by enumsortorder) from pg_enum e where e.enumtypid=t.oid) order by 1) from pg_type t where t.typnamespace='public'::regnamespace and t.typtype='e'))"
psql -X -At -d $DB -c "$catalog" > "$W/schema-restore.json"
python3 - "$B/schema-live.json" "$W/schema-restore.json" <<'PY'
import json, sys
a, b = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
bad = [k for k in a if set(a[k]) != set(b.get(k) or [])]
if bad: sys.exit(f"::error::restored schema differs from live production in: {bad}")
print("  schema: identical to live production (columns, constraints, policies, functions, types)")
PY

"${P[@]}" -d $DB -v rows="$W/rows.json" <<'SQL'
\set content `cat :rows`
create temp table b as select :'content'::jsonb d;
-- Sign-in users: the parts other tables depend on (id, email, created). The full
-- sign-in records stay in Supabase's own backups; nobody's password is exported.
insert into auth.users (id, email, created_at, raw_user_meta_data)
  select (u->>'id')::uuid, u->>'email', (u->>'created_at')::timestamptz, coalesce(u->'raw_user_meta_data', '{}')
  from jsonb_array_elements((select d->'auth_users' from b)) u;
insert into public.tenants select * from jsonb_populate_recordset(null::public.tenants, (select d->'tenants' from b));
insert into public.clients select * from jsonb_populate_recordset(null::public.clients, (select d->'clients' from b));
insert into public.jobs select * from jsonb_populate_recordset(null::public.jobs, (select d->'jobs' from b));
insert into public.invoices select * from jsonb_populate_recordset(null::public.invoices, (select d->'invoices' from b));
insert into public.expenses select * from jsonb_populate_recordset(null::public.expenses, (select d->'expenses' from b));
insert into public.time_entries select * from jsonb_populate_recordset(null::public.time_entries, (select d->'time_entries' from b));
insert into public.memberships select * from jsonb_populate_recordset(null::public.memberships, (select d->'memberships' from b));
insert into public.profiles select * from jsonb_populate_recordset(null::public.profiles, (select d->'profiles' from b));
insert into public.scenarios select * from jsonb_populate_recordset(null::public.scenarios, (select d->'scenarios' from b));
SQL

# Read everything back the same way it was exported and compare value by value.
psql -X -At -d $DB -c "select jsonb_build_object(
  'tenants', (select coalesce(jsonb_agg(to_jsonb(x) order by x.id), '[]') from public.tenants x),
  'clients', (select coalesce(jsonb_agg(to_jsonb(x) order by x.id), '[]') from public.clients x),
  'jobs', (select coalesce(jsonb_agg(to_jsonb(x) order by x.id), '[]') from public.jobs x),
  'invoices', (select coalesce(jsonb_agg(to_jsonb(x) order by x.id), '[]') from public.invoices x),
  'expenses', (select coalesce(jsonb_agg(to_jsonb(x) order by x.id), '[]') from public.expenses x),
  'time_entries', (select coalesce(jsonb_agg(to_jsonb(x) order by x.id), '[]') from public.time_entries x),
  'memberships', (select coalesce(jsonb_agg(to_jsonb(x)), '[]') from public.memberships x),
  'profiles', (select coalesce(jsonb_agg(to_jsonb(x)), '[]') from public.profiles x),
  'scenarios', (select coalesce(jsonb_agg(to_jsonb(x)), '[]') from public.scenarios x),
  'auth_users', (select coalesce(jsonb_agg(u.id::text || '|' || u.email order by u.id), '[]') from auth.users u),
  'totals', jsonb_build_object('invoice_total', (select sum(total) from public.invoices), 'expense_total', (select sum(amount) from public.expenses)))" > "$W/restored.json"
python3 - "$W/rows.json" "$W/restored.json" <<'PY'
import json, sys
from decimal import Decimal
src = json.load(open(sys.argv[1]), parse_float=Decimal); got = json.load(open(sys.argv[2]), parse_float=Decimal)
src["auth_users"] = sorted(f"{u['id']}|{u['email']}" for u in src["auth_users"])
def norm(v): return json.loads(json.dumps(v, default=str), parse_float=Decimal)
fail = []
for k in ["tenants","clients","jobs","invoices","expenses","time_entries","memberships","profiles","scenarios","auth_users"]:
    a, b = norm(src[k]), norm(got[k])
    print(f"  {k:13s} backup {len(a):2d}  restored {len(b):2d}  {'identical' if a == b else 'DIFFERENT'}")
    if a != b: fail.append(k)
for t in ["invoice_total", "expense_total"]:
    a, b = Decimal(str(src["totals"][t])), Decimal(str(got["totals"][t] or 0))
    print(f"  {t:13s} backup {a}  restored {b}  {'match' if a == b else 'MISMATCH'}")
    if a != b: fail.append(t)
if fail: sys.exit(f"::error::restore check failed: {fail}")
print("RESTORE VERIFIED")
PY
"${P[@]}" -d postgres -c "drop database $DB with (force)"
