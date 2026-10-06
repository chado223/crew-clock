#!/usr/bin/env bash
# Proves ops/0002_revert_platform_migrations.sql returns production's starting
# point exactly: schema dump identical, original rows identical, other app intact.
# Uses the same local/CI Postgres as run.sh.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../.." && pwd)"
if [[ -z "${PGHOST:-}" ]]; then export PGHOST=/var/tmp/cc-pgtest PGPORT=5499 PGUSER=postgres; fi
export PGOPTIONS='-c client_min_messages=warning'
W="$(mktemp -d /var/tmp/cc-rev.XXXX)"; chmod 755 "$W"; cp -r "$ROOT" "$W/s"; chmod -R a+rX "$W"; trap 'rm -rf "$W"' EXIT
P=(psql -X -q -v ON_ERROR_STOP=1)
build() { # $1 db
  "${P[@]}" -d postgres -c "drop database if exists $1 with (force)" -c "create database $1"
  "${P[@]}" -d "$1" -f "$W/s/tests/fixtures/00_supabase_shim.sql" -f "$W/s/migrations/20261005000000_baseline.sql" -f "$W/s/tests/fixtures/10_legacy_existing_state.sql"
}
build cc_rev_base
build cc_rev_up
started=$("${P[@]}" -At -d cc_rev_up -c "select now()")
for f in $(ls "$W/s/migrations"/*.sql | sort | tail -n +2); do "${P[@]}" -d cc_rev_up -f "$f" >/dev/null; done
# Use the platform a little, like the first day would.
"${P[@]}" -d cc_rev_up -f "$W/s/ops/0001_link_production_owner.sql" >/dev/null
"${P[@]}" -d cc_rev_up -c "insert into public.clients (tenant_id, name) values ('055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Added after migration')"
"${P[@]}" -d cc_rev_up -v migration_started="'$started'" -f "$W/s/ops/0002_revert_platform_migrations.sql" >/dev/null

# Sorted so ACL ordering inside GRANT lists can't cause noise; any missing or extra line still shows.
dump() { pg_dump -s -n public -d "$1" | grep -v '^--' | grep -v '^$' | grep -v 'SET \|set_config\|SELECT pg_catalog\|^\\restrict\|^\\unrestrict' | sort; }
rows() { for t in tenants profiles memberships clients jobs time_entries invoices expenses scenarios; do
  "${P[@]}" -At -d "$1" -c "select '$t', md5(coalesce(string_agg((to_jsonb(x) - case when '$t' = 'scenarios' then 'created_at' else '' end)::text, '|' order by to_jsonb(x)::text), '')) from public.$t x"; done; }
pol() { "${P[@]}" -At -d "$1" -c "select tablename, policyname, cmd, coalesce(qual,''), coalesce(with_check,''), array_to_string(roles, ',') from pg_policies where schemaname in ('public','storage') order by 1,2"; }
grants() { "${P[@]}" -At -d "$1" -c "select grantee, table_name, privilege_type from information_schema.role_table_grants where table_schema='public' order by 1,2,3"; }
authtrig() { "${P[@]}" -At -d "$1" -c "select tgname from pg_trigger where tgrelid = 'auth.users'::regclass and not tgisinternal order by 1; select nspname from pg_namespace where nspname = 'private'; select id from storage.buckets order by 1"; }
fail=0
for check in dump rows pol grants authtrig; do
  if ! diff <($check cc_rev_base) <($check cc_rev_up) > "$W/$check.diff"; then
    echo "::error title=revert::$check differs after revert"; head -40 "$W/$check.diff"; fail=1
  else echo "  $check: identical"; fi
done
"${P[@]}" -d postgres -c "drop database cc_rev_base with (force)" -c "drop database cc_rev_up with (force)"
[[ $fail == 0 ]] && echo "REVERT CHECK PASSED"
exit $fail
