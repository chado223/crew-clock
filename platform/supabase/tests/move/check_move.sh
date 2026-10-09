#!/usr/bin/env bash
# Rehearses the move to a dedicated production project:
#   old  = simulated current shared production (baseline + its real rows + other app)
#   new  = empty project built from every migration
# Export from old (ops/0003) -> import into new (ops/0004), then prove:
#   * old is byte-for-byte unchanged (schema and rows)
#   * every exported row arrived with the same values
#   * re-running the import adds nothing; the import refuses the old project
#   * Chad's owner link (ops/0001) works on the new project after his first sign-in
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../.." && pwd)"
if [[ -z "${PGHOST:-}" ]]; then export PGHOST=/var/tmp/cc-pgtest PGPORT=5499 PGUSER=postgres; fi
export PGOPTIONS='-c client_min_messages=warning'
W="$(mktemp -d /var/tmp/cc-move.XXXX)"; chmod 755 "$W"; cp -r "$ROOT" "$W/s"; chmod -R a+rwX "$W"; trap 'rm -rf "$W"' EXIT
P=(psql -X -q -v ON_ERROR_STOP=1)
fail() { echo "::error title=move::$1"; exit 1; }

"${P[@]}" -d postgres -c "drop database if exists cc_move_old with (force)" -c "create database cc_move_old" \
  -c "drop database if exists cc_move_new with (force)" -c "create database cc_move_new"
"${P[@]}" -d cc_move_old -f "$W/s/tests/fixtures/00_supabase_shim.sql" -f "$W/s/migrations/20261005000000_baseline.sql" -f "$W/s/tests/fixtures/10_legacy_existing_state.sql"
"${P[@]}" -d cc_move_new -f "$W/s/tests/fixtures/00_supabase_shim.sql"
# Build the new project exactly as production would be: with the real migration script (records history + fingerprints).
if [[ "${PGHOST:-}" == /* ]]; then NEW_URL="postgresql:///cc_move_new?host=$PGHOST&port=${PGPORT:-5432}&user=${PGUSER:-postgres}"
else NEW_URL="postgresql://${PGUSER:-postgres}${PGPASSWORD:+:$PGPASSWORD}@${PGHOST}:${PGPORT:-5432}/cc_move_new"; fi
DB_URL="$NEW_URL" bash "$W/s/scripts/apply_migrations.sh" >/dev/null
LATEST="$(ls "$W/s/migrations"/*.sql | sort | tail -1 | xargs basename | cut -d_ -f1)"
IMPORT=(-v export_file="$W/export.json" -v expected_latest="$LATEST" -f "$W/s/ops/0004_import_into_dedicated_project.sql")

snap() { pg_dump -d cc_move_old | grep -v '^--' | grep -v '^\\restrict\|^\\unrestrict'; }
snap > "$W/old_before.sql"

psql -X -At -v ON_ERROR_STOP=1 -d cc_move_old -f "$W/s/ops/0003_export_for_dedicated_project.sql" > "$W/export.json"
python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print('  export manifest:', d['manifest'])" "$W/export.json"

snap > "$W/old_after.sql"
diff -q "$W/old_before.sql" "$W/old_after.sql" >/dev/null || fail "export changed the old project"
echo "  old project unchanged by export"

"${P[@]}" -d cc_move_new "${IMPORT[@]}"

# Same values for every exported column.
python3 - "$W/export.json" > "$W/cmp.sql" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
for t in ["tenants","clients","jobs","invoices","expenses","time_entries"]:
    for r in d[t]:
        keys = ",".join("'%s'" % k for k in r)
        lit = json.dumps(r).replace("'", "''")
        print(f"select case when (select count(*) from public.{t} x where x.id = '{r['id']}' "
              f"and (select jsonb_object_agg(k, v) from jsonb_each(to_jsonb(x)) e(k, v) where k in ({keys})) "
              f"@> (select jsonb_object_agg(k, v) from jsonb_each('{lit}'::jsonb) e(k, v) where k in (select column_name from information_schema.columns where table_schema='public' and table_name='{t}'))) = 1 "
              f"then 'ok' else 'MISMATCH {t} {r['id']}' end;")
PY
out=$(psql -X -At -v ON_ERROR_STOP=1 -d cc_move_new -f "$W/cmp.sql")
echo "$out" | grep -q MISMATCH && { echo "$out" | grep MISMATCH; fail "rows differ after import"; }
echo "  $(echo "$out" | grep -c ok) rows arrived with identical values"

before=$(psql -X -At -d cc_move_new -c "select (select count(*) from public.tenants)+(select count(*) from public.clients)+(select count(*) from public.jobs)+(select count(*) from public.invoices)+(select count(*) from public.expenses)")
"${P[@]}" -d cc_move_new "${IMPORT[@]}" >/dev/null 2>&1
after=$(psql -X -At -d cc_move_new -c "select (select count(*) from public.tenants)+(select count(*) from public.clients)+(select count(*) from public.jobs)+(select count(*) from public.invoices)+(select count(*) from public.expenses)")
[[ "$before" == "$after" ]] || fail "re-running the import added rows ($before -> $after)"
echo "  re-run adds nothing ($after rows)"

# Real production has no rows in the other app's table, so the guard must not depend on them.
"${P[@]}" -d cc_move_old -c "create table move_saved_scenarios as select * from public.scenarios; delete from public.scenarios"
if "${P[@]}" -d cc_move_old "${IMPORT[@]}" >/dev/null 2>&1; then
  fail "import ran against the old shared project"
fi
"${P[@]}" -d cc_move_old -c "insert into public.scenarios select * from move_saved_scenarios; drop table move_saved_scenarios"
snap > "$W/old_after2.sql"; diff -q "$W/old_before.sql" "$W/old_after2.sql" >/dev/null || fail "refused import still changed the old project"
echo "  import refuses the old project and leaves it unchanged"

# A column the new schema doesn't have is refused, not dropped.
python3 - "$W/export.json" "$W/export-extra.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1])); d["jobs"][0]["gate_code_xyz"] = "1234"; json.dump(d, open(sys.argv[2], "w"))
PY
"${P[@]}" -d postgres -c "drop database if exists cc_move_new2 with (force)" -c "create database cc_move_new2 template cc_move_new"
"${P[@]}" -d cc_move_new2 -c "truncate public.tenants, public.clients, public.jobs, public.invoices, public.expenses cascade" >/dev/null 2>&1 || true
if out=$("${P[@]}" -d cc_move_new2 -v export_file="$W/export-extra.json" -v expected_latest="$LATEST" -f "$W/s/ops/0004_import_into_dedicated_project.sql" 2>&1); then
  fail "an unknown exported column was silently dropped"
fi
grep -q "gate_code_xyz" <<<"$out" || fail "the refusal doesn't name the unknown column: $out"
"${P[@]}" -d postgres -c "drop database cc_move_new2 with (force)"
echo "  unknown columns are refused by name"

sent=$(psql -X -At -d cc_move_new -c "select count(*) from public.invoices where sent_at is not null and sent_at = issued_at")
[[ "$sent" == "1" ]] || fail "the legacy sent invoice did not get its sent date"
echo "  legacy sent invoice counts as billed on its issue date"

# Chad signs in to the new project once (auth creates his user), then the owner link runs.
"${P[@]}" -d cc_move_new -c "insert into auth.users (id, email) values (gen_random_uuid(), 'chadwasham64@gmail.com')"
"${P[@]}" -d cc_move_new -f "$W/s/ops/0001_link_production_owner.sql" >/dev/null
role=$(psql -X -At -d cc_move_new -c "select m.role from public.memberships m join auth.users u on u.id = m.user_id where u.email = 'chadwasham64@gmail.com'")
[[ "$role" == "owner" ]] || fail "owner link did not work on the new project"
echo "  owner link works on the new project"

"${P[@]}" -d postgres -c "drop database cc_move_old with (force)" -c "drop database cc_move_new with (force)"
echo "MOVE CHECK PASSED"
