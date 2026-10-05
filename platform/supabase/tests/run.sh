#!/usr/bin/env bash
# Database test runner.
#
# Builds two databases from the migrations and runs every test file against a
# fresh copy of each:
#   upgrade : simulated current production (baseline + existing policies + seed
#             data) upgraded by the migrations. Proves no data loss.
#   fresh   : empty project (staging/CI/new environment) built from migrations.
#
# Exit code is non-zero if any assertion fails. Tenant isolation failures block release.
#
# Uses PG* env vars if set (CI). Otherwise starts a throwaway local Postgres.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
MIG="$ROOT/migrations"
FIX="$HERE/fixtures"

if [[ -z "${PGHOST:-}" ]]; then
  BIN="$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -V | tail -1)"
  DATA="${PGDATA_DIR:-/var/tmp/cc-pgtest}"
  export PGHOST="$DATA" PGPORT="${PGPORT:-5499}" PGUSER=postgres
  if [[ ! -f "$DATA/data/PG_VERSION" ]]; then
    mkdir -p "$DATA"; chown postgres "$DATA" 2>/dev/null || true; chmod 700 "$DATA"
    su postgres -c "$BIN/initdb -D $DATA/data -A trust -U postgres" >/dev/null
  fi
  if ! su postgres -c "$BIN/pg_ctl -D $DATA/data status" >/dev/null 2>&1; then
    su postgres -c "$BIN/pg_ctl -D $DATA/data -o '-k $DATA -p $PGPORT -c listen_addresses=' -l $DATA/log start -w" >/dev/null
  fi
fi

export PGOPTIONS='-c client_min_messages=warning'
PSQL=(psql -X -q -v ON_ERROR_STOP=1)
# Files are copied somewhere the postgres user can read.
WORK="$(mktemp -d /var/tmp/cc-sql.XXXX)"; chmod 755 "$WORK"
cp -r "$ROOT" "$WORK/supabase"; chmod -R a+rX "$WORK"
trap 'rm -rf "$WORK"' EXIT
M="$WORK/supabase/migrations"; F="$WORK/supabase/tests/fixtures"; T="$WORK/supabase/tests"

build_template() { # $1 = scenario
  local db="cc_tpl_$1"
  "${PSQL[@]}" -d postgres -c "drop database if exists $db with (force)" -c "create database $db"
  "${PSQL[@]}" -d "$db" -f "$F/00_supabase_shim.sql" -f "$F/05_test_helpers.sql"
  local first=1
  for f in $(ls "$M"/*.sql | sort); do
    "${PSQL[@]}" -d "$db" -f "$f"
    if [[ $first == 1 && $1 == upgrade ]]; then
      "${PSQL[@]}" -d "$db" -f "$F/10_legacy_existing_state.sql"
      "${PSQL[@]}" -d "$db" -At -c "create table tests.before_counts as
        select 'clients' t, count(*) n from clients union all select 'jobs', count(*) from jobs
        union all select 'time_entries', count(*) from time_entries union all select 'invoices', count(*) from invoices
        union all select 'expenses', count(*) from expenses union all select 'memberships', count(*) from memberships
        union all select 'tenants', count(*) from tenants union all select 'profiles', count(*) from profiles"
    fi
    first=0
  done
}

total_fail=0
for scenario in upgrade fresh; do
  echo "== scenario: $scenario"
  build_template "$scenario"
  for tf in $(ls "$T"/*.sql "$T/$scenario"/*.sql 2>/dev/null | sort); do
    db="cc_t_$$"
    "${PSQL[@]}" -d postgres -c "drop database if exists $db with (force)" -c "create database $db template cc_tpl_$scenario"
    "${PSQL[@]}" -d "$db" -f "$F/20_test_world.sql"
    if ! out=$("${PSQL[@]}" -d "$db" -f "$tf" 2>&1); then
      echo "  ERROR in $(basename "$tf"): $out"; total_fail=$((total_fail+1))
    fi
    res=$("${PSQL[@]}" -d "$db" -At -F '|' -c "select ok, name, coalesce(detail,'') from tests.results order by id")
    pass=$(grep -c '^t|' <<<"$res" || true); fail=$(grep -c '^f|' <<<"$res" || true)
    printf "  %-34s %3d passed, %d failed\n" "$(basename "$tf")" "$pass" "$fail"
    grep '^f|' <<<"$res" | sed 's/^f|/    FAIL: /' || true
    total_fail=$((total_fail+fail))
    "${PSQL[@]}" -d postgres -c "drop database $db with (force)"
  done
done

if [[ $total_fail -gt 0 ]]; then echo "FAILED: $total_fail"; exit 1; fi
echo "ALL TESTS PASSED"
