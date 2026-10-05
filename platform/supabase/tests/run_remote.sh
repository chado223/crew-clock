#!/usr/bin/env bash
# Run the database test suite against a real Supabase database (staging).
#
# Each test file runs inside ONE transaction that is always rolled back:
# test helpers, the two-company test world, the test itself, then results are
# read and everything is discarded. Staging keeps no test data.
#
# Usage: DB_URL=postgresql://... platform/supabase/tests/run_remote.sh
set -euo pipefail
: "${DB_URL:?DB_URL is required}"
HERE="$(cd "$(dirname "$0")" && pwd)"
F="$HERE/fixtures"
total_fail=0

for tf in $(ls "$HERE"/*.sql | sort); do
  out="$( {
    echo "\\set ON_ERROR_STOP 1"
    echo "begin;"
    echo "set local client_min_messages = warning;"
    cat "$F/05_test_helpers.sql"
    cat "$F/20_test_world.sql"
    cat "$tf"
    echo "\\pset tuples_only on"
    echo "\\pset format unaligned"
    echo "\\pset fieldsep '|'"
    echo "select '##', ok, name, coalesce(detail,'') from tests.results order by id;"
    echo "rollback;"
  } | psql "$DB_URL" -X -q 2>&1 )" || { echo "  ERROR in $(basename "$tf"):"; echo "$out" | tail -5; total_fail=$((total_fail+1)); continue; }
  res="$(grep '^##|' <<<"$out" | cut -d'|' -f2- || true)"
  pass=$(grep -c '^t|' <<<"$res" || true); fail=$(grep -c '^f|' <<<"$res" || true)
  printf "  %-34s %3d passed, %d failed\n" "$(basename "$tf")" "$pass" "$fail"
  grep '^f|' <<<"$res" | sed 's/^f|/    FAIL: /' || true
  if [[ $pass -eq 0 && $fail -eq 0 ]]; then echo "    no results; output:"; echo "$out" | tail -5; total_fail=$((total_fail+1)); fi
  total_fail=$((total_fail+fail))
done

if [[ $total_fail -gt 0 ]]; then echo "FAILED: $total_fail"; exit 1; fi
echo "ALL TESTS PASSED (remote)"
