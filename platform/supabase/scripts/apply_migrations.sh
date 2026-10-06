#!/usr/bin/env bash
# Apply pending migrations to a real Supabase database, in order, each in its
# own transaction. Already-applied versions are skipped.
#
# Tracks versions in supabase_migrations.schema_migrations (the same table the
# Supabase CLI and dashboard use), so the history stays compatible.
#
# Usage: DB_URL=postgresql://... platform/supabase/scripts/apply_migrations.sh
# Never point this at production without owner approval.
set -euo pipefail
: "${DB_URL:?DB_URL is required}"
HERE="$(cd "$(dirname "$0")" && pwd)"
MIG="$HERE/../migrations"
PSQL=(psql "$DB_URL" -X -q -v ON_ERROR_STOP=1)

"${PSQL[@]}" -c "create schema if not exists supabase_migrations;
  create table if not exists supabase_migrations.schema_migrations (version text primary key, statements text[], name text);"

applied="$("${PSQL[@]}" -At -c "select version from supabase_migrations.schema_migrations")"
# Fingerprints of applied files (recorded since 2026-10-07). An applied migration
# must never change; a fix goes in a new migration.
hashes="$("${PSQL[@]}" -At -F' ' -c "select version, substring(statements[1] from 5) from supabase_migrations.schema_migrations where statements[1] like 'md5:%'")"
drift=0

for f in $(ls "$MIG"/*.sql | sort); do
  base="$(basename "$f" .sql)"; version="${base%%_*}"; name="${base#*_}"
  sum="$(md5sum "$f" | cut -d' ' -f1)"
  if grep -qx "$version" <<<"$applied"; then
    was="$(awk -v v="$version" '$1==v {print $2}' <<<"$hashes")"
    if [[ -n "$was" && "$was" != "$sum" ]]; then
      echo "::error title=migration-drift::$base was changed after it was applied. Put the change in a new migration."
      drift=1
    elif [[ -z "$was" ]]; then   # applied before fingerprints existed: record it now
      "${PSQL[@]}" -c "update supabase_migrations.schema_migrations set statements = array['md5:$sum'] where version = '$version' and statements is null"
    fi
    echo "  = $base (already applied)"; continue
  fi
  echo "  + $base"
  {
    cat "$f"
    printf "\ninsert into supabase_migrations.schema_migrations (version, name, statements) values ('%s', '%s', array['md5:%s']);\n" "$version" "$name" "$sum"
  } | "${PSQL[@]}" --single-transaction -f -
done
[[ $drift == 0 ]] || exit 1
echo "migrations up to date"
