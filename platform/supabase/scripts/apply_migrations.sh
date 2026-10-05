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

for f in $(ls "$MIG"/*.sql | sort); do
  base="$(basename "$f" .sql)"; version="${base%%_*}"; name="${base#*_}"
  if grep -qx "$version" <<<"$applied"; then
    echo "  = $base (already applied)"; continue
  fi
  echo "  + $base"
  {
    cat "$f"
    printf "\ninsert into supabase_migrations.schema_migrations (version, name) values ('%s', '%s');\n" "$version" "$name"
  } | "${PSQL[@]}" --single-transaction -f -
done
echo "migrations up to date"
