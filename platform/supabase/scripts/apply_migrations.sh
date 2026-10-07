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

# Pass 1, before applying anything: every applied migration must still exist
# unchanged, and nothing new may sort before the newest applied version.
# (Only versions that match a file count; a dashboard-tool record carries its own timestamp.)
newest_applied="$(for v in $applied; do ls "$MIG/${v}_"*.sql >/dev/null 2>&1 && echo "$v"; done | sort | tail -1)"
names="$("${PSQL[@]}" -At -F' ' -c "select version, coalesce(name, '') from supabase_migrations.schema_migrations")"
for v in $applied; do
  # A record made by the Supabase dashboard tool names the file it came from instead of using its version.
  n="$(awk -v v="$v" '$1==v {print $2}' <<<"$names")"
  if ! ls "$MIG/${v}_"*.sql >/dev/null 2>&1 && [[ ! -f "$MIG/$n.sql" ]]; then
    echo "::error title=migration-drift::applied migration $v ($n) has no file any more (deleted or renamed)."; drift=1
  fi
done
for f in $(ls "$MIG"/*.sql | sort); do
  base="$(basename "$f" .sql)"; version="${base%%_*}"; sum="$(md5sum "$f" | cut -d' ' -f1)"
  if grep -qx "$version" <<<"$applied"; then
    was="$(awk -v v="$version" '$1==v {print $2}' <<<"$hashes")"
    if [[ -n "$was" && "$was" != "$sum" ]]; then
      echo "::error title=migration-drift::$base was changed after it was applied. Put the change in a new migration."; drift=1
    fi
  elif [[ -n "$newest_applied" && "$version" < "$newest_applied" ]]; then
    echo "::error title=migration-order::$base is older than already-applied $newest_applied. Give it a newer version."; drift=1
  fi
done
[[ $drift == 0 ]] || { echo "Nothing applied."; exit 1; }

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
