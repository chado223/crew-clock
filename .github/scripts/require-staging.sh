#!/usr/bin/env bash
# Exits non-zero unless $DB_URL points at crew-clock-staging, checked exactly.
# Supabase URLs name the project either in the host (db.<ref>.supabase.co) or in
# the pooler user name (postgres.<ref>). Anything else is refused, including any
# production project listed in platform/production-projects.txt.
set -euo pipefail
STAGING=newivmnolbzhjypusmob
url="${DB_URL:?DB_URL is not set}"
rest="${url#*://}"; creds="${rest%%@*}"; hostport="${rest#*@}"; host="${hostport%%[:/]*}"; user="${creds%%:*}"
ref=""
if [[ "$host" =~ ^db\.([a-z0-9]{20})\.supabase\.co$ ]]; then ref="${BASH_REMATCH[1]}"; fi
if [[ "$user" =~ ^postgres\.([a-z0-9]{20})$ ]]; then ref="${BASH_REMATCH[1]}"; fi
here="$(cd "$(dirname "$0")/../.." && pwd)"
while read -r prod _; do
  [[ -z "$prod" || "$prod" == \#* ]] && continue
  if [[ "$ref" == "$prod" || "$url" == *"$prod"* ]]; then echo "::error::DB_URL points at PRODUCTION ($prod). Refusing."; exit 1; fi
done < "$here/platform/production-projects.txt"
if [[ "$ref" != "$STAGING" ]]; then echo "::error::DB_URL is not the crew-clock-staging project. Refusing."; exit 1; fi
echo "Target: crew-clock-staging"
