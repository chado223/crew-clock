#!/usr/bin/env bash
# Exits non-zero unless $DB_URL is exactly the dedicated production project.
# Refuses staging, the old shared project, and anything else.
set -euo pipefail
PROD=kymnehbnmqvpzwtxtizm
url="${DB_URL:?DB_URL is not set}"
rest="${url#*://}"; creds="${rest%%@*}"; hostport="${rest#*@}"; host="${hostport%%[:/]*}"; user="${creds%%:*}"
ref=""
if [[ "$host" =~ ^db\.([a-z0-9]{20})\.supabase\.co$ ]]; then ref="${BASH_REMATCH[1]}"; fi
if [[ "$user" =~ ^postgres\.([a-z0-9]{20})$ ]]; then ref="${BASH_REMATCH[1]}"; fi
case "$url" in *iwowjrnrbjiydckhjsfi*|*newivmnolbzhjypusmob*) echo "::error::Not the dedicated production project. Refusing."; exit 1;; esac
if [[ "$ref" != "$PROD" ]]; then echo "::error::DB_URL is not crew-clock-prod ($PROD). Refusing."; exit 1; fi
echo "Target: crew-clock-prod ($PROD)"
