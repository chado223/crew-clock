# Staging (test) environment: prepared, not active (2026-10-11)

**Read-only check 2026-10-13 (`staging-readonly-check.yml`): passed.** Details under "Staging state" below.

Chad chose to test with made-up data in a separate copy (Option A in `TEST_PLAN.md`), so the production company stays clean.

## Pieces

| | Production (real) | Staging (test) |
|---|---|---|
| Database | `crew-clock-prod` (`kymnehbnmqvpzwtxtizm`), CWLC org, Pro | `crew-clock-staging` (`newivmnolbzhjypusmob`), old free org |
| Web app | `crew-clock-web`, $7, branch `web-release` | `crew-clock-web-staging`, **free**, branch `web-staging` |
| Address | crew-clock-web.onrender.com | crew-clock-web-staging.onrender.com |
| Sign-up | Off | On (test accounts allowed) |
| Sign-in email | Resend, noreply@chadwashamlawns.com | Supabase's built-in sender. It only delivers to members of the Supabase org (Chad) and only a few per hour. Enough for Round 1. Crew and customer logins (rounds 5–6) need a staging email setup first, a separate approval. |
| Config file | `platform/deploy/render-web.yaml` | `platform/deploy/render-web-staging.yaml` |

## Why staging can't touch production

1. **A different database in a different Supabase organization**, with its own address and keys.
   - The staging service is given only staging's address and public key.
   - No production value, password or key is anywhere in its settings.
2. **A built-in guard.** With `NEXT_PUBLIC_APP_ENV=staging`, the app refuses to run unless the database is `crew-clock-staging`, and the production app refuses anything but `crew-clock-prod`. The guard has unit tests, and it was proven on the live production service on 2026-10-11.
3. **Separate Render services and branches.**
   - Deploying staging can't redeploy production (`web-release`) or the Flask clock (`main`).
   - Both new services deploy only by hand.
4. **Separate sign-ins.** Accounts on staging don't exist on production, and the reverse.
5. **Production tooling refuses staging and the reverse:** `require-production.sh`, `require-staging.sh` and `production-projects.txt`.

## Approvals needed (in order)

1. **Wake the staging database (free).**
   - Free Supabase projects pause after 7 days without activity. Staging's last activity was 2026-10-07, so it may still be awake.
   - Chad switches to the old organization in Supabase, opens `crew-clock-staging`, and either confirms it's active or clicks **Restore project**.
   - Then a read-only check (GitHub, staging secret only) shows its migration level and what test data it already has.
2. **Create the free Render service** from `render-web-staging.yaml`.
   - It costs $0. Render's free tier gives 750 instance-hours a month per workspace, and the sleeping Flask service uses some of them.
   - First deploy, then a read-only live check like production's.

Later, only when needed (rounds 5–7): an email sender for staging, a phone test build, and a staging weather job.

## Keeping staging alive

Free projects pause after a week idle. Waking one is a free click and nothing is lost. Projects paused for more than 90 days can't be restored with a click. Testing every few days keeps it awake.

## Staging state (read-only check, 2026-10-13)

Chad confirmed the project is **Active**, so no restore was needed.

The check ran entirely in read-only transactions, using the staging secret behind the staging-only guard:
- **Database:** PostgreSQL 17.11, 18 MB.
- **Migrations:** all 36 repo migrations are present with matching fingerprints, newest `20261007001600`, the same as production.
  - There is one extra history row, `20261005215701`: the original baseline, applied early through Supabase's tool and recorded again under the repo's version. Cosmetic only.
- **Flags:** `production=false`, `live_messaging=false`, `online_payments=false`.
- **Data:** empty. 0 companies, 0 accounts, 0 rows in every table, 0 messages, 0 stored files.
- **Isolation posture:** row security is on for every table, and 41 company tables were found.
  - A live probe as real accounts wasn't possible (there are no accounts yet). It will be once test accounts exist.
  - The full isolation suite passes in CI on every database change. Its last run against staging (2026-10-07, every test rolled back) also passed.
- **One difference from production:** anonymous visitors still have grants on `scenarios`, the other app's empty leftover table.
  - Production revoked these (migration `20261007001600` only acts when the production flag is on).
  - Row security means anonymous visitors still see nothing there, and the table is empty.
  - A small follow-up migration could revoke them everywhere, with approval.
