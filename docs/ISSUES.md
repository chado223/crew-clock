# Prioritized Issues

P0 = do now · P1 = Phase 1 blocker · P2 = Phase 2 · P3 = later. "Owner" is who has to act.

## P0: Now

| ID | Issue | Owner | Status |
|---|---|---|---|
| P0-1 | Confirm `SECRET_KEY` is set on Render to a long random value (if not, admin sessions are forgeable) | Chad (connector can't read env vars) | Minimal steps will come with PR #1 deploy approval |
| P0-2 | Lock `/gs-test`, `/gs-debug`, `/rebuild-totals` behind admin login; POST-only for writes | Claude | **Done in hotfix** |
| P0-3 | App refuses to start with default/missing `SECRET_KEY` | Claude | **Done in hotfix** |
| P0-4 | Rate-limit `/login` (Flask-Limiter already installed); constant-time password compare | Claude | **Done in hotfix** |
| P0-5 | Fix open redirect in `?next=` | Claude | **Done in hotfix** |
| P0-6 | `/health` no longer returns exception text | Claude | **Done in hotfix** |
| P0-7 | Check Render for a persistent disk + `DB_PATH` | Claude | **Done**: free plan, no disk; SQLite is wiped on every sleep/deploy; Google Sheet is the only history |
| P0-8 | Back up every Google Sheet tab before migration (migration source) | Claude (Google Drive connector, needed by Phase 2) | Open |
| P0-9 | Review live Supabase schema/RLS | Claude | **Done**: docs/PRODUCTION_RECONCILIATION.md |
| P0-10 | Connect GitHub so work lands as PRs and CI runs | Chad | **Done** |
| P0-11 | Link owner login to Chad Washam Lawncare in production | Chad confirms email, then approves | Waiting on Chad |

## P1: Foundation

| ID | Issue | Status |
|---|---|---|
| P1-1 | Monorepo scaffold (`apps/web`, `apps/mobile`, `packages/shared`, `supabase/`), CI | **Done**: web + mobile + shared, CI builds |
| P1-2 | Baseline migration from existing Supabase schema | **Done**: mirrors production exactly |
| P1-3 | Review/fix RLS per audit §5.4 (recursion, UPDATE `WITH CHECK`, crew vs financial tables, membership escalation, `profiles` RLS, function grants) | Done; verified on staging |
| P1-4 | **Tenant isolation test suite as CI release blocker** | Done: runs locally, in CI (PG17) and on staging |
| P1-5 | Schema: `employees`, `crews`, `crew_members`, `properties`, `tenant_settings.timezone`, `audit_log`, `activity` | Done; on staging |
| P1-6 | Audit-log triggers on time entries, memberships, invoices, payments, clients, jobs | Done; on staging |
| P1-7 | Auth: email code, invites, org switcher | Done (web + mobile; web accepts the typed code on any device) |
| P1-8 | Staging Supabase project (needs Chad's OK; handoff says no new projects without instruction) | **Done**: crew-clock-staging (free); migrations + tests run from GitHub |
| P1-9 | Sentry + structured logging | Open |

## P2: Time + cutover

| ID | Issue |
|---|---|
| P2-1 | `clock_in/out` + break RPCs: state machine, idempotent, `timestamptz`, DST-correct (fixes B1–B4) |
| P2-2 | Corrections with reason + audit trail |
| P2-3 | Forgotten clock-out detection job + notifications |
| P2-4 | Weekly totals, overtime, labor cost, payroll CSV |
| P2-5 | Expo crew app v1 (one-tap clock, offline queue) → TestFlight |
| P2-6 | Web: live board, time cards, employee management |
| P2-7 | History migration + exception report + reconciliation vs Sheet totals |
| P2-8 | Optional Sheets weekly export job |
| P2-9 | Cutover; Flask read-only 2 weeks; retire |

## Known bugs in live Flask app (fixed by replacement, not patched)

Patching these in Flask would change historical payroll numbers mid-stream. They get fixed by design in P2-1, and the migration exception report surfaces past occurrences.

| ID | Bug |
|---|---|
| B1 | Forgotten clock-out → page shows inflated hours (32 h test case), Sheet shows different number |
| B2 | Page and Sheet pair IN/OUT differently (oldest vs newest open IN) |
| B3 | Name case/typos split one employee into several |
| B4 | DST fall-back night undercounts by 1 h |
| B5 | `/admin` template shows only the week title; data is computed but never displayed |
| B6 | Sheets calls in request path; failures invisible to user; likely cause of "Sheets stopped working" |
| B7 | Concurrent clock-outs can lose rows in "All Weeks Summary" |

## Cleanup (low priority, Flask only)

- Remove dead `models.py`, `utils.py` (or leave; Flask is being retired)
- `.env.example` lists variables nothing reads (`DATABASE_URL`, `HOURLY_RATE`, `TIMEZONE`) and omits ones it does (`DB_PATH`, `TZ`, `SHEET_ID`). **Fixed in hotfix.**
- Narrow Sheets scope from `drive` to `spreadsheets` only (test on Render first; `open_by_key` works with spreadsheets scope)

## Next up (not blocked on Chad)
- Generated database types in CI (replace hand-written row types and casts)
- Business health: compute job costing once per page load (today it runs twice)
- Expo SDK upgrade before the first store build: npm audit's high findings are all Expo/React Native build tooling (metro, jest, cli), fixed in newer SDKs; nothing in the web runtime
- Playwright smoke tests once a hosted staging web exists
- Google Sheets hours export (needs a Google service account: owner setup)

- Error reporting (Sentry free tier or similar) on web + mobile before TestFlight (needs an account: owner OK)
- App Review demo sign-in (a review account that doesn't need an inbox)
- "Close my company" flow for a sole owner deleting their account (Apple guideline 5.1.1(v))
- Per-email throttle on web code sign-in (all checks come from the server's IP)
- Supabase redirect URL allow-list kept exact (no wildcards) at cutover

## Owner decisions pending
- Protect `main` (required review, no direct pushes) and mark PR #1 draft until the Flask deploy is approved: today one Merge click redeploys the live clock
- Support email and legal sign-off for /privacy and /terms (drafts in place)
- Apple Developer ($99/yr) and Google Play ($25 once) accounts; EAS project link
- Email provider (e.g. Postmark/Resend/SES) and SMS provider (e.g. Twilio) + turning on live messaging (`private.platform_flags.live_messaging`)
- Paid road routing (Google Route Optimization / Mapbox) if straight-line ordering isn't good enough
- Hosting for web + scheduler; Supabase Pro; Apple Developer account
- Production migrations + owner link script (`platform/supabase/ops/0001_link_production_owner.sql`)
