# Prioritized Issues

P0 = do now · P1 = Phase 1 blocker · P2 = Phase 2 · P3 = later. "Owner" is who has to act.

## P0: Now

| ID | Issue | Owner | Status |
|---|---|---|---|
| P0-1 | Confirm `SECRET_KEY` is set on Render to a long random value (if not, admin sessions are forgeable) | Chad | Open |
| P0-2 | Lock `/gs-test`, `/gs-debug`, `/rebuild-totals` behind admin login; POST-only for writes | Claude | **Done in hotfix** |
| P0-3 | App refuses to start with default/missing `SECRET_KEY` | Claude | **Done in hotfix** |
| P0-4 | Rate-limit `/login` (Flask-Limiter already installed); constant-time password compare | Claude | **Done in hotfix** |
| P0-5 | Fix open redirect in `?next=` | Claude | **Done in hotfix** |
| P0-6 | `/health` no longer returns exception text | Claude | **Done in hotfix** |
| P0-7 | Check Render for a persistent disk + `DB_PATH`; if a disk exists, take `sqlite3 clock.db .dump` backup | Chad | Open |
| P0-8 | Export every Google Sheet tab to CSV (File → Download) as backup + migration source | Chad | Open |
| P0-9 | Run `scripts/supabase_inspect.sql`, share output for RLS review | Chad | Open |
| P0-10 | Link GitHub to Claude so work lands as PRs instead of downloads | Chad | Open |

## P1: Foundation

| ID | Issue |
|---|---|
| P1-1 | Monorepo scaffold (`apps/web`, `apps/mobile`, `packages/shared`, `supabase/`), CI |
| P1-2 | Baseline migration from existing Supabase schema |
| P1-3 | Review/fix RLS per audit §5.4 (recursion, UPDATE `WITH CHECK`, crew vs financial tables, membership escalation, `profiles` RLS, function grants) |
| P1-4 | **Tenant isolation test suite as CI release blocker** |
| P1-5 | Schema: `employees`, `crews`, `crew_members`, `properties`, `tenant_settings.timezone`, `audit_log`, `activity` |
| P1-6 | Audit-log triggers on time entries, memberships, invoices, payments, clients, jobs |
| P1-7 | Auth: OTP/magic link, invites, org switcher |
| P1-8 | Staging Supabase project (needs Chad's OK; handoff says no new projects without instruction) |
| P1-9 | Sentry + structured logging |

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
