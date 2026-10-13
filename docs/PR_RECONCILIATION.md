# Reconciliation of the 22 stacked pull requests (DEV-1)

**Reviewed 2026-10-13. Result: nothing is lost.** Every change in PRs #1–#22 is in `platform-main`, which was created at `da1cb32`, the same commit as `production-ops` and the code the production web app was built from (`web-release` = `24e6ff7`, an ancestor of it).

## How this was checked

1. **Ancestry:** for every branch in the repository, `git merge-base --is-ancestor <branch> da1cb32`.
   - 21 of the 22 PR heads, and `main`, `web-release` and `web-staging`, are fully contained.
2. **The one exception, `phase-20-audit-fixes` (PR #22):** it has 2 commits outside that ancestry.
   - `bf87b32`: phone sign-in accepts codes up to 10 digits.
   - `da72ae8`: the migration that locks down the unused `scenarios` table.
   - Both were carried into `production-ops` as separate commits. The files they touch are byte-identical in `platform-main` (`git diff` = 0 lines).
3. **Whole-tree comparison** of the stack tip (`phase-20-audit-fixes`) against `platform-main`:
   - 24 files differ, all additions or the production-preparation edits made after the stack (database guard, production build settings, preflight modes, CI placeholder, docs).
   - No stack file was removed.
4. **Tests at `platform-main`:**
   - Database suite run locally on 2026-10-13: 1,399 assertions per scenario, fresh and upgrade, all passing.
   - CI "Apps" was green at `df435da` and "Database tests" on 2026-10-11. Since then only docs, workflows and `platform/deploy/*.yaml` changed. The pull request that brings this record into `platform-main` re-runs both.
5. **Content review:** the code in these PRs was reviewed in the two readiness audits (PR #14 and PR #22 are their fix sets) and the 2026-10-13 commercial readiness assessment. This reconciliation verifies containment; it doesn't re-review every line.

## Per-PR record

| PR | Branch → base | Title | Commits | Files | +/− lines | Head in production? | Verdict |
|---|---|---|---|---|---|---|---|
| #1 | `phase-0-audit` → `main` | Phase 0: architecture audit + security hotfix for live Crew Clock | 2 | 6 | +626/−19 | Yes | Superseded by platform-main; keep open: its Flask hotfix still needs a separate deploy decision |
| #2 | `phase-1-foundation` → `phase-0-audit` | Phase 1: database foundation (tenancy, RLS, time clock, onboarding) + tests | 6 | 26 | +2579/−20 | Yes | Superseded by platform-main |
| #3 | `phase-1-apps` → `phase-1-foundation` | Phase 1: web dashboard (Next.js) + crew app (Expo) scaffolds | 5 | 44 | +1761/−2 | Yes | Superseded by platform-main |
| #4 | `phase-2-time-crm` → `phase-1-apps` | Time cards with audited corrections + CRM core (customers, properties, history) | 2 | 13 | +976/−16 | Yes | Superseded by platform-main |
| #5 | `phase-3-field-ops` → `phase-2-time-crm` | Field operations core: jobs, recurring visits, scheduling board, crew app workflow | 2 | 24 | +1902/−264 | Yes | Superseded by platform-main |
| #6 | `phase-4-costing` → `phase-3-field-ops` | Job costing and profitability (labor per visit, drive time allocation) | 2 | 8 | +504/−6 | Yes | Superseded by platform-main |
| #7 | `phase-4-money` → `phase-4-costing` | Money: estimates to jobs, visits to invoices, payments | 3 | 19 | +1306/−7 | Yes | Superseded by platform-main |
| #8 | `phase-6-customer-portal` → `phase-4-money` | Customer portal with database-enforced customer isolation | 2 | 19 | +1355/−4 | Yes | Superseded by platform-main |
| #9 | `phase-7-weather` → `phase-6-customer-portal` | Weather automation (National Weather Service) | 4 | 22 | +1732/−2 | Yes | Superseded by platform-main |
| #10 | `phase-8-routes` → `phase-7-weather` | Routes: ordered stops, free planner behind a provider interface, map handoff | 2 | 14 | +829/−8 | Yes | Superseded by platform-main |
| #11 | `phase-9-communications` → `phase-8-routes` | Communications: test-mode outbox, live locks, templates, preferences, workflows, dispatcher, screens | 4 | 27 | +1859/−16 | Yes | Superseded by platform-main |
| #12 | `phase-10-owner-dashboard` → `phase-9-communications` | Owner command center: Today attention list + Business health | 3 | 10 | +1163/−10 | Yes | Superseded by platform-main |
| #13 | `phase-11-photos` → `phase-10-owner-dashboard` | Visit photos, portal service history, online-payments model | 4 | 17 | +798/−4 | Yes | Superseded by platform-main |
| #14 | `phase-12-hardening` → `phase-11-photos` | Readiness-audit fixes: crew least privilege, customer consistency, money controls, safe redirects | 1 | 19 | +897/−24 | Yes | Superseded by platform-main |
| #15 | `phase-13-editing` → `phase-12-hardening` | Editing customers, properties and recurring jobs; schedule actions; automatic fill | 2 | 10 | +539/−9 | Yes | Superseded by platform-main |
| #16 | `phase-14-import-export` → `phase-13-editing` | CSV import with preview; CSV exports (customers, invoices, payments, timesheet, payroll) | 1 | 13 | +730/−0 | Yes | Superseded by platform-main |
| #17 | `phase-15-billing` → `phase-14-import-export` | Billing for a residential route; expenses; company settings | 1 | 11 | +446/−3 | Yes | Superseded by platform-main |
| #18 | `phase-16-team-field` → `phase-15-billing` | Team admin; crew breaks and 'couldn't do this stop'; phone works without signal | 1 | 10 | +531/−47 | Yes | Superseded by platform-main |
| #19 | `phase-17-ops` → `phase-16-team-field` | Ops: tested emergency revert, lockfile + npm ci, staging backups, audit reporting | 6 | 9 | +10875/−15 | Yes | Superseded by platform-main |
| #20 | `phase-18-readiness` → `phase-17-ops` | Readiness: expenses in profitability, services, editing, offline queue, recurring tests | 2 | 18 | +794/−88 | Yes | Superseded by platform-main |
| #21 | `phase-19-production-prep` → `phase-18-readiness` | Production prep + readiness polish (nothing enabled) | 2 | 31 | +1052/−18 | Yes | Superseded by platform-main |
| #22 | `phase-20-audit-fixes` → `phase-19-production-prep` | Second readiness audit: fixes | 9 | 67 | +2662/−192 | No (2 commits not in ancestry) | Content verified identical in platform-main |

PR #19's line count is mostly `package-lock.json`.

## Disposition (with Chad's approval of Task 1, 2026-10-13)

- **PRs #2–#22:** close as superseded, each with a comment linking here.
  - Their branches are kept (not deleted), so history and links stay intact.
  - Closing a PR doesn't touch its branch, so dependent PRs are unaffected.
- **PR #1** (`phase-0-audit` → `main`) **stays open as a draft.** It contains the Flask security hotfix, which would redeploy the live Flask clock if merged into `main`. That needs its own approval (`LAUNCH_READINESS.md` DEV-3).
