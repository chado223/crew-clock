# Branches, releases and rollback (DEV-2 / DEV-4)

**Adopted 2026-10-13.** Every change reaches staging and production this way. Anything that touches production, or costs money, still needs Chad's approval at the step marked **(approval)**.

## Branch map

| Branch | What it is | Who/what deploys from it | Rules |
|---|---|---|---|
| `main` | **The live Flask Crew Clock**, at `e9144e7` | Render service `crew-clock` auto-deploys on any commit except under `platform/**` | Locked (Chad's lock rule). **Nothing is merged here.** Docs or workflows merged into `main` would redeploy Flask. Changes only with a separately approved Flask decision (`LAUNCH_READINESS.md` DEV-3). |
| `platform-main` | **The platform's main line.** All new work lands here. Started at `da1cb32`, which production runs. | Nothing deploys from it directly | Protected: pull requests only, required checks `apps` and `db-tests`, no force-push, no deletion. Feature branches `feat/<plan ID>-short-name` merge in by PR. |
| `release/vYYYY.MM.N` | **Release markers**: one branch per release, cut from `platform-main` | Nothing directly | Never changed after creation (protected pattern `release/*`). Branches are used instead of git tags because tags can't be pushed from Claude's session. |
| `web-staging` | What the **staging** web app runs | Render `crew-clock-web-staging`, manual deploy only | Moved only to a release branch's commit. |
| `web-release` | What the **production** web app runs | Render `crew-clock-web`, manual deploy only | Moved only to a release that passed staging **(approval)**. |
| `production-ops` | Approval-gated production database runs (`.github/production/request.txt`) | GitHub environment `production` (Chad approves each run) | Fast-forwarded to the release before a production migration run **(approval)**, so the run uses that release's migrations and scripts. |
| `phase-*` | History of the 22 original pull requests | Nothing | Kept read-only for history; see `docs/PR_RECONCILIATION.md`. |

## Normal change

1. Create a branch `feat/<ID>-name` from `platform-main` (IDs from `docs/LAUNCH_READINESS.md`).
2. Open a pull request into `platform-main`. CI runs `apps` (typecheck, unit tests, web build) and `db-tests` (every migration plus 1,399 assertions, fresh and upgrade).
3. Merge when both are green. Chad reviews anything user-facing or security-relevant.

## Release (staging, then production)

1. **Cut the release:** create `release/v2026.11.0` from the `platform-main` commit. The release notes (in the pull request or `docs/releases/`) list plan IDs and test evidence.
2. **Staging database** (if migrations changed): run the `Staging database` workflow manually on the release branch. It refuses anything but staging.
3. **Staging web:** move `web-staging` to the release commit, then Render, `crew-clock-web-staging`, **Manual Deploy**.
4. **Staging checks:**
   - Read-only live check (`Web live check`, target = staging) and staging read-only check.
   - The release's manual test steps from `docs/TEST_PLAN.md`.
   - Record the results in the release notes.
5. **(approval)** Chad approves the release for production.
6. **Production database** (if migrations changed): fast-forward `production-ops` to the release commit, then set `action=migrate` in `request.txt`. Chad approves the run; it applies migrations and runs the preflight.
   - Migrations are **expand-then-contract**: add first, remove only in a later release. So the previous app version keeps working against the new database, and the app can be rolled back without touching the data.
7. **Production web:** move `web-release` to the release commit, then Render, `crew-clock-web`, **Manual Deploy**. Then run the read-only live check (target = production) and production preflight.
8. **Phone apps** (once they exist): EAS staging builds point only at staging; production builds only at production (enforced in the app). Store releases use staged rollouts.

## Rollback

| What | How | Time | Data impact |
|---|---|---|---|
| Production web app | Render, `crew-clock-web`, Deploys, **Rollback** to the previous deploy (Render keeps earlier builds). Then move `web-release` back to the previous release commit so git matches. | ~2 min | None. Expand-then-contract keeps the old version compatible. |
| Staging web app | Same, on `crew-clock-web-staging` | ~2 min | None |
| Production database change | Preferred: a new forward-fix migration through the normal release. Last resort: restore per the disaster-recovery runbook (Supabase daily backup, or the encrypted off-site copy). **(approval)** | Forward fix: hours. Restore: up to 4 h. | Restore loses data since the backup; forward fix loses none |
| Phone app | Halt the staged rollout in App Store Connect / Play Console, or publish the previous build | Minutes to hours | None |
| Flask clock | Not part of platform releases. Its own rollback is Render's Rollback on `crew-clock`. | ~2 min | Flask's local data is lost on every restart anyway; the Google Sheet is its record |

## Hotfix

1. Branch from the live release (`release/…`).
2. Open a pull request into `platform-main`.
3. Cut a patch release (`release/v2026.11.1`).
4. Follow the same staging, approval and production steps. No direct edits to `web-release`.

## Branch protection settings (set by Chad in GitHub; free)

**Status, verified through the GitHub API on 2026-10-13:**
- `platform-main` requires a pull request (0 approvals) and checks `apps` and `db-tests`; force pushes and deletion are blocked.
- `web-release` and `web-staging` block force pushes and deletion.
- `main` is locked, with admins included.
- The `release/*` rule can only be confirmed once the first release branch exists.
- **Two gaps:**
  - "Do not allow bypassing the above settings" is off, so admins (Chad, and Claude's sessions using his access) can push straight past the rules.
  - "Require branches to be up to date before merging" is off.

GitHub doesn't let changes to these settings come from Claude's session, so they're set by hand: **Settings, Branches, Add classic branch protection rule**.

| Branch | Settings |
|---|---|
| `platform-main` | Require a pull request before merging (0 approvals, since Chad is the only reviewer and authors can't approve their own); require status checks `apps` and `db-tests`, and branches up to date; do not allow force pushes or deletions |
| `web-release` | Do not allow force pushes or deletions |
| `web-staging` | Do not allow force pushes or deletions |
| `release/*` | Do not allow force pushes or deletions (pattern rule) |
| `main` | Already locked; leave as is |

Optional, separate decision: making `platform-main` the repository's *default* branch. That makes new pull requests target it automatically. It also starts the two daily scheduled jobs from it (the staging backup rehearsal and the staging weather check), which today don't run at all because `main` has no workflows. Render is unaffected: it tracks branches by name.
