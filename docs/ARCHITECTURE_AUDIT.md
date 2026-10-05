# Crew Clock: Architecture Audit (Phase 0)

**Repo:** github.com/chado223/crew-clock, branch `main` @ `e9144e7`
**Audited:** 2026-10-05
**Scope:** full repository, full git history (13 commits), hours-calculation logic, deployment config, security. Supabase reviewed against the handoff description only (see §5.4).

---

## TL;DR

Crew Clock is a ~460-line single-file Flask app. One shared web page lets anyone type a name and press IN or OUT. Punches go to a SQLite file and are mirrored to Google Sheets, which builds the weekly totals.

It does its one job on a good day, but it isn't a foundation for the SaaS:

- **No employee identity.** Anyone with the URL can clock anyone in or out under any name.
- **Hours can be wrong, and two places disagree.** The web page and the Google Sheet use different pairing rules, so a forgotten clock-out produces different totals in each (32 h vs 8 h in one test). Daylight-saving nights are off by an hour. Name typos split one person into two.
- **Data durability is unknown.** If Render isn't mounting a persistent disk for `clock.db`, every deploy has wiped the database, and the Google Sheet is the only real history.
- **Five unauthenticated endpoints** can write to the Google Sheet or trigger rebuilds.

None of the app's code carries forward into the target product (Supabase + iOS + web admin). The data does, and so does the knowledge of how the crew uses it. **Recommendation:** keep the Flask app running, with a small security hotfix, until the new crew app replaces it. Build the new platform in parallel, migrate the history once, then retire Flask. Per the handoff's "don't rewrite working systems" rule, this is a replacement with a cutover plan, not a rewrite of something worth keeping.

---

## 1. Current architecture

```
Phone/browser ──POST /clock──► Flask (gunicorn, Render)
                                  │
                                  ├─► SQLite clock.db   table: entries(id, crew TEXT, action IN|OUT, ts TEXT)
                                  │
                                  └─► Google Sheets (synchronous, in the request)
                                        ├─ "Week YYYY-WW"        one row per punch
                                        ├─ "Totals Week YYYY-WW" rebuilt on every OUT
                                        └─ "All Weeks Summary"   cleared + rewritten on every OUT
```

| Area | What's there |
|---|---|
| Framework | Flask 3.0, server-rendered Jinja templates, gunicorn (`Procfile: web: gunicorn App:app`) |
| Language/runtime | Python; no `runtime.txt`, so Render's default Python is used |
| Database | SQLite via raw `sqlite3`, one table `entries`. Timestamps are **naive local-time strings** (`"2026-10-05 07:00:00"`) |
| ORM | `models.py` defines a SQLAlchemy `ClockLog` model. **Never imported.** Dead code |
| Helpers | `utils.py` (`weekly_buckets`, `to_local`). **Never imported.** Dead code |
| Auth | Single shared `ADMIN_PASSWORD` → Flask session flag. Crew have no auth at all |
| Google Sheets | `gspread` service account loaded from Render Secret File `/etc/secrets/service_account.json`; `SHEET_ID` env var |
| Supabase | **No integration in the code.** Nothing references Supabase or Postgres |
| Render config | `Procfile` only. No `render.yaml`, no disk declared in repo |
| Tests | None |
| Migrations | None (`CREATE TABLE IF NOT EXISTS` at import time) |
| PWA/mobile | Viewport meta tag only. No manifest, service worker, offline, or install support |
| CI | None |

### Routes

| Route | Method | Auth | What it does |
|---|---|---|---|
| `/` | GET | none | Clock form + last 50 punches + hours |
| `/clock` | POST | none | Records IN/OUT for any typed name; writes SQLite then Sheets |
| `/health`, `/healthz` | GET | none | Reads 1 row; returns `{"ok":true}` or the exception text |
| `/gs-test` | GET | **none** | **Writes** a TEST/PING row to the live sheet |
| `/gs-debug` | GET | **none** | **Writes** a DEBUG row; returns sheet title + all tab names |
| `/rebuild-totals` | GET/POST | **none** | Rebuilds totals tabs (clears + rewrites summary) |
| `/login` | GET/POST | none | Shared admin password |
| `/logout` | GET | none | Clears session |
| `/admin` | GET/POST | admin | Reads Sheets totals; template **only shows the week title** |

### Environment variables

| Read by code | In `.env.example`? |
|---|---|
| `DB_PATH` (default `clock.db`) | ✗ |
| `ADMIN_PASSWORD` | ✓ (`letmein` placeholder) |
| `SECRET_KEY` (default `dev-insecure`) | ✓ (`change-me` placeholder) |
| `TZ` (default `America/New_York`) | ✗ (example says `TIMEZONE`, which nothing reads) |
| `SHEET_ID` | ✗ |
| `PORT` (dev only) | ✗ |

`.env.example` also lists `DATABASE_URL` and `HOURLY_RATE`, which **nothing reads**. It describes an app that was planned, not the one that runs.

### Git state

Single branch `main`, 13 commits, clean. Files once committed but now removed: `templates/*.html.txt`. **Secret scan of full history: clean.** Only placeholders appear, and no service-account JSON or keys were ever committed.

---

## 2. What works

- Clocking IN/OUT by name from any browser, including phones (the form wraps).
- Recent-punches table, color-coded IN/OUT.
- Same-day, single-shift hours (one IN, one OUT) calculate correctly; verified in test.
- Overnight shifts are paired correctly (credited to the start date).
- Weekly tabs and totals in Google Sheets, **when** Sheets credentials are valid.
- Jinja autoescaping protects the punch table from script injection through the name field.
- `/health` gives Render a liveness check.

## 3. What is broken or wrong

Verified by extracting the app's own hour-calculation functions and running them against edge cases (harness: stdlib only, run 2026-10-05).

| # | Scenario | Web page shows | Google Sheet records | Correct |
|---|---|---|---|---|
| B1 | Forgot to clock out yesterday (IN Oct 4 07:00, IN Oct 5 07:00, OUT Oct 5 15:00) | **32.0 h on Oct 4** | 8.0 h | Flag it; 8 h today plus an open shift needing a manager fix |
| B2 | Double IN same day (IN 07:00, IN 12:00, OUT 15:00) | 8.0 h | **3.0 h** | Flag it; reject the duplicate IN |
| B3 | `Jessie` clocks in, `jessie` clocks out | 0 h | OUT with no hours | Same person, 8 h |
| B4 | DST fall-back night (Nov 1 2026, 00:30 → 03:30) | **3.0 h** | 3.0 h | **4.0 h** of real time worked |

Root causes:
- **Two different pairing algorithms.** The page pairs each OUT with the *oldest* open IN (`deque.popleft`, App.py:80), while the Sheets writer pairs it with the *newest* (`stack.pop`, App.py:140). Payroll reads the Sheet, so the owner sees one number and pays another.
- **Free-text names are the identity.** Typos, nicknames and capitalization create separate people.
- **Naive local timestamps.** The stored time doesn't say which timezone it's in, so DST transitions lose or gain an hour.
- **No state machine.** Nothing prevents IN→IN or OUT→OUT, and nothing detects a forgotten clock-out.

Other defects:
- **Admin dashboard is empty.** `/admin` computes totals, summary and punches, but `admin.html` renders only the week title.
- **Google Sheets is in the request path.** Each OUT makes roughly 8–12 Sheets API calls, including reading and rewriting the entire "All Weeks Summary." Clock-out is slow, and failures are only `print`ed, so the crew sees success either way. Google throttles at about 60 read requests per minute per user, and a busy morning or a growing summary tab can hit that. **This is the most likely reason Sheets "stopped working"**, along with an expired or removed Render Secret File or the sheet being unshared from the service account. `/gs-debug` will show which (after the hotfix it requires admin login).
- **The `"All Weeks Summary"` rewrite isn't safe under concurrency.** Two clock-outs at once can each clear and rewrite the tab, and one write can lose the other's rows.
- **Hours-on-page scans the whole table** on every page load (`calculate_daily_hours` reads all rows). That's fine at hundreds of rows but gets slower forever.
- **Rate limiting is installed but never used.** `Flask-Limiter` is in `requirements.txt` but not wired up.

## 4. Security risks

Ordered by severity.

| Sev | Issue | Where | Impact |
|---|---|---|---|
| **Critical** | Crew have no authentication; anyone with the URL can clock anyone in/out | `/clock` | Time fraud; impossible to trust payroll data |
| **Critical** | `SECRET_KEY` silently defaults to `dev-insecure` | App.py:25 | If unset on Render, anyone can forge an admin session cookie |
| **High** | `/gs-test`, `/gs-debug`, `/rebuild-totals` are unauthenticated and write to the live sheet; `/gs-debug` reveals sheet title and tab names | App.py:313–369 | Spam/vandalism of payroll sheet; information disclosure. Also a GET that changes data |
| **High** | Admin login has no rate limit or lockout; password compare isn't constant-time | `/login` | Online password guessing |
| **Medium** | Open redirect: `?next=` accepts any URL | App.py:389 | Phishing via a trusted-looking link |
| **Medium** | No CSRF protection on any POST | all forms | A malicious page can submit punches or admin actions |
| **Medium** | Service account requests full `drive` scope | App.py:91 | Credential leak exposes all Drive files shared with it, not just the sheet |
| **Low** | `/health` returns raw exception text | App.py:307 | Leaks internals |
| **Low** | `debug=True` in `__main__` | App.py:466 | Only when run directly, not under gunicorn; still worth removing |

No committed secrets. No SQL injection (all queries parameterized). XSS is mitigated by Jinja autoescape.

## 5. Scalability and data risks

### 5.1 Data durability (check this first)
SQLite lives at `DB_PATH`, default `clock.db` in the app directory. **On Render, the app filesystem is wiped on every deploy and restart unless a persistent disk is attached** and `DB_PATH` points into it. With 13 deploy commits, unless a disk is mounted, `clock.db` has been reset each time, and **the Google Sheet is the only complete history.**

→ **Action for Chad:** In the Render dashboard, check the service for a Disk and check whether `DB_PATH` is set to a path on it. That answer decides where the migration pulls history from (§6).

### 5.2 Architecture limits
- SQLite + local disk = **one instance only.** No horizontal scaling, and Render disks also block zero-downtime deploys.
- No tenant concept anywhere: one company, one shared page.
- Synchronous third-party calls in the request path.
- Whole-table scans for hours.
- These are all expected for an internal tool. They aren't fixable by tweaking, and the target platform addresses each one by design.

### 5.3 Supabase schema (from handoff description) — design gaps to fix in Phase 1
Not verified against the live database; see 5.4.

| Gap | Why it matters | Fix |
|---|---|---|
| `time_entries.user_id` → `auth.users` | Crew may not have logins yet (and seasonal workers churn); history from SQLite has names, not users | Add `employees` table (tenant-scoped person record, optional `user_id` link). Time entries reference `employee_id` |
| `jobs.crew_id` references nothing | No `crews` table | Add `crews` + `crew_members` |
| No `properties` | Handoff requires client → many properties | Add `properties`; jobs reference `property_id` |
| `jobs.schedule` JSONB | Can't index or query "jobs on Oct 7 for crew 2"; recurring rules mixed with occurrences | `job_templates`/recurrence rule + `visits` (one row per scheduled occurrence with `scheduled_date`) |
| Invoices have only `total` | No line items, tax, partial payments | `invoice_lines`, `payments`; money as `numeric(12,2)` or integer cents |
| `expenses` has no `job_id`/`created_by`/receipt | Job costing and profitability impossible | Add links + `receipt_path` |
| No audit log | Required early by handoff | `audit_log` table + triggers on sensitive tables |
| No offline idempotency | Duplicate punches during sync | Client-generated `id uuid` + unique constraint; or `client_event_id` |
| No punch source data | Disputes, GPS later | `source`, `device_id`, `lat/lng` (nullable), `edited_by`, `edit_reason` |
| `profiles`, `tenants` lack fields | Timezone needed for 5 AM weather jobs and correct day boundaries | `tenants.timezone`, settings |

### 5.4 Supabase verification: blocked, needs Chad
I don't have access to the Supabase project, and the handoff correctly says not to create another one. The policies and helper functions **must be reviewed before anything depends on them**. Common failure modes to look for:

1. **Recursive RLS.** If `in_tenant()` reads `memberships` and the `memberships` policy calls `in_tenant()`, queries fail with "infinite recursion detected." Helpers must be `SECURITY DEFINER` with `SET search_path = ''` (or `public`) and fully qualified table names.
2. **Missing `WITH CHECK` on UPDATE.** Without it, a user can move a row *into* another tenant by changing `tenant_id`.
3. **Crew can read financial tables.** If `invoices`/`expenses` SELECT uses `in_tenant()` rather than `is_admin_or_owner()`, every crew member sees company revenue.
4. **Privilege escalation through `memberships`.** If admins can UPDATE memberships, they can promote themselves to owner unless the policy forbids changing `role` to `owner`.
5. **Tables without RLS.** `profiles` isn't in the RLS list in the handoff. With RLS off, any logged-in user from any company can read every profile.
6. **Helper functions callable by `anon`.** They should be executable by `authenticated` only.

→ `scripts/supabase_inspect.sql` is a read-only script. Run it in the Supabase SQL editor and paste the output back; I'll review it line by line.

## 6. Migration requirements (SQLite/Sheets → Supabase)

**Source of truth for history:** decided by §5.1.
- **Disk attached:** download `clock.db` from the Render shell (`sqlite3 clock.db .dump > backup.sql`) and use it as primary, with Sheets as cross-check.
- **No disk:** export every `Week YYYY-WW` tab to CSV and use them as primary; `clock.db` holds only punches since the last deploy.

**Mapping:**

| Old | New |
|---|---|
| distinct `crew` names (case-folded, trimmed) | `employees` rows for tenant `055bdb3c…`. **Chad confirms the name→person list** (e.g. "jessie", "Jessie", "Jess" → one employee) |
| `entries` IN/OUT pairs | `time_entries(employee_id, clock_in, clock_out)`, timestamps converted from America/New_York to `timestamptz` with DST handled |
| unpaired IN | `time_entries` with `clock_out NULL`, flagged `needs_review` |
| unpaired OUT | `migration_exceptions` report, not imported silently |
| every imported row | `source='legacy_sqlite'` or `'legacy_sheets'`, `legacy_id` kept for traceability |

**Pairing rule for migration:** use the **Sheet's** rule (newest open IN), since that's what payroll was paid on. Anything ambiguous (double INs, orphan OUTs) goes on the exception report for Chad, not into the data silently. *Do not silently manipulate employee time.*

**Validation:** row counts in vs out, per-employee per-week hour totals compared against the Sheet's `Totals Week` tabs, and a diff report. **SQLite and the Sheet stay untouched until Chad signs off.**

## 7. Recommended architecture

Built for the stated end state: multi-tenant SaaS → iOS App Store → Android, with the web as the owner/office console.

```
 iOS / Android crew + owner app          Web admin / office console
  Expo (React Native, TypeScript)          Next.js (TypeScript)
  offline queue (SQLite on device)              │
            │                                   │
            └───────────────┬───────────────────┘
                            ▼
                Supabase (one project, existing)
   ┌───────────────────────────────────────────────────────────┐
   │ Auth (email + magic link / OTP; invites)                  │
   │ Postgres + RLS  ← source of truth, tenant isolation       │
   │ RPC functions (clock_in, clock_out, correct_time_entry…)  │
   │ Storage (private buckets, signed URLs) for photos         │
   │ Edge Functions: Stripe webhooks, email, push, AI          │
   │ Scheduled jobs: pg_cron + queue table (weather 5 AM/tenant)│
   └───────────────────────────────────────────────────────────┘
          │                     │                    │
       Stripe          Weather/routing APIs     Expo Push / APNs
                      (behind provider adapters)
```

**Why this stack:**
- **Expo, not a PWA wrapper.** The goal is a native-feeling App Store app with offline clock-in, push, camera and location. Expo builds real iOS/Android binaries from one TypeScript codebase, ships to TestFlight with EAS, and avoids the "web page inside an app" look the handoff rules out.
- **TypeScript everywhere.** Mobile, web and server functions share types and validation schemas (`packages/shared`) generated from the database. One language for one developer and one agent.
- **Business rules live in the database layer** (Postgres functions + RLS), not in either client. The phone and the web call the same `clock_in()` RPC, so duplicate-punch rules, audit logging and tenant checks can't drift between platforms. This is the direct fix for bug class B1–B2.
- **Monorepo** (`apps/mobile`, `apps/web`, `packages/shared`, `supabase/migrations`, `supabase/tests`): one PR can change schema, types and both apps together.
- **Background jobs are server-side only.** `pg_cron` enqueues per-tenant jobs into a `jobs_queue` table, and a worker (Edge Function) processes them idempotently with retries and logging. Nothing depends on a phone being awake, which handles the iOS background limits.
- **Google Sheets becomes an optional export** (scheduled job writes weekly totals), never in the clock path.
- **Avoid lock-in where it's cheap.** It's standard Postgres with plain SQL migrations. Routing, weather, SMS and payments sit behind adapter interfaces.

**What I'd decide later, not now:** routing provider, weather provider, background-job runner if `pg_cron` + queue outgrows itself (Inngest/Trigger.dev are drop-in options), iOS subscription billing approach (needs a fresh read of Apple's rules at release — see roadmap Phase 8/9).

**Alternatives considered:**
- *Extend the Flask app.* Rejected: no auth model, no tenancy, no API for mobile, and the logic worth keeping is about 40 lines. Every piece would be replaced anyway, so extending only delays that.
- *Keep Python backend (FastAPI) + Expo.* Viable, but it adds a server to host and scale, a second language, and duplicate authorization next to RLS. Supabase RPC + Edge Functions covers the same needs with less to run.
- *PWA first, wrap later.* Rejected for the crew app because of App Store quality expectations, iOS PWA push and background limits, and offline reliability. The web admin stays web.

## 8. Prioritized implementation plan

Full phase detail is in `docs/ROADMAP.md`; the ticket-level list is in `docs/ISSUES.md`.

1. **Now (no approval needed, low risk):** security hotfix to the live Flask app: lock the debug endpoints behind admin, fail if `SECRET_KEY` is missing, rate-limit login, fix the open redirect. *(Prepared on branch `phase-0-audit`.)*
2. **Chad to provide:** Render disk status, the Supabase inspection output, a Google Sheet CSV export, and the crew name list.
3. **Phase 1 Foundation** (after audit approval): monorepo scaffold, Supabase migrations for corrected schema, RLS rewrite, **tenant isolation test suite as a CI release blocker**, audit log, auth + invites.
4. **Phase 2 Time + Employees:** `clock_in/clock_out` RPCs with a state machine, Expo crew app (one-tap clock, offline queue), web time cards, corrections with audit trail, overtime, payroll export, forgotten-clock-out alerts. **Then migrate history and cut the crew over from Flask.**
5. Phases 3–9 per roadmap.
