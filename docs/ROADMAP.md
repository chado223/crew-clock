# Roadmap: Crew Clock → Field-Service SaaS → App Store

Phases follow the handoff, with the CRM requirement folded in where its data is created. Each phase ships behind passing tests and leaves the product usable. The Flask app keeps running until Phase 2 cutover.

**Standing rules, every phase:** migrations for every schema change · tenant isolation tests green before merge · audit log on sensitive writes · no secrets in client bundles · small reviewable PRs · staging before production.

---

## Phase 0: Audit & Stabilization *(this PR)*

- [x] Repo, history, routes, deployment, security audit → `docs/ARCHITECTURE_AUDIT.md`
- [x] Edge-case test of hours logic (bugs B1–B4 documented)
- [x] Roadmap + prioritized issues
- [x] Read-only Supabase inspection script
- [x] Security hotfix for the live Flask app (branch `phase-0-audit`)
- [ ] Set `SECRET_KEY` on Render if unset, deploy hotfix (Claude via Render connector, with Chad's OK: production deploy)
- [ ] Confirm Render disk / `DB_PATH` (Claude, via Render connector); back up SQLite and Google Sheet before migration
- [ ] Review live Supabase schema/RLS (Claude, via Supabase connector; replaces the manual script)
- [x] Architecture approved by Chad 2026-10-05 (ADR 0001)

**Done when:** hotfix is live, backups exist (SQLite dump if a disk exists, plus Sheet CSV export), Supabase reviewed, architecture approved.

---

## Phase 1: Foundation

**Progress (2026-10-05, branch `phase-1-foundation`):** database foundation built and tested locally. Migrations: baseline, foundation (employees, pay rates, crews, properties, invitations, audit log, CRM activity, tenant-safe foreign keys, per-operation RLS), time clock (punch/break/correction functions, `timesheet`/`weekly_hours`), onboarding (create company, invites, roles), privileges. 779 checks pass across upgrade and fresh scenarios; mutation testing confirmed the suite fails when security rules are broken. **Not yet applied to Supabase**: waiting on the Supabase connection to diff against the live schema and set up staging. Web/mobile scaffolds wait on GitHub (CI builds, since this workspace can't install npm packages).

Monorepo scaffold: `apps/web` (Next.js), `apps/mobile` (Expo), `packages/shared`, `supabase/`.

- Supabase CLI linked to the **existing** project; current schema captured as migration `0000_baseline`
- Corrective migrations: `employees`, `crews`, `crew_members`, `properties`, `tenant_settings` (timezone, business hours), `audit_log`, indexes on `(tenant_id, …)` access paths
- RLS rewritten as separate SELECT/INSERT/UPDATE/DELETE policies; helpers `SECURITY DEFINER` with fixed `search_path`; financial tables owner/admin only
- **Tenant isolation test suite** (pgTAP or SQL tests via `supabase test db`): two tenants, three roles each; every table proves no cross-tenant read, insert, update, or delete. CI fails on any break.
- Auth: email OTP / magic link, invite flow (owner invites by email or phone → membership + employee link), organization switcher
- Generated DB types shared by web and mobile
- Environments: local, staging (separate Supabase project, **with approval**), production
- Error tracking (Sentry) + structured logging foundation
- GitHub Actions: lint, typecheck, unit tests, DB tests

**Done when:** a new company can sign up on staging, invite a user, and tests prove it can't see Chad's company.

---

## Phase 2: Time + Employees → **cutover from Flask**

- `clock_in()`, `clock_out()`, `start_break()`, `end_break()` RPCs: state machine enforced server-side (no IN→IN), idempotent by client event ID, tenant-timezone day boundaries, DST-correct (`timestamptz` throughout)
- Corrections: `correct_time_entry()`, owner/admin only, reason required, before/after in audit log, original never overwritten
- Forgotten clock-out detection (scheduled job), alert to owner + reminder to employee
- Weekly totals, overtime rules (configurable; default FLSA >40 h/week), labor cost from pay rate
- Payroll export (CSV; Gusto/QuickBooks formats later)
- **Expo crew app v1:** sign-in, one big CLOCK IN / OUT button, today's hours, offline queue with sync status, works with gloves (≥56 pt targets), high-contrast outdoor mode
- **Web:** live "who's clocked in," time cards, approve/correct, employee management (invite, role, pay rate, active)
- **Migration:** import history per audit §6, exception report reviewed by Chad, totals reconciled to the Sheet
- Optional: weekly Google Sheets export job (keeps the familiar report)
- **Cutover:** crew installs via TestFlight; Flask set read-only for 2 weeks, then retired

**Done when:** a full pay week runs on the new app, payroll totals match a manual check, and Flask isn't needed.

---

## Phase 3: Clients, Properties, Jobs + **CRM core**

- Clients (residential/commercial), multiple contacts, multiple properties (address, geocode, gate/access notes, lawn size, photos)
- **CRM:** customer status, tags, lead source, assigned owner, internal notes, preferred contact method
- **Leads + pipeline:** stages New → Contacted → Estimate Scheduled → Estimate Sent → Follow-Up → Won / Lost (with lost reason); kanban with drag-and-drop; pipeline value and conversion stats
- **Tasks/follow-ups:** due date, assignee, priority, linked customer/property, reminders
- **Customer timeline:** unified `activity` table every module writes to (call logged, note, job done, invoice sent, payment…), the single most important CRM screen
- Services catalog (mow, aeration, overseeding, mulch, leaves, …), recurring service plans per property
- Jobs/visits: recurrence rules → generated visits with `scheduled_date`, crew assignment, estimated duration
- Scheduling: day/week views, drag/drop reschedule, unscheduled queue
- Crew app: today's visits in order, property notes, navigate, start/finish visit (ties time to job), notes, before/after photos (private storage, signed URLs)

**Done when:** Chad runs a real week of CWLC routes from the app, and opening any customer shows their full history.

---

## Phase 4: Money

- Estimates: line items from service catalog, photos, terms, customer approval link → converts to job/service plan
- Invoices: line items, tax, discounts, PDF, send by email, due dates, statuses, partial payments, recurring billing per service plan
- Stripe (Connect, so each company gets paid into its own account): card/ACH, webhooks → payments, reconciliation job
- Expenses with receipts, linked to job/crew/equipment
- Job costing: labor (from time entries × rate) + materials + expenses vs revenue
- Owner financial dashboard: revenue, A/R, profit per job/client/crew, revenue per labor hour
- CRM automations: estimate not approved in N days → follow-up task; invoice overdue → reminder workflow (owner-approved templates)

---

## Phase 5: Field Intelligence

- Route optimization behind a provider adapter (evaluate Google Route Optimization, Mapbox, OR-Tools service); respects windows, durations, crew capacity; re-optimize on change
- Weather: per-tenant 5 AM local scheduled job, rain/severe thresholds, flags affected visits, suggests reschedule, owner approves, crews notified
- GPS at clock-in/out (only at the punch, not continuous tracking), optional geofence verification per company
- Push notifications (Expo Push → APNs/FCM) with per-user preferences: assigned, route changed, rain delay, forgot clock-out

---

## Phase 6: Customer Experience

- Customer portal (web, magic-link login): properties, service history, before/after photos, approve estimates, pay invoices, update card, request work, message company
- Review/referral requests after completed visits
- CRM: requests/complaints logged to timeline; repeated-complaint detection

---

## Phase 7: AI Operations

All customer-, money-, schedule- or employee-affecting actions are drafts until a human confirms.

- Owner morning briefing (today's work, weather, who's out, A/R, leads needing attention)
- Estimate drafting from photos + notes
- Customer history summary + recommended next action on the CRM record
- Upsell detection (e.g., mowing customers without fall aeration → seasonal opportunity list + drafted campaign)
- At-risk customer detection, follow-up message drafting, late-payment communications
- Profitability insights in plain language

---

## Phase 8: Commercial Release (web/SaaS)

- Self-serve signup → company creation → onboarding checklist (add crew, add first clients, import CSV)
- Subscription plans + trial (Stripe Billing on web); plan entitlements as feature flags, not hard-coded columns
- Data export, account deletion, company deletion, employee removal with retention rules
- Privacy policy, terms, DPA; monitoring, backups (PITR), disaster-recovery runbook, status page
- Load test: many tenants, concurrent clock-ins at 7 AM
- Support tooling: admin impersonation (audited, read-only by default)

---

## Phase 9: iOS / App Store Release (+ Google Play)

Follows the handoff's 28-step list. Key gates:
- **Fresh review of Apple's current rules** for B2B subscriptions, account deletion, Sign in with Apple, privacy nutrition labels, and location/camera purpose strings, documented before any purchase flow is built
- Crash reporting, privacy-respecting analytics, permission prompts only at point of need with clear explanations
- App icon, launch screen, screenshots, listing copy
- TestFlight closed beta with CWLC crew + 2–3 friendly companies → fix → App Review → release → close monitoring
- Android build from the same Expo codebase → Play internal testing → release

---

## Sequencing notes

- **Phase 2 before Phase 3** because it replaces the only thing the business uses today and proves the platform with real users.
- The CRM's **timeline/activity table is designed in Phase 1** even though screens arrive in Phase 3, so every module writes to it from day one.
- Weather and routing need properties with coordinates (Phase 3) before they're useful.
- App Store submission is last, but the crew app ships to TestFlight starting in Phase 2, so the native path gets exercised continuously.
