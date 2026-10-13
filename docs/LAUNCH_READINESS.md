# Crew Clock: Commercial V1 Launch Readiness Plan

**Official plan, adopted by Chad Washam on 2026-10-13.** This file is the single source of truth for launch readiness, and progress is reported against it.

- **Last full status review:** 2026-10-13.
- **Change control:** this file changes only through commits on the platform line, with each change logged in the Change log at the bottom. Status changes cite evidence: a test run, a check, or a beta report.
- **Other docs:** `docs/ISSUES.md` holds defects, `docs/TEST_PLAN.md` holds manual test rounds, and the 2026-10-13 readiness assessment (Claude doc) holds the reasoning behind this plan.

---

## 1. Rules

### Status values (only **Done** counts toward launch)

| Status | Meaning |
|---|---|
| Not started | No code or setup exists. |
| Partial | Some code exists, but the requirement isn't met even on paper. |
| Built | Code meant to meet the requirement exists and its automated tests pass. **Not proof that it works.** |
| Verified (staging) | Passed every acceptance check in staging, on real devices and browsers where relevant. |
| Verified (beta) | Passed its real-world acceptance in daily use by beta companies. |
| Done | Verified (beta) where real-world testing applies, or Verified (staging) where it doesn't. Shipped to production with Chad's approval, and its evidence linked in the item. |

**A feature is never marked Done because its code exists or its database tests pass.** Each item's definition of done (DoD) lists the end-to-end and real-world checks it needs.

### Classes

- **V1-required:** required before the full commercial V1 launch. Every requirement Chad listed on 2026-10-13 is in this class, and none were dropped.
- **Beta-blocker:** must also be Done before the private beta starts, a subset of V1-required.
- **Enhancement:** valuable, but not required for V1.

### Flags (Chad acts or approves before work proceeds)

| Flag | Meaning |
|---|---|
| **A** | Needs Chad's explicit approval (production change, deploy, merge, enabling a capability) |
| **ACCT** | Needs a new account (Chad creates it; credentials never pass through chat) |
| **$** | Paid service or fee |
| **L** | Legal, tax or compliance review by a qualified professional |
| **REAL** | Needs real-world testing with people outside the dev process |

### Standing guardrails (unchanged)

- The Flask Crew Clock (Render service `crew-clock`, branch `main`) stays running and untouched until a separately approved retirement. The release strategy is in section 6.
- Production and staging stay separate. Nothing is deployed, merged, enabled or paid for without Chad's approval.
- Live messaging and online payments stay off in production until their items are Done and Chad approves turning them on.

---

## 2. Requirements checklist

Code paths are relative to `platform/` unless they start with `.github/` or `docs/`.

### 2.1 Mobile and field operations

| ID | Requirement | Class | Status | Code location | Depends on | Flags | Definition of done |
|---|---|---|---|---|---|---|---|
| MOB-1 | Working iOS and Android apps, installed and tested on physical devices | V1, Beta-blocker | Partial: 8 screens written, never built | `apps/mobile/` (Expo SDK 54) | DEV-9 (Expo upgrade), PRD-3 (app ids), MOB-0 | ACCT, $ (Apple $99/yr, Google $25), REAL | Release builds install from TestFlight / Play internal testing on at least 2 iPhones and 2 Android phones (one Android at least 4 years old). Sign-in, clock, breaks, visit flow and photos pass the device test script in staging. Then 3 crew members use it daily for 10 working days with no crash and no lost punch (checked against server records). |
| MOB-0 *(added)* | Mobile build pipeline: EAS project, staging and production build profiles, versioning, crash reporting | V1, Beta-blocker | Partial: `eas.json` profiles exist; no EAS project | `apps/mobile/eas.json`, `app.json` | DEV-9, PRD-3 | ACCT (Expo, free) | One command builds staging and production apps. The build number increments automatically. Crashes reach the error tracker (REL-1). A staging build cannot reach production (guard tested on device). |
| MOB-2 | GPS clock-in/out with consent and privacy controls | V1 | Not started (location deliberately blocked in `app.json`) | `apps/mobile/app/(tabs)/index.tsx`; new location columns on `time_entries` (migration) | MOB-1, PRD-6 (privacy policy) | L (employee-monitoring notice), REAL | Location is captured only at clock in/out and break events, never continuously. A consent screen explains what's collected and why, and clocking in still works if the person declines; the punch is marked "no location". Accuracy in metres is stored, and the owner sees it on time cards. A retention rule is applied (PRD-7). Real-world: 3 crews, 10 days, every punch shows location or a clear reason why not. |
| MOB-3 | Optional geofencing for jobsite verification, with accuracy handling and exceptions | V1 | Not started | Properties already have coordinates (`weather.sql` geocoding); new geofence settings per company/property | MOB-2 | REAL | Off by default per company. When on, a clock-in or visit start outside the radius (default 150 m, adjustable) is allowed but flagged. A low-accuracy fix (over 100 m) shows "location uncertain", never "outside". The owner sees an exceptions list and can approve with a reason (audited). Real-world: flags reviewed on 3 beta companies, with a false-flag rate under 5%. |
| MOB-4 | Push notifications | V1 | Not started | New: device tokens table, Expo push sender in the job runner (REL-2) | MOB-0, REL-2 | ACCT (Apple push keys via EAS) | Crew get pushes for schedule changes, new assignments and messages; owners for missed clock-outs and problem reports. Per-user opt-out. Pushes go only to members of the right company (isolation test). Delivered on both platforms in the device test. Real-world: beta crews confirm receipt over 2 weeks. |
| MOB-5 | Crew messaging | V1 | Not started | New tables and screens (web + mobile) | MOB-4 | REAL | Owner-to-crew and crew-to-office threads per company, with photo attachments. Readable only within the company (isolation tests). Unread badges, and a push on new messages. Messages kept per the retention rule. Real-world: used by 2 beta companies for 2 weeks without misdelivery. |
| MOB-6 | Reliable offline clock-in/out, breaks, visit updates and photo uploads | V1, Beta-blocker | Built (punches and visit updates queue offline; photo upload is online only) | `packages/shared/src/queue.ts` (unit-tested), `apps/mobile/lib/` | MOB-1 | REAL | In airplane mode a crew member can clock in/out, take breaks, start/complete visits and take photos. Everything syncs in order within 60 s of reconnecting, with no duplicates (server idempotency) and the original times kept. Photos survive an app restart while offline. Problems show to the user and the office. Field test: 3 days in low-signal areas; server records match the phones exactly. |
| MOB-7 | Proper app-store releases | V1 | Not started | Store listings, privacy labels, screenshots, review notes | MOB-1…MOB-6, PRD-3, PRD-6, PRD-8 | ACCT, $, A | Approved and publicly downloadable on the App Store and Google Play under the final brand and ids. Privacy labels match actual data use (including location). A review demo account exists. Account deletion works from the app (already built, to be re-verified). Google's 14-day closed test completed if it's a personal account. |
| MOB-8 *(added)* | Forced-update and version-compatibility policy | V1 | Not started | App version check against the server | MOB-0 | — | An outdated app that the API no longer supports shows "Update required" instead of failing silently. Tested by raising the minimum version in staging. |

### 2.2 Business and payments

| ID | Requirement | Class | Status | Code location | Depends on | Flags | Definition of done |
|---|---|---|---|---|---|---|---|
| BIZ-1 | SaaS subscription billing: plans, trials, upgrades, downgrades, cancellations, failed payments | V1 | Not started (`tenants.plan` is only a label) | New billing module, Stripe Billing, webhook handler | PRD-0 (business entity), BIZ-0 (pricing), REL-2 | ACCT (Stripe), $ (fees), A, L (sales tax) | In Stripe test mode, every path passes end-to-end: trial → paid; upgrade/downgrade with proration; cancel at period end; reactivate; card declined → retries → dunning emails → read-only lockout after grace → recovery. Plan limits are enforced in the database (tests). Webhooks verified and idempotent. A billing page shows plan, invoices and payment method. Then 3 beta companies converted to paid in live mode with correct invoices and tax. |
| BIZ-0 *(added)* | Pricing and packaging decided (plans, limits, trial length) | V1, Beta-blocker | Not started | `docs/` | Interviews (section 5) | A | A written pricing page with plan limits, approved by Chad and tested with at least 5 owners. |
| BIZ-2 | Secure customer invoice payments by card and ACH | V1 | Partial: data model only (`online_payments.sql`, 21 assertions); portal says "coming soon" | `apps/web/app/portal/invoices/`, `supabase/migrations/*online_payments.sql` | PRD-0, BIZ-3, Stripe Connect onboarding per company | ACCT, $, A, L | Each company completes Stripe Connect onboarding. Customers pay by card and ACH from the emailed invoice and the portal, with card data never touching our servers (Stripe-hosted fields). Payments post to invoices automatically. Refunds, partial payments, ACH returns and disputes are handled and audited. Tested in Stripe test mode for every path, then 10+ real payments across 3 beta companies reconciled to Stripe payouts to the cent. |
| BIZ-3 | Actual delivery of estimates, invoices, reminders and invitations | V1, Beta-blocker | Built: outbox, templates, dispatcher (72 assertions + 8 unit tests); nothing runs | `packages/messaging/`, `supabase/migrations/*communications.sql` | REL-2 (runner), PRD-3 (sender domain), DEV-10 (email deliverability) | A, $ (Resend Pro over 3,000/mo) | Messages are delivered within 5 minutes by the job runner. Bounces and complaints are recorded and suppress future sends. Each message has a clear sender name and reply-to (the company's, per the email-separation rule). Every send is logged in the customer timeline. Invoices arrive as a link plus a PDF. Real-world: 3 beta companies send real estimates and invoices for 2 weeks, with no undelivered message unexplained. |
| BIZ-4 | SMS messaging with consent, opt-out and compliance | V1 | Partial: dispatcher has an SMS channel type, but there's no provider, consent capture or registration | `packages/messaging/`, `contact_preferences` | BIZ-3, PRD-0, 10DLC registration per company | ACCT (Twilio or similar), $, L (TCPA), A | A consent record (when, how, wording) is required before any SMS. STOP/HELP keywords work and are honored immediately. Quiet hours are respected. Each company is registered for A2P 10DLC before it can send. Opt-outs sync to the database. Tested in staging with real phones; then 2 beta companies send reminders for 2 weeks with opt-outs honored. |
| BIZ-5 | Functional customer portal | V1 | Built (10 pages, 58 isolation assertions); untested by real customers | `apps/web/app/portal/` | BIZ-3 (invites delivered), AUTH (customer sign-up), BIZ-2 for payments | REAL | A customer receives an invite, signs in, and sees only their own properties, visits, invoices, estimates and photos. They accept an estimate, request service, update contact details and pay an invoice (with BIZ-2). Works on a phone. Real-world: 10+ real customers of beta companies use it; no wrong-customer data, ever. |
| BIZ-6 | Accurate job costing, time records and financial reporting | V1 | Built (costing 32 assertions, time 40, money 49) | `supabase/migrations/*job_costing.sql`, `*time_clock.sql`, `*money.sql`; `apps/web/app/(app)/profit`, `insights`, `time` | MOB-1 (real time data), REAL | REAL | **Time:** weekly hours and overtime match an independent hand calculation for 2 pay periods at 3 beta companies, including a daylight-saving weekend; zero unexplained differences. **Money:** invoice, payment and receivables totals reconcile to the cent with Stripe and bank deposits for 1 month. **Costing:** owners confirm cost per visit is within their own estimate, or the difference is explained. |
| BIZ-7 *(added)* | Accounting export / QuickBooks | V1 | Not started | — | BIZ-6 | ACCT (Intuit developer), A | At minimum a QuickBooks-ready export of invoices, payments and payroll hours that an accountant imports without edits; ideally a direct QuickBooks Online sync. Verified by one beta company's bookkeeper. |
| BIZ-8 *(added)* | Sales tax on subscriptions and on customer invoices | V1 | Partial: invoices support a tax rate | `tenants.default_tax_rate`; billing (BIZ-1) | BIZ-1, PRD-0 | L (accountant) | An accountant confirms the obligations (Tennessee taxes remotely accessed software). Subscription tax is calculated and collected (e.g. Stripe Tax). Invoice tax rules are confirmed for lawn services in the beta companies' states. |

### 2.3 Reliability and security

| ID | Requirement | Class | Status | Code location | Depends on | Flags | Definition of done |
|---|---|---|---|---|---|---|---|
| REL-1 | Error monitoring and uptime alerts | V1, Beta-blocker | Not started (`ISSUES.md` P1-9) | Web, mobile, job runner | — | ACCT (free tiers), A | Errors from web server, browser, mobile and jobs reach one tracker with release version and company id (no personal data). Uptime checks on production web, the database API and the job runner every 1–5 minutes. Chad gets an alert by email/SMS within 5 minutes of an outage (tested by a deliberate staging outage). |
| REL-2 | Scheduled background jobs: weather, reminders, recurring visits, message delivery | V1, Beta-blocker | Partial: CLIs exist (`packages/messaging`, `packages/weather`); visits generated on page load; nothing scheduled in production | New job runner; `generate_visits`, `queue_*_reminders`, weather runner | REL-1 | A | Each job runs on schedule in staging for 14 days with a recorded run history and alert on failure. Every job is idempotent (a re-run doesn't duplicate). The forgotten-clock-out check alerts owners. Then production, with messaging still in test mode until BIZ-3 is approved. |
| REL-3 | Security review of all elevated database functions, with regression checks | V1, Beta-blocker | Partial: 83 functions; covered by isolation tests but not reviewed one by one; no automatic guard check | `supabase/migrations/`, `supabase/tests/` | — | — | A written review of all 83 functions (who may call it, what it checks, verdict). Any fixes go in migrations with tests. A CI test fails if any elevated function lacks an approved authorization check, or if a new one appears unreviewed. Supabase advisor shows no unexpected findings. |
| REL-4 | Automated browser, API and mobile testing | V1 | Partial: database 1,399 assertions × 2 scenarios; 40 unit tests; no browser, API or mobile tests | New `apps/web/e2e/` (Playwright), API tests, mobile end-to-end (Maestro or Detox) | Staging (live), MOB-0 | — | Browser: sign-in, setup, customer → job → visit → invoice → payment, time correction, portal, on desktop and phone sizes, run in CI against a disposable environment on every change. API: every exposed function tested for allowed and denied callers. Mobile: clock in/out, offline sync and photo flow automated on an emulator in CI. Releases blocked on failure. |
| REL-5 | Recurring backups and tested disaster recovery | V1, Beta-blocker | Partial: Supabase daily backups (7 days); one-time encrypted off-site copy with tested restore; nothing recurring for the new production database | `.github/workflows/`, private `crew-clock-backups` | — | A, $ (PITR $100/mo, decide by 100 companies) | Encrypted off-site backup of production nightly, kept 30 days, verified by restore-and-compare weekly in CI. A written disaster-recovery runbook (targets: lose at most 24 h of data, back within 4 h). A full restore drill from scratch in staging, timed and passed. Photos backed up too. |
| REL-6 | Database pagination and performance testing | V1 | Not started (56 web reads without row limits) | `apps/web/app/**` | REL-4 | — | Every list is paginated or bounded. A load test with a synthetic company of 2,000 customers, 50 employees and 2 years of visits keeps every page under 2 s at the 95th percentile on the production instance sizes. Index review done; no sequential scans on hot queries. |
| REL-7 | Authentication security, account recovery and rate-limit handling | V1, Beta-blocker | Partial: email codes only; production sign-up off; no email-change or recovery flow; shared 100/hr email limit; generic errors (T-2) | `apps/web/app/login`, `apps/mobile/app/login.tsx`, Supabase Auth settings | REL-1, PRD-3 (sender domain) | A, $ (possible Supabase add-ons) | Invite-only sign-up for crew and customers; owner sign-up gated by billing. Recovery: change email with confirmation on both addresses; owner transfer; support-assisted recovery runbook. Rate limits: per-email throttles, clear "too many attempts" messages, and a project email limit sized for 3× peak sign-ins (load-tested). Optional second factor for owners. All admin accounts (GitHub, Supabase, Render, Stripe, Apple, Google, Resend, GoDaddy) have 2FA (checklist confirmed). |
| REL-8 | Resolve critical and high-risk dependency vulnerabilities | V1, Beta-blocker | Not started (23 high, all in Expo/RN build tooling) | `package-lock.json`, `apps/mobile` | DEV-9 | — | npm audit shows 0 critical and 0 high in shipped code, or each remaining one documented as build-only with a dated review. Automated dependency updates (Dependabot) on, with CI. |
| REL-9 *(added)* | Status page and incident response | V1 | Not started | `docs/runbooks/` | REL-1 | ACCT (free tier) | Public status page. A written incident runbook (detect, assess, communicate, fix, review). One practiced drill. Customers notified within 30 minutes of a major outage. |
| REL-10 *(added)* | Independent security review before launch | V1 | Not started | — | REL-3, REL-7 | $, A | An outside reviewer (or a structured self-assessment against OWASP ASVS level 1 if budget won't allow) tests tenant isolation, auth and payments; every high finding fixed and retested. |
| REL-11 *(added)* | Make the repository private | V1 | Not started (public today) | GitHub settings | DEV-1 | A, $ (maybe GitHub Actions minutes) | The repository is private. CI minutes are budgeted. No secrets in history (scan run). Today the whole product, schema and security design are public. |
| REL-12 *(added)* | Capacity and connection limits | V1 | Not started | Supabase compute, pooling; Render instance | REL-6 | $ | Documented upgrade triggers (CPU, memory, connections, email limits) with alerts at 70%. |

### 2.4 Professional development and releases

| ID | Requirement | Class | Status | Code location | Depends on | Flags | Definition of done |
|---|---|---|---|---|---|---|---|
| DEV-1 | Review and reconcile the 22 stacked pull requests | V1, Beta-blocker | Not started (PRs #1–#22 open; #1 draft) | GitHub | — | A | Every PR reviewed with a written summary. The production-equivalent tip (`production-ops`) lands on the new protected platform line through one reviewed baseline PR. The 22 PRs are closed as superseded with links, except #1's Flask hotfix, which waits for a separate approval. Nothing lost (diff check), and the production web build is unchanged. |
| DEV-2 | Maintainable Git branching and release strategy | V1, Beta-blocker | Not started (section 6 is the proposal) | `docs/RELEASES.md` (new) | DEV-1 | A | The strategy in section 6 is written up, adopted, and used for 3 consecutive releases. |
| DEV-3 | Protect the original Flask Crew Clock until a separately approved retirement | V1 | Done for now: `main` locked; Flask deploys only from `main`; Render ignores `platform/**` | Render `crew-clock`, `main` | — | A (for retirement) | Flask keeps serving with no unapproved change until its retirement item is approved. A test confirms platform releases never trigger a Flask deploy. |
| DEV-4 | Enforce CI checks, staging validation, release approvals and rollback | V1, Beta-blocker | Partial: CI runs but isn't required; production database changes are approval-gated; web deploys are manual; rollback documented only in part | `.github/workflows/`, branch protection | DEV-2 | A | The platform line requires passing CI and Chad's approval. Every release goes to staging first with a signed-off checklist. Production deploys need approval. Rollback rehearsed for the web app (redeploy previous tag), the mobile app (previous build or update) and the database (forward fix or restore). |
| DEV-5 | Separate production and staging environments | V1 | Verified (staging) on 2026-10-13: separate projects, services and branches; guards tested | `platform/deploy/*.yaml`, `packages/shared/src/projects.ts` | — | — | Already true; kept true. Staging must also keep its own email sender (DEV-10) and stay awake during test cycles. |
| DEV-9 *(added)* | Upgrade Expo / React Native to a supported release | V1, Beta-blocker | Not started (SDK 54) | `apps/mobile/` | — | — | On a current SDK; typecheck, build and device test pass; REL-8 findings cleared. |
| DEV-10 *(added)* | Email deliverability setup | V1, Beta-blocker | Partial: Resend domain verified (DKIM); DMARC unknown; staging has no sender | DNS at GoDaddy; Resend | PRD-3 | ACCT/$ maybe | SPF, DKIM and DMARC pass for the final sending domain. Bounce and complaint webhooks are handled. A seed test lands in the inbox, not spam, at Gmail, Outlook and Yahoo. Staging has its own sender. |
| DEV-11 *(added)* | Generated database types | V1 | Not started | Web and mobile | DEV-1 | — | Types generated from the schema in CI; a schema change that breaks a screen fails the build. |

### 2.5 Product and business readiness

| ID | Requirement | Class | Status | Code location | Depends on | Flags | Definition of done |
|---|---|---|---|---|---|---|---|
| PRD-1 | Professional onboarding and employee invitations | V1, Beta-blocker | Partial: company setup asks for a name only; invites are created but not delivered | `apps/web/app/onboarding`, `team` | BIZ-3, REL-7 | REAL | A guided setup checklist (company details, services and prices, crew invites, customer import, first job). Invites delivered by email (and SMS once BIZ-4 is Done), accepted on a phone into the app. Real-world: 3 new owners finish setup without help in under 45 minutes (timed). |
| PRD-2 | Mobile-friendly office application and clear error handling | V1 | Partial: 12 of 17 stylesheets have phone rules; no loading screens; generic errors (T-2) | `apps/web/app/**` | REL-4 | REAL | Every office page usable on a 375 px phone (browser tests at that size). Every action shows progress and a specific, human error. Real-world: owners run a day from a phone in the beta. |
| PRD-3 | Finalized branding, domain and permanent mobile app identifiers | V1, Beta-blocker | Not started (app named "Crew", id `com.cwlc.crew`) | `apps/mobile/app.json`, web copy, email sender | PRD-0 | A, $ (domain), L (trademark search) | Name chosen with a trademark search. Domain registered. Bundle and package ids set permanently before the first store submission. Logo and icons. All "CWLC" defaults replaced. |
| PRD-0 *(added)* | Business entity, bank and ownership of accounts | V1, Beta-blocker | Unknown | — | — | L, $, A | A legal entity owns the product, the Stripe account, the Apple/Google developer accounts (Apple organization accounts need a D-U-N-S number) and the domain. Separate business bank account. Ownership of all accounts documented. |
| PRD-4 | Privacy policy, terms, account deletion and data-retention rules | V1, Beta-blocker | Partial: drafts marked "Draft"; account deletion built (14 assertions); no retention rules | `apps/web/app/privacy`, `terms`, `components/legal.tsx` | PRD-0, MOB-2 (location), BIZ-4 (SMS) | L, A | An attorney has reviewed the privacy policy, terms, data processing terms for business customers, and the SMS and location disclosures. Users accept versioned terms (recorded). Retention schedule implemented (time records at least 3 years per payroll law; location, photos and messages per policy). Company data export and deletion on cancellation works end-to-end. |
| PRD-5 | Support processes, help documentation and incident response | V1 | Not started | `docs/runbooks/`, help site | REL-9 | ACCT (support mailbox) | Support email with an auto-acknowledgement and response-time targets. Help articles for the top 20 tasks. In-app help link. A support back-office view (company status, billing, recent errors) without seeing more data than needed. Incident response per REL-9. |
| PRD-6 *(added)* | Employee-monitoring and location notices | V1 | Not started | Consent screens, policy | MOB-2 | L | Legal review of notice requirements (some states require written notice of electronic monitoring). Notice text in the app and the owner's setup. |
| PRD-7 | Successful real-world beta across multiple independent companies | V1 | Not started | `docs/BETA.md` (new) | Beta-blockers Done | REAL, A | At least 3 independent lawn/field-service companies (not owned by Chad), at least 8 weeks of in-season daily use, at least 15 crew members total. Each V1 item's real-world DoD met. No data-loss or cross-company incident. Weekly feedback logged and critical issues resolved. At least 2 companies say they would pay at the planned price. |
| PRD-8 *(added)* | Accessibility basics | V1 | Not started | Web and mobile | REL-4 | — | Automated accessibility checks clean on main flows; large tap targets on mobile (already partly designed); keyboard use on web. |
| PRD-9 *(added)* | Data import from competitors | V1 | Partial: customer CSV import (22 assertions) | `apps/web/app/(app)/import` | — | REAL | Import customers, properties, recurring jobs and prices from Jobber and a spreadsheet export. 2 beta companies migrated with a counts check. |
| PRD-10 *(added)* | Product analytics (privacy-respecting) | V1 | Not started | Web and mobile | PRD-4 | ACCT | Activation and weekly-use metrics per company, no personal data, disclosed in the privacy policy. |

### 2.6 Optional enhancements (not required for V1)

| ID | Enhancement | Why later |
|---|---|---|
| ENH-1 | Road-based route optimization (paid provider) | Straight-line ordering works for small routes; costs money per request |
| ENH-2 | Drag-and-drop calendar | List board works; nice for larger crews |
| ENH-3 | E-signature on estimates | Acceptance in the portal covers V1 |
| ENH-4 | Payroll provider integration (Gusto etc.) | QuickBooks export first (BIZ-7) |
| ENH-5 | AI features (scheduling suggestions, message drafting) | Not needed to win first customers |
| ENH-6 | Marketing tools (review requests, campaigns) | Competitors' strength; later |
| ENH-7 | Point-in-time recovery | Required by 100 companies (REL-5 cost note), not at V1 |
| ENH-8 | Multi-region / high-availability hosting | Single region is fine at V1 scale with monitoring and DR |
| ENH-9 | Customer-facing native app | The portal on mobile web covers V1 |

---

## 3. Roadmap (dependency-ordered)

Estimates assume AI-assisted development with Chad as product owner and approver, about 15–20 hours a week of his time. Code moves fast; **the calendar is set by outside gates**:
- Apple, Google and Stripe approvals.
- SMS registration.
- Attorney review.
- Device testing.
- The mowing season, since real-world beta needs crews working daily, which in Tennessee means March onward.

| Milestone | Target | Contents (IDs) | Gate (all must pass) |
|---|---|---|---|
| **M0. Foundations** | Oct 13 – Nov 14, 2026 | DEV-1, DEV-2, DEV-4, REL-1, REL-3, REL-8, DEV-9, REL-11 (decision); T-1, T-2; staging test rounds 1–4; owner interviews (5–10); BIZ-0 draft; PRD-0 and PRD-3 decisions started | Platform line protected with required CI; monitoring alerting; elevated-function review done; Expo current; brand and entity decisions made |
| **M1. Delivery and jobs** | Nov 10 – Dec 12 | REL-2, BIZ-3, DEV-10, REL-5, REL-7 (invite-only sign-up, recovery, limits) | Jobs run 14 days in staging; messages delivered in staging; restore drill passed |
| **M2. Field app alpha** | Nov 17, 2026 – Jan 16, 2027 | MOB-0, MOB-1 (device builds), MOB-6, MOB-2, MOB-4, MOB-8 | Device test script passes on 4 phones; offline field test passed |
| **M3. Private beta start** | Jan 18 – Feb 28, 2027 | PRD-1, PRD-2, REL-4 (browser and API), BIZ-5, PRD-4 (attorney review started), PRD-5 (basic), PRD-9 | All Beta-blockers Done; 3+ companies onboarded on TestFlight / Play internal builds |
| **M4. Money and messaging** | Jan 4 – Apr 15 | BIZ-1, BIZ-2, BIZ-8, BIZ-7, BIZ-4 (10DLC registration has the longest lead time; start in January), MOB-3, MOB-5 | Every payment and billing path passed in Stripe test mode; SMS compliance verified; beta companies' Stripe onboarding done |
| **M5. In-season beta** | Mar 1 – Jun 30, 2027 | PRD-7, BIZ-6 real-world checks, REL-6, REL-4 (mobile), REL-9, REL-10, PRD-8, PRD-10, MOB-7 (store submission from May) | 8+ weeks of in-season use at 3+ companies; reconciliations pass; security review findings fixed |
| **M6. Commercial V1 launch** | Target Jul 2027 (range Jun–Sep) | All V1-required items Done | Chad's launch approval |

Seasonal note: lawn companies usually choose software in winter. A July V1 means the first big sales season is winter 2027–28. Beta companies can convert to paid at V1.

---

## 4. Approvals, accounts, paid services and legal reviews (summary)

| Item | Type | When needed | Est. cost |
|---|---|---|---|
| Brand name + trademark search; domain | A, L, $ | M0 | $10–20/yr domain; trademark search $0–500 |
| Business entity, bank, D-U-N-S | L, $, A | M0 | State LLC filing fees; D-U-N-S free |
| Error tracking + uptime/status page | ACCT | M0 | $0 on free tiers to start |
| Make repository private | A | M0 | Possibly GitHub Actions minutes |
| Expo account | ACCT | M2 | $0 to start; $19/mo later |
| Apple Developer | ACCT, $ | M2 | $99/yr |
| Google Play Console | ACCT, $ | M2 | $25 once |
| Resend Pro | $ | When over 3,000 emails/mo | $20/mo |
| Stripe (Billing, Connect, Tax) | ACCT, $, A, L | M4 | Per-transaction fees |
| SMS provider + 10DLC registration per company | ACCT, $, L | M4 | Per-text plus registration fees |
| Attorney: terms, privacy, data processing, SMS and location notices | L, $ | M3–M4 | Roughly $1,000–3,000 |
| Accountant: sales tax on SaaS and on lawn services | L, $ | M4 | Varies |
| External security review | $, A | M5 | $0 (structured self-review) to several thousand |
| Business insurance (cyber/E&O) | $ | Before paid launch | Roughly $50–150/mo |
| Every production deploy, migration, merge, enabling of messaging, payments, sign-up | A | Ongoing | — |

---

## 5. Things added that were not in the original list

These are marked *(added)* above.
- **Mobile:** MOB-0 build pipeline, MOB-8 forced updates.
- **Business:** BIZ-0 pricing, BIZ-7 accounting export, BIZ-8 sales tax.
- **Reliability and security:** REL-9 status page and incident response, REL-10 independent security review, REL-11 private repository (the code and security design are public today), REL-12 capacity limits.
- **Development:** DEV-9 Expo upgrade, DEV-10 email deliverability, DEV-11 generated types.
- **Product:** PRD-0 business entity and account ownership, PRD-6 employee-monitoring notices, PRD-8 accessibility, PRD-9 competitor data import, PRD-10 analytics.
- **Also: customer discovery.** 5–10 owner interviews before building billing. That isn't a feature, but it decides pricing (BIZ-0) and positioning.

---

## 6. Release-management strategy

**Goal:** one reviewed, protected line for the platform, with nothing touching the Flask clock or the live production app unless Chad approves it.

1. **Freeze Flask where it is.**
   - `main` stays the Flask app, locked, with Render auto-deploying it as today.
   - Tag its current state `flask-live-e9144e7`.
   - Nothing platform-related is merged into `main`. Render ignores only `platform/**`, so merging docs or workflows into `main` would redeploy Flask.
   - At retirement, a separate approval decides whether `main` becomes the platform line.
2. **Create a protected platform line, `platform-main`.**
   - It starts from the current `production-ops` tip, the code production runs, via one reviewed baseline PR.
   - Required: passing CI (database tests, typecheck, web build; later browser and mobile tests) and Chad's review.
   - No direct pushes; linear history.
   - The 22 stacked PRs are then closed as superseded, with links. PR #1's Flask hotfix stays open for its own decision.
3. **Work happens on feature branches** named `feat/<ID>-short-name` (IDs from this plan), then a PR into `platform-main`, then CI, then review.
4. **Releases are tags** on `platform-main` (`v2026.11.0`, `v2026.11.1` …) with release notes listing plan IDs and test evidence.
5. **Staging first.**
   - `web-staging` is moved to the release tag, staging is deployed manually, and database migrations go through the staging workflow.
   - The staging checklist is run and signed off.
6. **Production by approval.**
   - Chad approves; `web-release` moves to the same tag; the production deploy is manual.
   - Database migrations go through the gated production workflow (`production-ops` stays only for those approval requests).
   - Migrations follow expand-then-contract, so the previous app version keeps working and the app can be rolled back.
7. **Rollback.**
   - **Web:** redeploy the previous tag (Render keeps previous deploys).
   - **Database:** forward-fix migration; restore from backup only as a last resort, per the DR runbook.
   - **Mobile:** stop the staged rollout, or ship an over-the-air fix to the right channel with approval.
8. **Hotfixes.** Branch from the live tag, open a PR into `platform-main`, tag a patch, then staging, then production.
9. **Mobile channels.** EAS "staging" builds point only at staging and "production" builds only at production (the guard is already in the app). Store releases use staged rollouts.

---

## 7. Five highest-priority engineering tasks (start now, with approval)

1. **DEV-1 / DEV-2 / DEV-4: create `platform-main` and reconcile the 22 PRs.** Every later change needs a reviewed, protected line, and Flask must stay safe while it's created.
2. **REL-1: error monitoring and uptime alerts.** Free tiers are enough. Without them, every later test and beta problem is invisible.
3. **REL-3: review the 83 elevated functions and add the automatic guard check.** This is the one place a single mistake leaks one company's data to another.
4. **REL-2: the scheduled job runner,** staging first. It unblocks message delivery, reminders, weather, recurring visits and forgotten-clock-out alerts.
5. **DEV-9 + MOB-0: upgrade Expo and stand up the EAS build pipeline,** then get an Android staging build onto Chad's phone. Every field feature (GPS, push, offline, photos) needs real devices, and this clears the 23 high audit findings.

---

## 8. Progress summary

| Status | Count of V1 items |
|---|---|
| Done | 0 |
| Verified (staging) | 1 (DEV-5) |
| Built | 4 (MOB-6, BIZ-3, BIZ-5, BIZ-6) |
| Partial | 16 |
| Not started | 26 |
| Unknown | 1 (PRD-0) |
| Holding as intended | 1 (DEV-3, Flask protected) |
| **Total V1 items** | **49** (32 from Chad's list of 2026-10-13, 17 added) |

Progress reports list each item whose status changed, with its evidence.

## Change log

| Date | Change |
|---|---|
| 2026-10-13 | Plan adopted from Chad's requirements of 2026-10-13, plus additions; all statuses set from the read-only review of the same day. |
