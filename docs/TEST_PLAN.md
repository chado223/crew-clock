# Crew Clock test plan (owner testing, prepared 2026-10-11)

**Status: plan only. No test data has been created.** Every round below needs Chad's approval before it starts.

This is a survey of the code and of what's running in production as of 2026-10-11 (web app on Render, database `crew-clock-prod`, company "Chad Washam Lawncare").

## What's actually running in production

| Piece | Running? |
|---|---|
| Web app (office/owner screens, customer portal pages) | ✅ `crew-clock-web.onrender.com` |
| Database rules, permissions, audit trail | ✅ |
| Sign-in emails (codes) | ✅ via Resend |
| **Phone app** (crew clock-in, visit photos, breaks, offline) | ❌ **Not installed anywhere.** It needs a build (Expo/EAS). The production build settings exist but have never been built. |
| **Message sender** (estimates, invoices, reminders, invites by email/SMS) | ❌ **No sender runs in production.** "Send" puts a test-mode message in the outbox; nothing is delivered. Live sending is locked twice over. |
| **Weather fetcher** (daily forecast pull from the free National Weather Service) | ❌ It runs only for staging, so production forecasts stay empty. |
| Online payments | ❌ Built as a model only; switched off |
| New sign-ups (crew, customers) | ❌ Off by design right now. Invites can be created, but the person can't make an account until sign-up reopens. |

## Feature status

**Ready** means it works end-to-end in production today. **Partial** means part works and part waits on something above. **Blocked** means it can't be tested in production yet. **Placeholder/draft** means it isn't finished.

| # | Area | Status | What works now | What doesn't (yet) |
|---|---|---|---|---|
| 1 | Sign-in, account | **Ready** | Email code sign-in, sign out, account page | "Delete my account" works; **don't test it on your real account** |
| 2 | Owner dashboard (home) | **Ready** | Overview, "Needs attention", today's work, hours | Mostly empty until there's data |
| 3 | Company settings | **Ready** | Name, address, tax rate, payment terms, invoice/estimate notes, overtime, week start | |
| 4 | Customers / CRM | **Ready** | Add/edit customers and properties, notes, history timeline, jobs (one-off and recurring), visit generation | Portal invite: see 17 |
| 5 | Services / price list | **Ready** | Add/edit services and prices | |
| 6 | Team / employees | **Partial** | Employee records, pay rates, crews, roles | Inviting a person to sign in: blocked (sign-up off, no sender) |
| 7 | Time clock | **Partial** | Web: timesheets, weekly hours, overtime, add/correct/void entries with reasons, break corrections, CSV export | **Live clock in/out is phone-only**, and the phone app isn't installed |
| 8 | Scheduling | **Ready** | Generate visits from jobs, assign crews, reschedule, skip, mark done, reopen | |
| 9 | Routes | **Partial** | Order the day's stops (straight-line estimate), reorder by hand, open in Maps | No real road routing; that would be a paid provider, not chosen |
| 10 | Weather | **Placeholder in production** | Page and alerts are built | No forecasts until a production fetch job is approved (free NWS data, a GitHub schedule) |
| 11 | Job costing / profit / insights | **Ready** (needs data) | Labor and expense cost per visit and job, profitability, receivables, at-risk customers | Meaningful only after visits, time and pay rates exist |
| 12 | Estimates | **Partial** | Create, edit, accept/decline, convert to job | "Send" only queues a test-mode message |
| 13 | Invoices & payments | **Partial** | Invoice completed visits (one or all), record manual payments, voids, letterhead | "Send" only queues; **online payment is a placeholder** ("coming soon" in the portal) |
| 14 | Expenses | **Ready** | Add, categorize, void; they feed into profit | |
| 15 | Messages (outbox) | **Partial** | Templates, outbox, test mode, cancel | Nothing is delivered (no sender); live mode locked |
| 16 | Import / export | **Ready** | CSV customer import with preview, then commit; CSV exports | |
| 17 | Customer portal | **Blocked in production** | Built and tested on staging: visits, invoices, estimates, requests, photos, contact preferences | A customer needs an account, and sign-up is off |
| 18 | Visit photos | **Blocked** | Built (private storage) | Taken from the phone app |
| 19 | Privacy / terms pages | **Draft** | Pages exist | Marked "Draft, not yet reviewed" until you approve the wording |

## Where to test: decision needed first

Testing creates records. Some can't be fully erased: audit history, voided invoices and their numbers stay on file by design. So the first decision is where the test data goes:

- **A. Staging copy (recommended).**
  - A free Render web service pointed at `crew-clock-staging`. It sleeps when idle, which is fine for testing.
  - Use made-up customers, open sign-up there, and test crew and customer logins freely.
  - Production stays clean for real use.
  - Needs: wake the staging database (free, one click, in the old Supabase org) and approve a $0 Render service.
- **B. Production with labeled test records.**
  - Nothing new to set up.
  - Every test record is named "TEST – …" and voided or archived afterwards.
  - The voided rows and audit entries stay in your real books.
  - Crew login, portal and phone tests stay blocked until sign-up reopens.

## Test rounds (each started only with Chad's approval)

| Round | Covers | Needs |
|---|---|---|
| 1. Office basics | 1, 2, 3, 5, 14, 16 (export only) | Nothing new |
| 2. Customers to schedule | 4, 8, 9 | Round 1 |
| 3. Money | 12, 13, 11 | Round 2 (completed visits) |
| 4. Team and time (web side) | 6 (records only), 7 (web corrections/exports), 11 labor cost | Round 2 |
| 5. Crew experience | 7 live clock, 18 photos, offline | A phone build: Android test build (free) or iOS (Apple Developer, $99/yr). On staging (A), or production once sign-up reopens. |
| 6. Customer experience | 17 portal, 12 and 13 from the customer side | A customer login: staging (A), or production after sign-up reopens |
| 7. Messages and weather | 10, 15 delivery | A production sender and weather job (separate approvals); live messaging stays locked |

For each item, Chad records **works / broken / confusing**, with a note. I log fixes in `docs/ISSUES.md`, fix on a branch, rehearse, and redeploy only with his approval.
