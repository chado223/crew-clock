# ADR 0001: Platform architecture and permanent product requirements

**Status:** Accepted by Chad Washam, 2026-10-05
**Context:** Phase 0 audit (`docs/ARCHITECTURE_AUDIT.md`)

## Decision

1. **Keep the Flask Crew Clock running** unchanged (except security fixes) until the crew is moved to the new app in Phase 2. It is retired only after a verified cutover.
2. **Supabase/PostgreSQL is the single source of truth**, using the **existing** Supabase project. No new production project without Chad's instruction.
3. **Web/admin:** Next.js (TypeScript). **Mobile:** Expo / React Native (TypeScript) for iOS and Android.
4. **Business rules live in the database** (Postgres functions + Row Level Security), so web, mobile, exports and background jobs share one implementation.
5. **Google Sheets is an optional export only.** Payroll and timekeeping never depend on it.
6. **Monorepo** under `platform/` in this repo (`platform/supabase`, later `platform/apps/web`, `platform/apps/mobile`, `platform/packages/shared`). Nothing is added at the repo root, so the Render Python deploy of the Flask app is unaffected.

## Permanent requirements (apply to every phase)

- **Commercial multi-tenant SaaS, released on the Apple App Store** (Google Play after). Every decision must support the native mobile release rather than require rebuilding later.
- **Self-serve:** a lawn-care or field-service company can sign up, create its company, invite employees and start working with **no manual Supabase setup** by us.
- **Tenant isolation is enforced in the database** (RLS) and proven by automated tests that block release.
- **CRM is core.** The customer/property record is the hub connecting leads, communications, follow-ups, estimates, recurring services, scheduling, jobs, photos, invoices, payments, service history, profitability, upsells and the customer portal. Every module writes to the customer timeline (`activity` table) from the start.
- **One authoritative hours calculation** (`timesheet()` / `weekly_hours()` in Postgres) used by the web dashboard, mobile app, payroll reports and exports. No client computes hours on its own.
- **Employee time is never silently changed.** Corrections go through functions that require a reason and write before/after to the audit log.
- **No historical data is removed or destroyed** during migration. Legacy SQLite and Google Sheets stay untouched until migrated data is reconciled and Chad signs off.

## Full product scope

CRM · clients/properties · employees · time clock · scheduling · routes · jobs · estimates · invoicing · payments · expenses/profitability · customer portal · weather automation · AI · iOS/Android.

## Consequences

- Clients (web/mobile) talk to Supabase directly with the user's session. Privileged operations are Postgres functions that check membership and role themselves.
- Secrets (Stripe, service role, AI keys) live only in Edge Functions / server environment, never in app bundles.
- Schema changes go through numbered migrations in `platform/supabase/migrations`, tested locally before they touch the Supabase project.
