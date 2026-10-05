# Production reconciliation (2026-10-05)

Read-only inspection of the live systems, compared against the handoff and the Phase 1 migrations. Nothing was changed in either system, except that the Supabase project was **restored from pause** so it could be inspected.

## Supabase: project `iwowjrnrbjiydckhjsfi` (free plan, Postgres 17, us-east-2)

### What's actually there

| Table | Rows | Notes |
|---|---|---|
| tenants | 1 | Chad Washam Lawncare (`055bdb3c…`), plan `pro` |
| clients | 1 | Cool Springs HOA |
| jobs | 1 | Weekly Mow & Edge |
| invoices | 1 | $350, sent |
| expenses | 4 | Fuel $45, Equipment $120, Supplies $18.50, Maintenance $75 |
| **memberships** | **0** | **No one is linked to the company**, including Chad (handoff said owner was linked) |
| **time_entries** | **0** | No time history in Supabase |
| profiles | 0 | |
| scenarios | 0 | **Not part of this app** (see below) |
| auth.users | 2 | `chadwasham@gmail.com`, `chadwasham64@gmail.com` |

No migration history (schema was created by hand in the SQL editor).

### Differences from the handoff, and how the migrations now handle them

| Finding | Risk | Handled by |
|---|---|---|
| `memberships.role` is an enum `user_role`, not text | Functions writing text roles would fail | Casts added in onboarding functions |
| `profiles` has `email`, `stripe_customer_id`, `is_pro`, no `full_name`; plus a `scenarios` table | Another app shares this project. Locking these down blindly would break it, and a naive "edit own profile" rule would let users set `is_pro` themselves | `full_name` added; users can update **only** `full_name`; `scenarios` and its grants/policies left untouched; tested |
| `time_entries.user_id → auth.users ON DELETE CASCADE` | Deleting a login would **erase that person's hours** | Changed to `SET NULL`; tested |
| `invoices.client_id → clients ON DELETE CASCADE`; every table cascades from `tenants` | Deleting a client erases its invoices; deleting the company erases everything | Changed to `RESTRICT`; company deletion becomes an explicit, audited process later |
| `anon` and `authenticated` have every privilege (incl. TRUNCATE) on every table | RLS is the only barrier; TRUNCATE bypasses RLS | Platform tables: anon gets nothing, users get only what's needed |
| `time_entries` has RLS on but no policies | Nobody can read time (safe, but unusable) | Replaced with own-time / manager policies |
| Helper functions have mutable `search_path` (Supabase advisor WARN) | Function hijacking | Pinned `search_path`, `SECURITY DEFINER` |
| `tenants_insert` allows any user to insert a company row with no owner | Orphan companies | Replaced by `create_tenant()` |
| `expenses.spent_at` is `date`; `invoices.client_id` NOT NULL; `tenants.plan` default `free` | Compatible | Baseline mirrors exactly |

The baseline migration and test fixture now reproduce production exactly (schema, policies, functions, every row). The upgrade test proves every production row survives with all original values.

### Dashboard settings to change before launch (not schema)
- Leaked-password protection is off; only one MFA option enabled.
- Postgres 17.4.1.074 has security patches available (upgrade involves brief downtime → needs approval).
- **Free plan pauses after inactivity.** It was paused today. Fine for development; a production SaaS needs the Pro plan (~$25/month). Owner decision before launch.

## Render: service `crew-clock` (free plan, Ohio)

- Auto-deploys from `main` on every commit. **Merging PR #1 deploys to the live clock.**
- Last deploy 2025-11-05 (commit `e9144e7`, current `main`). Health check `/health`.
- **Free plan: no persistent disk**, and the service sleeps after ~15 idle minutes (logs show start/stop cycles). Every sleep wipes `clock.db`. **The SQLite database has never held history for more than a session; Google Sheets is the only time record.**
- No request logs in the last two weeks: the clock appears unused recently (Render free log retention is limited, so not certain).
- Env vars weren't readable through the connector; whether `SECRET_KEY` is set is still unknown.

## Consequences for migration
- There is **no time history to migrate from SQLite or Supabase**. The Google Sheet is the source. Migrating it needs the Google Drive connector (Phase 2).
- Before anyone can use the new app on the existing company, an owner membership must be created for Chad's account (a production data change → needs approval, and which of the two emails is the login).
