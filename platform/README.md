# Platform

The new multi-tenant field-service SaaS (see `docs/decisions/0001`). The Flask Crew Clock at the repo root keeps running until the Phase 2 cutover.

```
platform/
  supabase/
    migrations/   numbered SQL migrations, applied in order (source of truth for the schema)
    tests/        database tests; run.sh builds an "upgrade" and a "fresh" database and runs every file
      fixtures/   local Supabase stand-in, simulated current production, two-company test world
  apps/web        Next.js admin/office dashboard            (next)
  apps/mobile     Expo crew + owner app (iOS/Android)       (next)
  packages/shared generated DB types, validation, API client (next)
```

## Database tests

```bash
bash platform/supabase/tests/run.sh
```

Needs `psql` and either a Postgres 15+ server (`PG*` env vars) or a local Postgres install (the script starts a throwaway one). CI runs it on every change under `platform/supabase/`.

Two scenarios:
- **upgrade**: a copy of the current production shape, including its first-draft policies and seed data, upgraded by the migrations. Proves no existing row is lost or changed.
- **fresh**: an empty project built from migrations (staging, CI, new environments).

Tenant isolation tests are a **release blocker**.

## Calling the backend

Clients use the signed-in user's Supabase session. Business operations are Postgres functions:

| Area | Functions |
|---|---|
| Companies | `create_tenant(name, timezone)`, `my_companies()` |
| Team | `invite_member(tenant, email, role, employee_id?, name?)` → token, `accept_invitation(token)`, `revoke_invitation(id)`, `set_member_role`, `remove_member` |
| Time | `clock_in(tenant, event_id, at?, job?, notes?, source?)`, `clock_out(tenant, event_id, at?, notes?)`, `start_break`, `end_break` |
| Corrections | `correct_time_entry(id, in, out, reason)`, `add_time_entry(...)`, `void_time_entry(id, reason)` |
| Hours (the only calculation) | `timesheet(tenant, from, to)`, `weekly_hours(tenant, week_start)` |

Errors are raised with stable message keys (`already_clocked_in`, `not_clocked_in`, `punch_too_old`, `reason_required`, `forbidden`, …) for the apps to map to friendly text.
