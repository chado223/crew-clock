# ADR 0002: Time tracking model and the single hours calculation

**Status:** Accepted (implements requirement in ADR 0001)

## Problem
The Flask app computes hours in two places with two different rules, stores naive local timestamps, and lets anyone record IN/OUT under any typed name. The result was different hours on screen and in payroll, wrong hours across DST, and no protection against double punches (audit bugs B1–B4).

## Decision

### Storage
- `time_entries` holds **one row per shift**: `clock_in`, `clock_out` as `timestamptz` (absolute instants, so DST can't distort durations).
- Each entry belongs to an **`employee`**, a company-scoped person record. An employee may or may not have a login (`employees.user_id`), which supports legacy history and workers who haven't been invited yet.
- Unpaid breaks live in `time_entry_breaks`.
- **At most one open shift per employee**, enforced by a unique index. Duplicate clock-ins are impossible at the database level, whatever the client does.
- **Idempotent punches:** every clock-in/out from a device carries a `client_event_id` (UUID made on the phone). Re-sending the same event after a dropped connection returns the original result instead of creating a duplicate. This is the foundation for the offline queue.

### Writes go only through functions
`authenticated` users have **no INSERT/UPDATE/DELETE privilege** on `time_entries`. All changes go through:

| Function | Who | Notes |
|---|---|---|
| `clock_in`, `clock_out` | the employee themselves | device time accepted within a bounded window (offline punches), server receipt time also stored |
| `start_break`, `end_break` | the employee | |
| `correct_time_entry` | owner/admin | **reason required**; before/after written to `audit_log` |
| `add_time_entry` | owner/admin | for missed punches; reason required |
| `void_time_entry` | owner/admin | soft void with reason; row is never deleted |

An audit trigger on `time_entries` records every change, including changes made with the service role, with the actor and the reason.

### The calculation (the only one)
- `timesheet(tenant, from_date, to_date)`: one row per shift with `work_date`, `worked_seconds`, `break_seconds`, `status`.
- `weekly_hours(tenant, week_start)`: per employee regular, overtime and total seconds, plus a count of open/needs-review shifts.

Rules:
- **Worked time** = `clock_out − clock_in − unpaid break time`, computed on absolute instants (DST-correct).
- **Work date** = the date of `clock_in` in the **company's time zone** (`tenants.timezone`). Overnight shifts count toward the day they started, matching current practice.
- **Workweek** starts on the company's `week_start_day` (default Monday, matching the existing ISO-week sheets).
- **Overtime** default: hours over 40 in the workweek (FLSA). The threshold is a company setting, since some states also have daily overtime rules; that's a later extension inside the same function.
- **Open shifts are excluded from totals** and counted separately, so a forgotten clock-out shows up as a problem to fix rather than as 32 hours of pay.
- Voided entries are excluded.

Web, mobile, payroll export, the weekly Google Sheets export and AI summaries all call these two functions. Both are `SECURITY INVOKER`, so a crew member calling them sees only their own shifts.
