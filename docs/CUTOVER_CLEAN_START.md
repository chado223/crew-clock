# Cutover plan: clean start (chosen by Chad, 2026-10-10)

**Status: prepared, not approved.** Nothing in this file has been run. Each numbered step needs Chad's go-ahead, and every production run also needs his click on GitHub (Environments → production).

## What "clean start" means

- The new Crew Clock (`crew-clock-prod`) starts with **no data from the old systems**. Chad creates the company in the app and becomes its owner automatically.
- **Nothing is imported.** The old shared project's demo data (Cool Springs HOA, the $350 invoice, $258.50 expenses) stays where it is, frozen, plus the encrypted copy in `crew-clock-backups`.
- **The Sheet history is not imported.** It holds 0.04 hours of test punches. It stays in the original Sheet, the private Drive copy, and the encrypted copy.
- The crews keep using the **Flask clock and the Sheet** until Chad decides otherwise. Moving the crews over is its own later decision (step 7).

## Audit before choosing this (2026-10-10): did the Flask app hold hours that never reached the Sheet?

What the Flask app does with a punch:
1. **Saves it** to a SQLite file on the Render server (`clock.db`). It uses no other database: Render has no Postgres here, the shared Supabase project has 0 time entries, and `models.py`/`DATABASE_URL` aren't used by `App.py`.
2. **Writes it to the Sheet.** If that fails, it prints `Sheets logging failed` to the Render log, and the punch exists only in that SQLite file.

The service is on Render's **free plan**: no persistent disk, and it shuts down after about 15 minutes idle. Every shutdown or redeploy erases `clock.db`. So a punch that missed the Sheet survived only until the server next went to sleep.

Findings:
- **Last 30 days (all Render keeps):** the server woke 3 times (Oct 4 ×2, Oct 5, during our own audits). Each time it ran about 15 minutes and shut down. There was not one `Sheets logging failed` line or error.
  - Every punch leaves either a Sheet row or that line, and the Sheet has nothing after July 15. So **no punches were taken in the last 30 days.**
- **Before that:** Render no longer has the logs. Whatever the server held then was erased at its next sleep, so **there is no surviving copy anywhere to recover.**
- **One sign of the sync problem:** aiden fowler's clock-in on **2026-07-15** reached the Sheet, but no clock-out ever did. That's consistent with a clock-out that failed to sync (or was never made).
  - If real work happened after mid-July that isn't in the Sheet, the only sources left are outside the system: crew memory, texts, job schedules and payroll records.
- **Which Sheet the server writes to:** **verified 2026-10-10.** Chad compared Render's `SHEET_ID` with the original Sheet's address, and they match exactly (step A).

**Historical-data verification: complete (2026-10-10).** Conclusion: no recoverable, unsynced employee time exists in the Flask system. Nothing real is lost by starting clean, beyond what was already erased by Render's free plan.

## Already done (preparation)

Verified read-only on 2026-10-10:
- **Backups:**
  - old database restore verified;
  - Sheet fingerprinted;
  - encrypted off-site copy unlocked on Chad's PC;
  - Supabase Pro daily backups on.
- **Production project:**
  - 36 migrations, marked production, no other-app data;
  - 0 companies, 0 messages, 0 sessions;
  - messaging and payments off;
  - row security everywhere; photos private.
- **Sign-in:** owner-only code test passed (`noreply@chadwashamlawns.com`, 6-digit code).
- **Code safety:** tenant-isolation tests green on staging, and `main` locked.

## Steps (each needs Chad's approval)

| # | Step | Who | Touches production? | Undo |
|---|---|---|---|---|
| A ✅ | **Done 2026-10-10, match.** Check the Flask app's Sheet: Render → crew-clock → Environment → `SHEET_ID`. It should match the ID in the original Sheet's web address. Read only. | Chad | No | n/a |
| B | **Decide where the new web app runs** (it isn't hosted anywhere yet). Pending since the production plan. Options and prices come in a separate note before any choice. | Chad | No | n/a |
| 1 | Preflight, read-only (`action=preflight`, clean mode): expect every safety check to pass. *one company* and *owner* show "not yet". | me + Chad's click | Read-only | n/a |
| 2 | Put the web app on the chosen host with `NEXT_PUBLIC_APP_ENV=production` and the production URL and public key. The app refuses to start if those don't match. Not announced or linked anywhere. | me (+ Chad for any account) | No | Take it down |
| 3 | Chad signs in with an emailed code and creates **Chad Washam Lawncare** in the app. He becomes owner automatically. Messages default to test mode, and the platform switch blocks live sending. | Chad | Creates 1 company | Delete it (no other data) |
| 4 | Preflight again, clean mode: expect **all** checks to pass. That means 1 company, owner `chadwasham64@gmail.com`, nothing carried over, messaging off. | me + Chad's click | Read-only | n/a |
| 5 | Chad's own end-to-end check: add a test employee, clock in/out, void it, and make a test invoice. Confirm it shows **test mode** and nothing is emailed to anyone. | Chad + me | Test rows (voidable, audited) | Void |
| 6 | **Stop and report.** The new app runs for Chad only, and the crews are still on Flask + Sheet. | — | — | — |
| 7 | *Later, separate approval:* invite crews (invite emails are crew communications), then run Flask and the new app side by side. | — | — | — |
| 8 | *Later, separate approval:* retire Flask, after the crews have used the new clock and Chad is satisfied. The Sheet stays as an archive. | — | — | — |

Not in this plan, and each needs its own approval:
- live customer messages;
- online payments;
- app-store release;
- importing the Sheet history;
- retiring the old shared project. It stays frozen for at least 90 days.

## Rollback

- **Before step 2:** nothing has changed.
- **After step 3:** the only data is Chad's company and test rows. Delete the company, or rebuild the project from migrations (rehearsed, about 30 minutes).
- **Flask is never touched,** so the crews are never affected.
- **Supabase Pro daily backups** (7 days) cover anything entered after go-live.

## Risk while the crews stay on Flask

The Sheet is the Flask app's only durable record. If writing to the Sheet fails again, punches are lost when the free server sleeps. Until the crews move, a quick weekly look that the current week's tab is filling in catches that early. Fixing the Flask app itself would mean deploying it, which isn't approved.
