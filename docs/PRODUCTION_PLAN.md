# Production plan: dedicated project, backups, sign-in email

Status: **prepared, waiting for Chad's approval.** Nothing in this document has been created, purchased, migrated or sent. The current production project (`iwowjrnrbjiydckhjsfi`), the live Flask clock on Render and the Google Sheet are untouched.

Prices are what the vendors published as of mid-2026. I couldn't open the pricing pages from this workspace, so confirm the number on the sign-up screen before paying.

---

## Part 1 — Move Crew Clock to its own Supabase project

### Recommendation

**Create a new Supabase organization for CWLC on the Pro plan, with one project, `crew-clock-prod`, in us-east-2 (Ohio). Move Crew Clock's rows into it. Leave the old shared project exactly as it is.**

Why a new project instead of upgrading the shared one:

| | Upgrade the shared project in place | **New dedicated project (recommended)** |
|---|---|---|
| Other app (`scenarios`, `is_pro`, `stripe_customer_id`) | Lives beside customer data forever; every migration has to tiptoe around it | Not in the new project at all |
| Risk to existing data | Migrations run on the only copy | Old project is never written to; it *is* the rollback |
| Rollback | Emergency revert script (tested, but it's surgery) | Point the apps back at the old project |
| Migration history | Hand-built schema + 30 migrations on top | Clean history from day one |
| Downtime for the live clock | None (Flask app uses Sheets) | None |
| Data to move | — | 1 company, 1 customer, 1 job, 1 invoice ($350), 4 expenses ($258.50). No time history (that's in the Sheet). |

There's very little production data, which makes this the cheapest moment to separate.

### Expected costs

| Item | Monthly | Notes |
|---|---|---|
| Supabase Pro, new CWLC org | **$25** | Includes $10 compute credit, which covers the one Micro database. 8 GB disk, 250 GB transfer, 100k monthly sign-ins, daily backups kept 7 days, never pauses. |
| Old org (shared project + staging) | $0 | Stays on Free. Free orgs get 2 active projects, which is exactly what's left there. Free projects pause after a week without activity; waking staging is one free click. |
| Sign-in email (Resend free tier) | $0 | 3,000 emails/month, 100/day. See Part 3. |
| Sending domain | $0 if CWLC already owns one | Otherwise about $10–15/year. |
| Off-site encrypted backups (private GitHub repo) | $0 | See Part 2. |
| **Total to start** | **≈ $25/month** | |

Alternatives priced for comparison:
- Upgrade the existing org to Pro instead: all three projects bill compute, so about **$45/month** ($25 + two extra Micro databases at ~$10), and the other app ends up on your paid plan.
- Point-in-time recovery (restore to any minute): about $100/month and needs a bigger database. **Not recommended until there are paying customers**; daily backups plus our nightly encrypted dump cover a one-company launch.
- Custom domain for the sign-in service: ~$10/month. Not needed: sign-in is by typed code, so customers never see the Supabase address.

Not part of this decision (still waiting separately): web hosting plan, Apple Developer account ($99/year), route optimization, SMS.

### Migration plan

Every step below needs your go-ahead once; steps marked **(you)** need your hands because they involve billing or your accounts.

| # | Step | Who | Reversible? |
|---|---|---|---|
| 0 | Back up the old project and the Sheet (Part 2). Nothing proceeds until both backups are verified. | me + you | n/a (read-only) |
| 1 | Create the CWLC org, choose Pro, add the card. | **you** (2 min) | Delete org |
| 2 | Create `crew-clock-prod` in us-east-2 in that org. | me (after 1) | Delete project |
| 3 | Apply every migration with `scripts/apply_migrations.sh` (same script and fingerprints staging has used for all 28 migrations). | me | Delete project |
| 4 | Turn on the dashboard settings from the reconciliation report: leaked-password protection, a second MFA option, current Postgres patch level. | me | Yes |
| 5 | Export from the old project with `ops/0003_export_for_dedicated_project.sql`. It runs in a **read-only transaction** — Postgres itself refuses any write. | me, read-only | n/a |
| 6 | Import into the new project with `ops/0004_import_into_dedicated_project.sql`: keeps every original id, date and amount; checks row counts and money totals against the export; all-or-nothing; refuses to run if pointed at the old project. | me | Re-create project |
| 7 | You sign in to the new project once (email code). Then `ops/0001_link_production_owner.sql` makes `chadwasham64@gmail.com` the owner. | **you** + me | Yes |
| 8 | Point the web and mobile app settings at the new project's URL and public key. | me | Yes, point back |
| 9 | Run the end-to-end checks against production as you (sign in, see the company, customer, invoice and expenses; clock in/out on a test employee and void it). | me + you | Voids are audited, nothing deleted |
| 10 | Old project stays as-is, read-only by habit, for at least 90 days. Retiring it is a separate decision. | — | — |

The old project's second login (`chadwasham@gmail.com`) has no membership and isn't moved. Anyone else just signs in to the new project with their email.

### Data preservation

- The old project is never written to. The export is read-only; the import refuses the old project.
- Original ids, dates and amounts are kept. New columns get their normal defaults.
- The import checks counts and totals against the export manifest and keeps nothing if they disagree.
- Running the import twice adds nothing.
- The other app's data stays where it is and is never copied.
- **Rehearsed in CI on every change** (`tests/move/check_move.sh`): it builds a copy of the current production database (schema plus its real rows) and a fresh project from the migrations, then runs the export and import. It proves:
  - the old copy is byte-for-byte unchanged;
  - all 8 rows arrive with identical values;
  - a re-run adds nothing;
  - the import refuses the old project;
  - the owner link works afterwards.

### Rollback

Until step 8, rolling back means doing nothing: production is still the old project.

After step 8:
1. Point the web/mobile settings back at the old project's URL and key. Takes about 5 minutes and is a redeploy, not a data change.
2. Anything entered in the new project in the meantime is exported first with **Import & export → Export** (customers, jobs, invoices, payments, expenses, time as CSV). Nothing is deleted from the new project.
3. If the new project itself is the problem, it can be deleted and rebuilt from steps 2–7. That repeats a rehearsed process and takes about 30 minutes.

The live Flask clock and the Google Sheet are not involved at any step, so field crews are never affected.

---

## Part 2 — Backups of the existing production database and the Sheet

No source is modified or deleted. Both procedures only read.

### A. Existing production database (`iwowjrnrbjiydckhjsfi`)

Goal: a complete, restorable copy before anything else happens. It covers the other app's data too, so nothing can be lost by accident.

1. **Dashboard download** (you, 1 minute): Supabase → the project → Database → Backups. Free projects don't keep scheduled backups, so instead use Project Settings → Database → "Download backup" if offered. Otherwise use step 2.
2. **Full logical dump** (me, with your approval and the connection string pasted into a GitHub secret by you; I never see or store it):
   `pg_dump --schema=public --schema=auth --schema=storage -Fc` plus a plain-SQL schema file. The dump only reads.
3. **Encrypt before storing.** This repository is **public**, so production data must never go into it or into its CI artifacts, which any signed-in GitHub user can download from a public repo. The dump is encrypted with `age` to a key only you hold, then stored in a new **private** repo `chado223/crew-clock-backups` (free).
4. **Verify**: restore the dump into a throwaway local Postgres. Compare row counts per table and invoice/expense totals with the live project, using read-only `select count(*)` queries. Record the checksums in the backups repo.
5. After the move, the same nightly job (already rehearsed against staging as `staging-backup.yml`) runs against the new production project. It writes encrypted output to the private repo and keeps 30 days. Supabase Pro's own daily backups (7 days) are the first line of defense; this is the second, off-site copy.

### B. Google Sheets time history

The Sheet is the **only** record of past hours: the old SQLite file is wiped whenever Render's free service sleeps, and the shared Supabase project has no time entries.

1. **Copy inside Google** (you, 1 minute): open the Sheet → File → Make a copy → name it `Crew Clock time history — archive YYYY-MM-DD` → put it in a folder only you can edit. The original keeps working; the Flask app keeps writing to it.
2. **Download** (you, 1 minute): File → Download → Microsoft Excel (.xlsx). That one file holds every tab: weekly tabs, totals tabs and the all-weeks summary.
3. **Fingerprint** (me, once you attach the .xlsx): `python3 platform/tools/sheets_manifest.py file.xlsx` opens it read-only and records:
   - the file's SHA-256;
   - every tab and its row count;
   - hours per crew per week, as the sheet recorded them.

   That manifest is the yardstick for any future import.
4. Store the .xlsx and manifest in the private backups repo next to the database dump.
5. **Importing that history into the new app is a separate, later step that needs its own approval.**
   - The schema is ready: `time_entries.source = 'legacy_sheets'` plus `legacy_ref`, which records the tab and row.
   - Crew names in the Sheet ("Crew 1") need mapping to employees, which you'd confirm.
   - Rows that don't pair up cleanly get flagged for review, not guessed.
   - Totals must match the manifest week by week.

---

## Part 3 — Sign-in email (codes and invites)

### What happens today

Sign-in is by a 6-digit emailed code (web and mobile). Supabase's built-in mailer is for development only: it sends only to the project's own team members and is heavily rate-limited. Real users need a proper sending service before launch.

### Options compared

| Service | Free tier | First paid tier | Fit |
|---|---|---|---|
| **Resend (recommended)** | 3,000/month, 100/day, 1 domain | $20/month for 50,000 | Simple API and SMTP; works directly as Supabase's mailer; supports idempotency keys so a retry never double-sends. Free tier covers sign-in codes for dozens of companies. |
| Postmark | 100/month (testing only) | ~$15/month for 10,000 | Best reputation for transactional mail. Good upgrade path if deliverability ever becomes a problem. |
| Amazon SES | Limited free tier | ~$0.10 per 1,000 | Cheapest at large volume, but needs an AWS account, a sandbox-exit request and more setup. Overkill now. |
| Brevo | 300/day | ~$9/month | Marketing-first, shared sending reputation. Fine, but not better than Resend for codes. |

### What's prepared (nothing enabled)

- `platform/supabase/email-templates/`: plain, branded templates for the sign-in code, sign-up confirmation and email change, using Supabase's `{{ .Token }}` code.
- `platform/packages/messaging/src/resend.ts`: a Resend provider for the existing message dispatcher (reminders, invoices, invites). It adds a **third lock** to the two already there:
  1. The database decides test or live per message. Live messaging is off platform-wide.
  2. The dispatcher holds every live message unless `MESSAGING_LIVE=1`.
  3. The provider refuses any address not on an approved test-inbox list unless `EMAIL_LIVE_APPROVED=yes` is also set.
  - It's tested with a fake network: a real address is blocked with zero calls made, there's one idempotency key per message, and temporary errors retry while permanent ones don't.
  - It isn't configured anywhere, so the dispatcher still uses the log-only provider.

### Steps when you approve (all free)

1. **(you)** Create a Resend account and add CWLC's domain. Add the 3–4 DNS records Resend shows (SPF/DKIM) at your domain registrar.
2. **(you)** Create an API key and paste it into the new production project's Auth → SMTP settings:
   - host `smtp.resend.com`, port 465, user `resend`, password = the key;
   - sender e.g. `CWLC <no-reply@yourdomain>`.
3. **(me)** Paste the templates into Auth → Email Templates, then test sign-in codes to your own address only.
4. Customer/employee messages (reminders, invoices, invites) stay in test mode until you separately approve live messaging.

### What I need from you to finish this part
- Does CWLC have a domain for email (the part after the @ in your business email)? If not, buying one is a small paid decision.
