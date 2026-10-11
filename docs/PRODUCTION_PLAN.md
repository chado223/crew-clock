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
| Other app (`scenarios`, `is_pro`, `stripe_customer_id`) | Lives beside customer data forever; every migration has to tiptoe around it | None of its data. Its empty table and two profile columns come along in the baseline migration; they can be dropped later with a small migration. |
| Risk to existing data | Migrations run on the only copy | Old project is never written to; it stays as a frozen archive |
| Rollback | Emergency revert script (tested, but it's surgery) | Rebuild the new project (rehearsed, ~30 min); the old project is untouched throughout |
| Migration history | Hand-built schema + migrations on top | Clean history from day one |
| Downtime for the live clock | None (Flask app uses Sheets) | None |
| Data to move | — | 1 company, 1 customer, 1 job, 1 invoice ($350), 4 expenses ($258.50). No time history (that's in the Sheet). |

There's very little production data, which makes this the cheapest moment to separate.

### Expected costs

| Item | Monthly | Notes |
|---|---|---|
| Supabase Pro, new CWLC org | **$25** | Includes $10 compute credit, which covers the one Micro database. 8 GB disk, 250 GB transfer, 100k monthly sign-ins, daily backups kept 7 days, never pauses. |
| Old org (shared project + staging) | $0 | Stays on Free. Free orgs get 2 active projects, which is exactly what's left there. Free projects pause after a week without activity; waking staging is one free click. |
| Sign-in email (Resend free tier) | $0 | 3,000 emails/month, 100/day: plenty for sign-in codes. Once you approve live customer messages (reminders, invoices), budget **$20/month** for Resend Pro (50,000/month), because 100/day runs out quickly. See Part 3. |
| Sending domain | $0 if CWLC already owns one | Otherwise about $10–15/year. |
| Off-site encrypted backups (private GitHub repo) | $0 | Kept as release files, not commits, so old ones can be deleted; within GitHub's free limits at this size. See Part 2. |
| **Total to start** | **≈ $25/month** | |

Alternatives priced for comparison:
- Upgrade the existing org to Pro instead: all three projects bill compute, so about **$45/month** ($25 + two extra Micro databases at ~$10), and the other app ends up on your paid plan.
- Point-in-time recovery (restore to any minute): about $100/month and needs a bigger database. **Not recommended until there are paying customers**; daily backups plus our nightly encrypted dump cover a one-company launch.
- Custom domain for the sign-in service: ~$10/month. Not needed: sign-in is by typed code, so customers never see the Supabase address.
- Overage: Pro includes 8 GB of database disk; beyond that it's billed per GB. Leave Supabase's **spend cap on** (the default) so nothing can bill beyond $25 without you choosing it. At CWLC's size we'd use well under 1 GB for years; photos are the thing to watch (they're shrunk on the phone to about 300 KB each).
- Supabase limits sign-in emails to about 30 an hour by default even with your own mail service; I raise that setting at cutover (free).

Not part of this decision (still waiting separately): web hosting plan, Apple Developer account ($99/year), route optimization, SMS.

### Migration plan

Every step below needs your go-ahead once; steps marked **(you)** need your hands because they involve billing or your accounts.

| # | Step | Who | Reversible? |
|---|---|---|---|
| 0 | Back up the old project and the Sheet (Part 2). Nothing proceeds until both backups are verified. | me + you | n/a (read-only) |
| 1 | Create the CWLC org, choose Pro, add the card. | **you** (2 min) | Delete org |
| 2 | Create `crew-clock-prod` in us-east-2 in that org. | me (after 1) | Delete project |
| 3 | Apply every migration with `scripts/apply_migrations.sh`, the same script and fingerprints staging uses. It checks every file for changes before applying anything. Then mark the database as production and add its id to `platform/production-projects.txt`, so no test tool or staging job can ever run against it. | me | Delete project |
| 4 | Turn on the dashboard settings from the reconciliation report: leaked-password protection, a second MFA option, current Postgres patch level. | me | Yes |
| 5 | Export from the old project with `ops/0003_export_for_dedicated_project.sql`. It runs in a **read-only transaction** — Postgres itself refuses any write. Also confirm with a read-only count that the old project has no stored files (photos); it had none at inspection. The export holds customer contact details, so it's handled like the backups: never committed, deleted after the move. | me, read-only | n/a |
| 6 | Import into the new project with `ops/0004_import_into_dedicated_project.sql`. It keeps every original id, date and amount. It refuses: any database that isn't a fully migrated, empty Crew Clock project (so the old project is refused by design, not by luck); and any exported column the new schema doesn't have (named in the error, never silently dropped). It checks row counts and money totals, and it's all-or-nothing. | me | Re-create project |
| 7 | You sign in to the new project once (email code). Then `ops/0001_link_production_owner.sql` makes `chadwasham64@gmail.com` the owner. | **you** + me | Yes |
| 8 | Point the web and mobile app settings at the new project's URL and public key. | me | See Rollback (the old project can't run the new app) |
| 9 | Run the end-to-end checks against production as you (sign in, see the company, customer, invoice and expenses; clock in/out on a test employee and void it). | me + you | Voids are audited, nothing deleted |
| 10 | Old project stays as-is, read-only by habit, for at least 90 days. Retiring it is a separate decision. | — | — |

The old project's second login (`chadwasham@gmail.com`) has no membership and isn't moved. Anyone else just signs in to the new project with their email.

### Data preservation

- The old project is never written to. The export is read-only; the import refuses anything that isn't a freshly migrated Crew Clock project.
- Original ids, dates and amounts are kept. Every exported column must exist in the new schema, or the import stops and names it.
- Two columns the old schema never had are filled from existing data, and nothing else is changed:
  - The $350 invoice was marked sent, so its "sent" date is set to its issue date. Without this it would never count as billed.
  - The customer's "Added as a customer" history line is dated from when the customer was created. Without this it would show the move date.
- The old invoice has no invoice number, and new numbers start at INV-1001. If you want it numbered, that's a one-line decision.
- The import checks counts and totals against the export manifest and keeps nothing if they disagree.
- Running the import twice adds nothing.
- The other app's data stays where it is and is never copied.
- **Rehearsed in CI on every change** (`tests/move/check_move.sh`): it builds a copy of the current production database (schema plus its real rows) and a fresh project from the migrations, then runs the export and import. It proves:
  - the old copy is byte-for-byte unchanged;
  - all 8 rows arrive with identical values;
  - a re-run adds nothing;
  - the import refuses the old project even when the other app's table is empty, as it really is;
  - an unknown column is refused by name;
  - the legacy invoice counts as billed;
  - the owner link works afterwards.
- The new project is built in that rehearsal with the real migration script, not a shortcut.

### Rollback

Plainly: the old project can't run the new app (it has the old hand-built schema), so "rollback" never means switching the new app back to it. The old project is a frozen archive of the original data.

- **Before step 8** (apps not yet pointed at the new project): nothing to undo. Delete the new project if you like; production is exactly as it was.
- **After step 8**, if something is wrong with the new project:
  1. Export anything entered since cutover (**Import & export → Export**: customers, jobs, invoices, payments, expenses, time as CSV).
  2. Fix forward with a new migration, or delete and rebuild the project from steps 2–7. That's a rehearsed process, about 30 minutes, then re-enter or re-import the exported rows.
  3. Supabase Pro's daily backups (7 days) can also restore the new project to any of the last 7 nights.
- Today this risk is small: you're the only user of the new app, and the crews keep using the Flask clock and the Sheet until you decide otherwise.

The live Flask clock and the Google Sheet are not involved at any step, so field crews are never affected.

---

## Part 2 — Backups of the existing production database and the Sheet

No source is modified or deleted. Both procedures only read.

### A. Existing production database (`iwowjrnrbjiydckhjsfi`)

Goal: a complete, restorable copy before anything else happens. It covers the other app's data too, so nothing can be lost by accident.

1. **Private place for backups** (you, 2 minutes): create a **private** GitHub repo, `chado223/crew-clock-backups`. This code repository is **public**, so production data never goes into it or its CI files (any signed-in GitHub user can download those).
2. **Your encryption key** (you + me, 5 minutes): you generate an `age` key pair on your computer. I give you the one command. The private half stays with you, offline; only the public half goes into the backups repo. Nobody else, me included, can open a backup.
3. **The database password stays yours**: you paste the old project's connection string (the "session pooler" one, which works from GitHub's servers) into the backups repo's secrets. I never see it, and it never touches the public repo.
4. **One backup job, run in the private repo**, in this order:
   - Dump every schema that holds data: `public`, `private`, `auth` (sign-ins, which other tables point to), `storage` and the migration history. The dump only reads.
   - Restore it into a throwaway database on the same runner and compare row counts table by table (and invoice/expense totals) against the live source, before anything is encrypted.
   - Encrypt with your public key and attach it to a dated release. Old releases can be deleted, so storage doesn't grow forever. Keep 30.
   - Stored photo files aren't inside a database dump, so the job also copies the `visit-photos` bucket. The old project has none today; the new one will.
5. **Rehearsal:** the same dump → restore → compare steps run on staging (`staging-backup.yml`) on every change to the database. They upload nothing, because staging lives in the public repo. Until now that job only ran from `main`, so it never ran; it now runs on the work branches too.
6. After the move, Supabase Pro's daily backups (7 days) are the first line of defense and this job is the second, off-site copy.

### B. Google Sheets time history

The Sheet is the **only** record of past hours: the old SQLite file is wiped whenever Render's free service sleeps, and the shared Supabase project has no time entries.

1. **Copy inside Google** (you, 1 minute): open the Sheet → File → Make a copy → name it `Crew Clock time history — archive YYYY-MM-DD` → put it in a folder only you can edit. The original keeps working; the Flask app keeps writing to it.
2. **Download** (you, 1 minute): File → Download → Microsoft Excel (.xlsx). That one file holds every tab: weekly tabs, totals tabs and the all-weeks summary.
3. **Fingerprint** (me, once you attach the .xlsx): `python3 platform/tools/sheets_manifest.py file.xlsx` opens it read-only and records:
   - the file's SHA-256;
   - every tab and its row count;
   - hours per crew per week by the old app's own rule (the hours on clock-out rows), so the numbers match its Totals tabs;
   - every clock-out with no hours (its clock-in was lost when the old server slept), listed for review instead of skipped.

   That manifest is the yardstick for any future import.
4. Store the .xlsx and manifest in the private backups repo next to the database dump.
5. **Importing that history into the new app is a separate, later step that needs its own approval.**
   - The schema is ready: `time_entries.source = 'legacy_sheets'` plus `legacy_ref`, which records the tab and row.
   - Crew names in the Sheet ("Crew 1") need mapping to employees, which you'd confirm.
   - Rows that don't pair up cleanly get flagged for review, not guessed.
   - Totals must match the manifest week by week. One rule to settle at import: the Sheet files a shift under the week it *ended*, while the new app counts a shift in the week it *started*. That only differs for shifts crossing midnight on Sunday.

### Status: done (2026-10-10)

- **Old database:** read on 2026-10-07 (`rows.json`, `schema-live.json`). It was rebuilt into a throwaway database and every table and money total matched ("RESTORE VERIFIED").
- **Sheet:** Chad made a private Drive copy, "Crew Clock time history — archive 2026-10-10". The .xlsx (sha256 `db21bbb6…d287b`) was fingerprinted read-only.
  - Total recorded: 0.04 hours, which matches the Sheet's own Totals tabs.
  - Clock-ins left open: chad ×2, clint and aiden fowler.
  - The rest is test punches; there is no real payroll history.
- **Off-site copy:** stored in private `chado223/crew-clock-backups`.
  - All four files are age-encrypted to Chad's public key; the private key stays on his computer and a USB drive.
  - A guard workflow rejects any unencrypted file or private key.
- **Recovery test:** Chad downloaded the repo and ran `verify.ps1` on Windows. All four files unlocked and their fingerprints matched.
- **Not built yet:** the recurring job in step 4. The old project is frozen, so the one-time copy covers it. A recurring off-site backup of the **new** project comes after cutover; until then, Supabase Pro daily backups cover it.

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

### Settings (decided 2026-10-07)
- Sending domain: **chadwashamlawns.com** (verified in Resend with the DNS records it shows).
- Sign-in emails from: `Chad Washam Lawncare <noreply@chadwashamlawns.com>`.
- Replies and contact: `chadwashamlawncare@gmail.com`.
- Supabase Auth → SMTP: host `smtp.resend.com`, port `465`, user `resend`, password = the Resend API key (pasted by Chad), sender name `Chad Washam Lawncare`, sender email `noreply@chadwashamlawns.com`.
- Auth email rate limit raised from the default (about 30/hour) to 100/hour, which matches Resend's free daily allowance.
- Customer/employee messages: still test mode, all three locks on.
