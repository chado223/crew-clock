# Web app hosting: Render (approved by Chad, 2026-10-10). Prepared, not active.

Chad approved Render's $7/month always-on instance as the host. **Nothing has been created on Render yet.** Activating needs Chad's separate go-ahead, step by step below.

The full configuration is in `platform/deploy/render-web.yaml`. It is outside the repository root, so Render never applies it by itself.

## The service

| Setting | Value | Why |
|---|---|---|
| Name / address | `crew-clock-web` → `https://crew-clock-web.onrender.com` | Temporary address until Crew Clock has its own brand and domain |
| Type / runtime | Web service, Node 22 | Next.js needs a running server (pages, imports, sign-in) |
| Region | Ohio | Same region as the `crew-clock-prod` database |
| Instance | Starter: 0.5 CPU, 512 MB, always on | Rehearsal peak was 233 MB after 100 requests |
| Branch | `web-release` (new, dedicated) | Never `main`, because `main` runs the Flask app |
| Root folder | `platform` | |
| Build | `npm ci --no-audit --no-fund && npm run build -w @crew/web` | Rehearsed: about 50 seconds. It needs about 760 MB, and builds run on Render's separate 8 GB build machines. |
| Start | `npm run start -w @crew/web` | |
| Health check | `/login` | |
| Auto-deploy | **Off.** A deploy happens only when someone presses Deploy. | Nothing goes live from a push |
| Shared with Flask service `crew-clock` | Nothing: separate service, branch, settings and address | |

## Environment variables (all public; the web app has no server secrets)

| Name | Set to |
|---|---|
| `NODE_VERSION` | 22 |
| `NEXT_TELEMETRY_DISABLED` | 1 |
| `NEXT_PUBLIC_APP_ENV` | production. The app refuses to run unless the database address is `crew-clock-prod`. |
| `NEXT_PUBLIC_SUPABASE_URL` | the `crew-clock-prod` address |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | Supabase's *publishable* key, which is public by design |
| `NEXT_PUBLIC_SITE_URL` | the service address, used in invite and message links |
| `NEXT_PUBLIC_SUPPORT_EMAIL` | `chadwashamlawncare@gmail.com` (until the brand has its own) |

Deliberately left unset:
- `NEXT_PUBLIC_LEGAL_APPROVED`: the privacy and terms pages stay marked "draft" until Chad approves them.
- `NEXT_PUBLIC_LEGAL_OPERATOR`: falls back to the default operator name.
- `ROUTING_PROVIDER`: falls back to the free built-in route estimate.

No database password, service key or email key goes on this service.

## Cost

| Item | Monthly |
|---|---|
| Starter instance | $7 |
| Render account (workspace) plan | **$0. Hobby, confirmed by Chad 2026-10-11** (Workspace Settings → Billing) |
| Traffic | 5 GB included on Hobby, then $0.15/GB. Expected $0. |
| Build minutes | 500 included; each deploy uses about 2–5. |
| **Render total** | **$7/month** (with Supabase Pro $25: about **$32/month**) |

## Rehearsal (GitHub, 2026-10-11, `web-host-rehearsal.yml`)

- The production settings compiled cleanly. The guard accepted `crew-clock-prod` with `APP_ENV=production`.
- Runtime memory peaked at **233 MB of 512 MB** over 100 requests (login, privacy, terms, and redirects to login).
- No database was touched. The production build was compiled but never started.
- The rehearsal also found that the Apps build check on `production-ops` had been failing since Oct 9. My production-project guard rejected the check's placeholder address. That is fixed and the check is green again.

## Activation steps (each needs Chad's approval)

1. ✅ **Done 2026-10-11:** Chad checked the Render workspace plan. It is **Hobby** ($0), so the $7 instance is the whole Render bill.
2. ✅ **Done 2026-10-11 (approved by Chad):** public sign-up is closed. Chad turned off Supabase → crew-clock-prod → Authentication → Sign In / Providers → "Allow new users to sign up".
   - His existing account can still sign in. Strangers can't create accounts or companies, or trigger sign-in emails.
   - Reopen it at cutover step 7 (inviting crews). **Verified 2026-10-11, production run 11 (passed):** `sign-ups open: false`; email sign-in on, auto-confirm off, phone and anonymous sign-in off.
   - The database showed still 1 user (Chad), 0 sessions, 0 pending codes, 0 sign-in events, 0 companies, 0 messages. No emails were sent, nothing was written, and nothing was deployed (Render still shows only the Flask service).
3. ✅ **Done 2026-10-11:** branch `web-release` created from `production-ops`. App code is identical to rehearsed commit `df435da`; only docs differ. No service watches it yet.
4. Create the Render service from `render-web.yaml`. Chad confirms the $7 charge in Render.
5. First deploy (manual). Check `/login` loads and that the app is pointed at `crew-clock-prod`.
6. Continue the clean-start plan: Chad signs in and creates the company (`CUTOVER_CLEAN_START.md`, step 3).

**Undo:** suspend or delete the Render service, which stops the $7 charge. The Flask app and the database are unaffected.

## Brand and domain

Chad wants Crew Clock to have its own brand and domain, separate from `chadwashamlawns.com`. That is a separate decision: choose a name and register a domain, typically about $10–20/year. Until then the app lives at the `onrender.com` address, so nothing has to be moved off `chadwashamlawns.com` later.

When the brand exists, these move to it:
- **Web address:** add the custom domain on Render (included) and update `NEXT_PUBLIC_SITE_URL`.
- **Sign-in email sender:** currently `noreply@chadwashamlawns.com` through Resend. The new domain needs verifying in Resend (free).
- **Supabase Site URL:** currently `https://chadwashamlawns.com`.
- **Support email, legal operator name, and the email template footer.**
- **The phone app's name and icon.**
