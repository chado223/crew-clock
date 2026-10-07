// Message dispatcher command line.
//   node src/cli.ts run   one pass against DB_URL (log provider; live held unless MESSAGING_LIVE=1)
//   node src/cli.ts e2e   queue + dispatch on a throwaway test company inside a transaction, then ROLLBACK
import { dispatch, pgMessagesDb } from "./dispatcher.ts";
import { emailProviderFromEnv } from "./resend.ts";

const log = (m: string) => console.log(`[messages] ${m}`);
// Production projects (keep in step with platform/production-projects.txt), plus any given at run time.
const PRODUCTION_REFS = ["iwowjrnrbjiydckhjsfi", ...(process.env.PRODUCTION_PROJECT_REFS ?? "").split(",").map((r) => r.trim()).filter(Boolean)];

/** Test runs also refuse any database that marks itself as production. */
async function refuseProductionDatabase(c: { query(sql: string): Promise<{ rows: Record<string, unknown>[] }> }) {
  const { rows } = await c.query("select enabled from private.platform_flags where key = 'production'");
  if (rows[0]?.enabled === true) fail("This database is marked as production. Refusing.");
}

function fail(msg: string): never {
  console.log(`::error title=messages::${msg}`);
  throw new Error(msg);
}

async function connect() {
  const url = process.env.DB_URL;
  if (!url) fail("DB_URL is not set");
  for (const ref of PRODUCTION_REFS) if (url.includes(ref)) fail("Refusing to run against production from this tool");
  const { default: pg } = await import("pg");
  const c = new pg.Client({ connectionString: url, ssl: process.env.DB_SSL === "off" ? false : { rejectUnauthorized: false } });
  await c.connect();
  return c;
}

async function run() {
  const c = await connect();
  try {
    // Email provider is the log provider unless EMAIL_PROVIDER=resend (not configured anywhere yet).
    const email = emailProviderFromEnv(process.env);
    const s = await dispatch(pgMessagesDb(c), { allowLive: process.env.MESSAGING_LIVE === "1", log, providers: email ? { email } : {} });
    console.log(`::notice title=messages-run::${JSON.stringify(s)}`);
  } finally {
    await c.end();
  }
}

async function e2e() {
  const c = await connect();
  try {
    await c.query("begin");
    await refuseProductionDatabase(c);
    // Only this test company may be touched: hide everything else already queued.
    await c.query("update public.messages set send_after = send_after + interval '100 years' where status = 'queued'");
    const t = (await c.query(`insert into public.tenants (name, timezone) values ('Messages E2E (rolled back)', 'America/New_York') returning id`)).rows[0]!.id;
    await c.query(`insert into public.communication_settings (tenant_id, delivery_mode, test_email, visit_reminders) values ($1, 'test', 'test-inbox@example.test', true)`, [t]);
    const cl = (await c.query(`insert into public.clients (tenant_id, name, email) values ($1, 'E2E Customer', 'real-customer@example.test') returning id`, [t])).rows[0]!.id;
    const p = (await c.query(`insert into public.properties (tenant_id, client_id, address_line1, city) values ($1, $2, '1 Test Ln', 'Seymour') returning id`, [t, cl])).rows[0]!.id;
    const j = (await c.query(`insert into public.jobs (tenant_id, client_id, property_id, title) values ($1, $2, $3, 'Mow') returning id`, [t, cl, p])).rows[0]!.id;
    await c.query(`insert into public.visits (tenant_id, job_id, scheduled_date) values ($1, $2, private.tenant_today($1) + 1)`, [t, j]);

    const s = await dispatch(pgMessagesDb(c), { log });
    const { rows } = await c.query(
      `select m.status, m.mode, m.delivered_to, m.to_address, m.provider,
              (select string_agg(event, ',' order by id) from public.message_events e where e.message_id = m.id) as events
         from public.messages m where m.tenant_id = $1`, [t]);
    log(JSON.stringify(rows));
    const r = rows[0];
    if (rows.length !== 1 || !r) fail(`expected one reminder, got ${rows.length}`);
    if (r.status !== "sent" || r.provider !== "log") fail(`reminder not dispatched: ${r.status}/${r.provider}`);
    if (r.mode !== "test" || r.delivered_to !== "test-inbox@example.test") fail("test mode did not redirect to the test inbox");
    if (r.to_address !== "real-customer@example.test") fail("intended recipient not recorded");
    if (r.events !== "queued,sent") fail(`unexpected delivery history ${r.events}`);
    console.log(`::notice title=messages-e2e::reminder queued by workflow, redirected to test inbox, dispatched by log provider (${s.sent} sent); rolled back`);
  } finally {
    await c.query("rollback").catch(() => {});
    await c.end();
  }
}

const cmd = process.argv[2];
(cmd === "run" ? run() : cmd === "e2e" ? e2e() : Promise.reject(new Error("usage: cli.ts run | e2e"))).catch((e) => {
  console.error(e instanceof Error ? e.message : e);
  process.exit(1);
});
