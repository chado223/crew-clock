-- READ-ONLY export of Crew Clock's rows from the CURRENT (shared) production
-- project, for the move to a dedicated project. DO NOT RUN WITHOUT CHAD'S APPROVAL.
--
-- Changes nothing: the whole run is a READ ONLY transaction, so Postgres itself
-- refuses any write. Output is one JSON document (rows + a manifest of counts
-- and money totals) that ops/0004 imports and verifies against.
--
--   psql "$OLD_DB_URL" -X -At -v ON_ERROR_STOP=1 -f 0003_export_for_dedicated_project.sql > crew-clock-export.json
--
-- Not exported, on purpose:
--   * the other app's data (scenarios, profiles.is_pro / stripe_customer_id)
--   * auth.users — sign-in is by emailed code, so people simply sign in to the
--     new project; Chad's owner link (ops/0001) runs after his first sign-in.

\set QUIET 1
\pset tuples_only on
\pset format unaligned
begin transaction isolation level repeatable read read only;

with
  t  as (select coalesce(jsonb_agg(to_jsonb(x) order by x.id), '[]') j, count(*) n from public.tenants x),
  c  as (select coalesce(jsonb_agg(to_jsonb(x) order by x.id), '[]') j, count(*) n from public.clients x),
  jb as (select coalesce(jsonb_agg(to_jsonb(x) order by x.id), '[]') j, count(*) n from public.jobs x),
  i  as (select coalesce(jsonb_agg(to_jsonb(x) order by x.id), '[]') j, count(*) n, coalesce(sum(total), 0) s from public.invoices x),
  e  as (select coalesce(jsonb_agg(to_jsonb(x) order by x.id), '[]') j, count(*) n, coalesce(sum(amount), 0) s from public.expenses x),
  te as (select coalesce(jsonb_agg(to_jsonb(x) order by x.id), '[]') j, count(*) n from public.time_entries x),
  m  as (select coalesce(jsonb_agg(jsonb_build_object('tenant_id', x.tenant_id, 'email', u.email, 'role', x.role::text)), '[]') j, count(*) n
         from public.memberships x join auth.users u on u.id = x.user_id)
select jsonb_pretty(jsonb_build_object(
  'format', 'crew-clock-export/1',
  'exported_at', now(),
  'source_project', 'iwowjrnrbjiydckhjsfi',
  'manifest', jsonb_build_object(
    'tenants', t.n, 'clients', c.n, 'jobs', jb.n, 'invoices', i.n, 'invoice_total', i.s,
    'expenses', e.n, 'expense_total', e.s, 'time_entries', te.n, 'memberships', m.n),
  'tenants', t.j, 'clients', c.j, 'jobs', jb.j, 'invoices', i.j, 'expenses', e.j,
  'time_entries', te.j,
  -- For reference only: memberships are recreated by email after people sign in.
  'memberships', m.j))
from t, c, jb, i, e, te, m;

rollback;
