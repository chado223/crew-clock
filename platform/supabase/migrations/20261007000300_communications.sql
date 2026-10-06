-- Communications: templates, preferences, an outbox with delivery records, and
-- the estimate / invoice / reminder / invite workflows.
--
-- Safety model (no real messages until the owner approves):
--   * Every company has a delivery mode: 'off', 'test' (default) or 'live'.
--   * In 'test' mode every message is redirected to the company's test
--     recipients; the intended address is kept for review but never used.
--   * 'live' can only be switched on after the platform owner enables live
--     messaging (private.platform_flags, not reachable from the app), AND the
--     dispatcher refuses live messages unless its own environment allows them.
--   * The app never sends directly. It queues; a server-side dispatcher with a
--     provider (email/SMS vendor, chosen later) delivers and reports back.

-- ---------------------------------------------------------------------------
-- Platform switch (owner-only, via SQL with approval)
-- ---------------------------------------------------------------------------
create table if not exists private.platform_flags (
  key text primary key,
  enabled boolean not null default false,
  note text,
  updated_at timestamptz not null default now()
);
insert into private.platform_flags (key, enabled, note)
values ('live_messaging', false, 'Real email/SMS to customers and employees. Requires owner approval.')
on conflict (key) do nothing;
revoke all on private.platform_flags from public, anon, authenticated;

create or replace function private.flag(p_key text) returns boolean
language sql stable security definer set search_path = '' as $$
  select coalesce((select enabled from private.platform_flags where key = p_key), false)
$$;

-- ---------------------------------------------------------------------------
-- Company settings
-- ---------------------------------------------------------------------------
create table if not exists public.communication_settings (
  tenant_id uuid primary key references public.tenants (id) on delete restrict,
  delivery_mode text not null default 'test' check (delivery_mode in ('off','test','live')),
  test_email text check (test_email is null or test_email ~* '^[^@\s]+@[^@\s]+\.[^@\s]+$'),
  test_phone text check (test_phone is null or test_phone ~ '^\+?[0-9]{10,15}$'),
  from_name text check (from_name is null or length(from_name) <= 80),
  reply_to text check (reply_to is null or reply_to ~* '^[^@\s]+@[^@\s]+\.[^@\s]+$'),
  portal_url text check (portal_url is null or portal_url ~ '^https://'),
  visit_reminders boolean not null default false,
  invoice_reminders boolean not null default false,
  invoice_reminder_days integer not null default 7 check (invoice_reminder_days between 1 and 60),
  updated_by uuid references auth.users (id) on delete set null,
  updated_at timestamptz not null default now()
);

create or replace function private.communication_settings_guard() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  if new.delivery_mode = 'live' and (tg_op = 'INSERT' or old.delivery_mode <> 'live') and not private.flag('live_messaging') then
    raise exception 'live_messaging_not_enabled' using errcode = '42501';
  end if;
  new.updated_by := coalesce(auth.uid(), new.updated_by);
  return new;
end $$;
drop trigger if exists communication_settings_guard on public.communication_settings;
create trigger communication_settings_guard before insert or update on public.communication_settings
  for each row execute function private.communication_settings_guard();

-- ---------------------------------------------------------------------------
-- Templates: built-in defaults, overridable per company
-- ---------------------------------------------------------------------------
create table if not exists public.message_templates (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  template_key text not null,
  channel text not null check (channel in ('email','sms')),
  subject text check (subject is null or length(subject) <= 200),
  body text not null check (length(body) between 1 and 5000),
  active boolean not null default true,
  updated_at timestamptz not null default now(),
  unique (tenant_id, template_key, channel),
  unique (tenant_id, id)
);

create or replace function public.default_message_templates()
returns table (template_key text, channel text, label text, subject text, body text, variables text[])
language sql immutable set search_path = '' as $$
  values
  ('estimate_sent', 'email', 'Estimate sent',
   'Estimate {{estimate_number}} from {{company_name}}',
   E'Hi {{customer_name}},\n\nHere is your estimate {{estimate_number}} for {{estimate_total}}.\n\n{{portal_line}}\n\nThank you,\n{{company_name}}',
   array['customer_name','company_name','estimate_number','estimate_total','valid_until','portal_line']),
  ('estimate_sent', 'sms', 'Estimate sent', null,
   '{{company_name}}: your estimate {{estimate_number}} ({{estimate_total}}) is ready. {{portal_line}}',
   array['customer_name','company_name','estimate_number','estimate_total','portal_line']),
  ('invoice_sent', 'email', 'Invoice sent',
   'Invoice {{invoice_number}} from {{company_name}}',
   E'Hi {{customer_name}},\n\nInvoice {{invoice_number}} for {{invoice_total}} is due {{due_date}}.\n\n{{portal_line}}\n\nThank you for your business,\n{{company_name}}',
   array['customer_name','company_name','invoice_number','invoice_total','balance_due','due_date','portal_line']),
  ('invoice_sent', 'sms', 'Invoice sent', null,
   '{{company_name}}: invoice {{invoice_number}} for {{invoice_total}} is due {{due_date}}. {{portal_line}}',
   array['company_name','invoice_number','invoice_total','due_date','portal_line']),
  ('invoice_reminder', 'email', 'Payment reminder',
   'Reminder: invoice {{invoice_number}} from {{company_name}}',
   E'Hi {{customer_name}},\n\nA friendly reminder that invoice {{invoice_number}} has {{balance_due}} due (due {{due_date}}).\n\n{{portal_line}}\n\nIf you already paid, thank you and please ignore this.\n{{company_name}}',
   array['customer_name','company_name','invoice_number','balance_due','due_date','portal_line']),
  ('invoice_reminder', 'sms', 'Payment reminder', null,
   '{{company_name}}: reminder, invoice {{invoice_number}} has {{balance_due}} due. {{portal_line}}',
   array['company_name','invoice_number','balance_due','portal_line']),
  ('visit_reminder', 'email', 'Visit reminder',
   'Service scheduled {{visit_date}}',
   E'Hi {{customer_name}},\n\n{{company_name}} is scheduled for {{service}} at {{address}} on {{visit_date}}.\n\nPlease leave gates unlocked and pets inside. Thank you!',
   array['customer_name','company_name','service','address','visit_date']),
  ('visit_reminder', 'sms', 'Visit reminder', null,
   '{{company_name}}: we''re scheduled for {{service}} at {{address}} on {{visit_date}}. Please leave gates unlocked.',
   array['company_name','service','address','visit_date']),
  ('visit_rescheduled', 'email', 'Visit moved',
   'Your service has moved to {{visit_date}}',
   E'Hi {{customer_name}},\n\nWe moved your {{service}} at {{address}} to {{visit_date}}{{reason_line}}.\n\nThank you,\n{{company_name}}',
   array['customer_name','company_name','service','address','visit_date','reason_line']),
  ('visit_rescheduled', 'sms', 'Visit moved', null,
   '{{company_name}}: your {{service}} moved to {{visit_date}}{{reason_line}}.',
   array['company_name','service','visit_date','reason_line']),
  ('portal_invite', 'email', 'Customer account invite',
   'Your {{company_name}} account',
   E'Hi {{customer_name}},\n\nSee your visits, estimates and invoices online:\n{{invite_link}}\n\n{{company_name}}',
   array['customer_name','company_name','invite_link']),
  ('team_invite', 'email', 'Team invite',
   'Join {{company_name}} on Crew Clock',
   E'You''ve been invited to join {{company_name}}.\n\nOpen this link on your phone to get started:\n{{invite_link}}',
   array['company_name','invite_link'])
$$;

-- ---------------------------------------------------------------------------
-- Customer contact preferences
-- ---------------------------------------------------------------------------
create table if not exists public.contact_preferences (
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  client_id uuid not null,
  email_ok boolean not null default true,
  sms_ok boolean not null default false,            -- texts need consent
  sms_consent_at timestamptz,
  sms_consent_source text,                          -- 'portal', 'office (verbal)', 'signed form', ...
  kinds_off text[] not null default '{}',           -- e.g. {visit_reminder}
  unsubscribed_at timestamptz,                      -- stop everything
  updated_at timestamptz not null default now(),
  updated_by uuid references auth.users (id) on delete set null,
  primary key (client_id),
  foreign key (tenant_id, client_id) references public.clients (tenant_id, id),
  check (not sms_ok or sms_consent_at is not null)
);

-- ---------------------------------------------------------------------------
-- Outbox / history and delivery records
-- ---------------------------------------------------------------------------
create table if not exists public.messages (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  client_id uuid,
  employee_id uuid,
  subject_type text check (subject_type in ('estimate','invoice','visit','portal_invitation','invitation')),
  subject_id uuid,
  template_key text not null,
  channel text not null check (channel in ('email','sms')),
  mode text not null check (mode in ('test','live')),
  to_address text,                 -- the real recipient (never used in test mode)
  delivered_to text,               -- where it actually goes
  subject text,
  body text not null,
  status text not null default 'queued'
    check (status in ('queued','sending','sent','delivered','failed','suppressed','canceled')),
  suppressed_reason text,
  provider text,
  provider_message_id text,
  error text,
  attempts integer not null default 0,
  idempotency_key text,
  send_after timestamptz not null default now(),
  created_by uuid references auth.users (id) on delete set null,
  created_at timestamptz not null default now(),
  sent_at timestamptz,
  updated_at timestamptz not null default now(),
  unique (tenant_id, id),
  unique (tenant_id, idempotency_key),
  foreign key (tenant_id, client_id) references public.clients (tenant_id, id),
  foreign key (tenant_id, employee_id) references public.employees (tenant_id, id)
);
create index if not exists messages_tenant_created_idx on public.messages (tenant_id, created_at desc);
create index if not exists messages_client_idx on public.messages (client_id, created_at desc);
create index if not exists messages_queue_idx on public.messages (send_after) where status = 'queued';

create table if not exists public.message_events (
  id bigint generated always as identity primary key,
  tenant_id uuid not null,
  message_id uuid not null,
  event text not null check (event in ('queued','suppressed','sending','sent','delivered','bounced','failed','complained','canceled')),
  detail jsonb,
  occurred_at timestamptz not null default clock_timestamp(),
  foreign key (tenant_id, message_id) references public.messages (tenant_id, id) on delete cascade
);
create index if not exists message_events_message_idx on public.message_events (message_id, id);

-- Generic triggers
do $$
declare t text;
begin
  foreach t in array array['communication_settings','message_templates','contact_preferences','messages'] loop
    execute format('drop trigger if exists set_updated_at on public.%I', t);
    execute format('create trigger set_updated_at before update on public.%I for each row execute function private.set_updated_at()', t);
  end loop;
  foreach t in array array['communication_settings','message_templates','contact_preferences','messages','message_events'] loop
    execute format('drop trigger if exists prevent_tenant_change on public.%I', t);
    execute format('create trigger prevent_tenant_change before update of tenant_id on public.%I for each row execute function private.prevent_tenant_change()', t);
  end loop;
  foreach t in array array['communication_settings','message_templates','contact_preferences'] loop
    execute format('drop trigger if exists audit_row_change on public.%I', t);
    execute format('create trigger audit_row_change after insert or update or delete on public.%I for each row execute function private.audit_row_change()', t);
  end loop;
end $$;

create or replace function private.contact_preferences_stamp() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  new.updated_by := coalesce(auth.uid(), new.updated_by);
  if new.sms_ok and (tg_op = 'INSERT' or not old.sms_ok) and new.sms_consent_at is null then
    new.sms_consent_at := now();
  end if;
  return new;
end $$;
drop trigger if exists contact_preferences_stamp on public.contact_preferences;
create trigger contact_preferences_stamp before insert or update on public.contact_preferences
  for each row execute function private.contact_preferences_stamp();

-- ---------------------------------------------------------------------------
-- RLS: office only. Customers reach their own messages/preferences through portal_*.
-- ---------------------------------------------------------------------------
alter table public.communication_settings enable row level security;
alter table public.message_templates enable row level security;
alter table public.contact_preferences enable row level security;
alter table public.messages enable row level security;
alter table public.message_events enable row level security;

do $$
declare t text;
begin
  foreach t in array array['communication_settings','message_templates','contact_preferences'] loop
    execute format('drop policy if exists %I on public.%I', t || '_select', t);
    execute format('drop policy if exists %I on public.%I', t || '_insert', t);
    execute format('drop policy if exists %I on public.%I', t || '_update', t);
    execute format('create policy %I on public.%I for select to authenticated using (public.is_admin_or_owner(tenant_id))', t || '_select', t);
    execute format('create policy %I on public.%I for insert to authenticated with check (public.is_admin_or_owner(tenant_id))', t || '_insert', t);
    execute format('create policy %I on public.%I for update to authenticated using (public.is_admin_or_owner(tenant_id)) with check (public.is_admin_or_owner(tenant_id))', t || '_update', t);
  end loop;
  foreach t in array array['messages','message_events'] loop
    execute format('drop policy if exists %I on public.%I', t || '_select', t);
    execute format('create policy %I on public.%I for select to authenticated using (public.is_admin_or_owner(tenant_id))', t || '_select', t);
  end loop;
end $$;

revoke all on public.communication_settings, public.message_templates, public.contact_preferences,
  public.messages, public.message_events from anon, authenticated;
grant select on public.communication_settings, public.message_templates, public.contact_preferences,
  public.messages, public.message_events to authenticated;
grant insert (tenant_id, delivery_mode, test_email, test_phone, from_name, reply_to, portal_url, visit_reminders, invoice_reminders, invoice_reminder_days),
  update (tenant_id, delivery_mode, test_email, test_phone, from_name, reply_to, portal_url, visit_reminders, invoice_reminders, invoice_reminder_days)
  on public.communication_settings to authenticated;
grant insert (tenant_id, template_key, channel, subject, body, active),
  update (tenant_id, subject, body, active) on public.message_templates to authenticated;
grant insert (tenant_id, client_id, email_ok, sms_ok, sms_consent_source, kinds_off, unsubscribed_at),
  update (tenant_id, email_ok, sms_ok, sms_consent_source, kinds_off, unsubscribed_at) on public.contact_preferences to authenticated;
grant all on public.communication_settings, public.message_templates, public.contact_preferences,
  public.messages, public.message_events to service_role;

-- Template keys must be ones the system knows.
create or replace function private.message_template_check() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  if not exists (select 1 from public.default_message_templates() d where d.template_key = new.template_key and d.channel = new.channel) then
    raise exception 'invalid_template' using errcode = '22023';
  end if;
  if new.channel = 'email' and coalesce(btrim(new.subject), '') = '' then
    raise exception 'subject_required' using errcode = '22023';
  end if;
  return new;
end $$;
drop trigger if exists message_template_check on public.message_templates;
create trigger message_template_check before insert or update on public.message_templates
  for each row execute function private.message_template_check();

-- ---------------------------------------------------------------------------
-- Rendering and queueing
-- ---------------------------------------------------------------------------
create or replace function private.render_template(p_text text, p_vars jsonb) returns text
language plpgsql immutable set search_path = '' as $$
declare k text; v text; r text := p_text;
begin
  if r is null then return null; end if;
  for k, v in select key, value from jsonb_each_text(coalesce(p_vars, '{}')) loop
    r := replace(r, '{{' || k || '}}', coalesce(v, ''));
  end loop;
  return regexp_replace(r, '\{\{[a-z_]+\}\}', '', 'g');   -- unknown placeholders vanish
end $$;

create or replace function private.money_text(n numeric) returns text
language sql immutable set search_path = '' as $$ select '$' || to_char(coalesce(n, 0), 'FM999,999,990.00') $$;

create or replace function private.message_event(p_message public.messages, p_event text, p_detail jsonb default null)
returns void language sql security definer set search_path = '' as $$
  insert into public.message_events (tenant_id, message_id, event, detail) values (p_message.tenant_id, p_message.id, p_event, p_detail)
$$;

-- The one queueing path. Everything (staff buttons, workflows, reminders) goes through here.
-- Returns the message id (existing one when the idempotency key was already used).
create or replace function private.enqueue_message(
  p_tenant_id uuid,
  p_template_key text,
  p_channel text,
  p_client_id uuid,
  p_employee_id uuid,
  p_to text,
  p_subject_type text,
  p_subject_id uuid,
  p_vars jsonb,
  p_idempotency_key text,
  p_send_after timestamptz default null
) returns uuid
language plpgsql security definer set search_path = '' as $$
declare
  s public.communication_settings;
  pref public.contact_preferences;
  c public.clients;
  t record;
  v_vars jsonb;
  v_to text := nullif(btrim(coalesce(p_to, '')), '');
  v_status text := 'queued';
  v_reason text;
  v_delivered text;
  m public.messages;
begin
  if p_idempotency_key is not null then
    select * into m from public.messages where tenant_id = p_tenant_id and idempotency_key = p_idempotency_key;
    if found then return m.id; end if;
  end if;

  select * into s from public.communication_settings where tenant_id = p_tenant_id;
  if s.tenant_id is null then s.delivery_mode := 'test'; end if;

  if p_client_id is not null then
    select * into c from public.clients where id = p_client_id and tenant_id = p_tenant_id;
    if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
    select * into pref from public.contact_preferences where client_id = p_client_id;
    v_to := coalesce(v_to, case p_channel when 'email' then nullif(btrim(c.email), '') else nullif(regexp_replace(coalesce(c.phone, ''), '[^0-9+]', '', 'g'), '') end);
  end if;

  -- Template: company override, else built-in.
  select coalesce(mt.subject, d.subject) as subject, coalesce(mt.body, d.body) as body into t
  from public.default_message_templates() d
  left join public.message_templates mt on mt.tenant_id = p_tenant_id and mt.template_key = d.template_key
    and mt.channel = d.channel and mt.active
  where d.template_key = p_template_key and d.channel = p_channel;
  if not found then raise exception 'invalid_template' using errcode = '22023'; end if;

  v_vars := jsonb_build_object(
      'company_name', (select name from public.tenants where id = p_tenant_id),
      'customer_name', coalesce(nullif(btrim(c.name), ''), 'there'))
    || coalesce(p_vars, '{}');

  -- Who may receive what
  if s.delivery_mode = 'off' then
    v_status := 'suppressed'; v_reason := 'messaging_off';
  elsif v_to is null then
    v_status := 'suppressed'; v_reason := 'no_' || p_channel || '_on_file';
  elsif pref.client_id is not null and pref.unsubscribed_at is not null then
    v_status := 'suppressed'; v_reason := 'unsubscribed';
  elsif pref.client_id is not null and p_template_key = any (pref.kinds_off) then
    v_status := 'suppressed'; v_reason := 'customer_turned_off_' || p_template_key;
  elsif p_channel = 'email' and pref.client_id is not null and not pref.email_ok then
    v_status := 'suppressed'; v_reason := 'email_not_allowed';
  elsif p_channel = 'sms' and p_client_id is not null and not coalesce(pref.sms_ok, false) then
    v_status := 'suppressed'; v_reason := 'no_sms_consent';
  end if;

  if v_status = 'queued' then
    if s.delivery_mode = 'live' and private.flag('live_messaging') then
      v_delivered := v_to;
    else
      v_delivered := case p_channel when 'email' then s.test_email else s.test_phone end;
      if v_delivered is null then v_status := 'suppressed'; v_reason := 'test_mode_no_test_recipient'; end if;
    end if;
  end if;

  insert into public.messages (tenant_id, client_id, employee_id, subject_type, subject_id, template_key, channel, mode,
                               to_address, delivered_to, subject, body, status, suppressed_reason,
                               idempotency_key, send_after, created_by)
  values (p_tenant_id, p_client_id, p_employee_id, p_subject_type, p_subject_id, p_template_key, p_channel,
          case when s.delivery_mode = 'live' and private.flag('live_messaging') then 'live' else 'test' end,
          v_to, v_delivered,
          private.render_template(t.subject, v_vars), private.render_template(t.body, v_vars),
          v_status, v_reason, p_idempotency_key, coalesce(p_send_after, now()), auth.uid())
  returning * into m;
  perform private.message_event(m, case when v_status = 'queued' then 'queued' else 'suppressed' end,
    case when v_reason is null then null else jsonb_build_object('reason', v_reason) end);
  return m.id;
end $$;

create or replace function private.portal_line(p_tenant_id uuid, p_path text) returns text
language sql stable security definer set search_path = '' as $$
  select case when s.portal_url is null then 'Sign in to your customer account to view it.'
              else 'View it online: ' || rtrim(s.portal_url, '/') || p_path end
  from (select (select portal_url from public.communication_settings where tenant_id = p_tenant_id) as portal_url) s
$$;

-- ---------------------------------------------------------------------------
-- Staff actions
-- ---------------------------------------------------------------------------

-- Send an estimate: marks it sent (if still a draft) and queues the message.
create or replace function public.send_estimate(p_estimate_id uuid, p_channel text default 'email') returns uuid
language plpgsql security definer set search_path = '' as $$
declare e public.estimates; v_id uuid;
begin
  select * into e from public.estimates where id = p_estimate_id;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(e.tenant_id);
  if p_channel not in ('email','sms') then raise exception 'invalid_channel' using errcode = '22023'; end if;
  if e.status = 'draft' then e := public.set_estimate_status(p_estimate_id, 'sent'); end if;
  if e.status not in ('sent','approved','declined') then raise exception 'estimate_not_open' using errcode = '22023'; end if;
  v_id := private.enqueue_message(e.tenant_id, 'estimate_sent', p_channel, e.client_id, null, null, 'estimate', e.id,
    jsonb_build_object('estimate_number', e.number, 'estimate_total', private.money_text(e.subtotal),
                       'valid_until', to_char(e.valid_until, 'Mon FMDD, YYYY'),
                       'portal_line', private.portal_line(e.tenant_id, '/portal/estimates/' || e.id)),
    null);
  perform private.log_activity(e.tenant_id, e.client_id, e.property_id, 'message',
    format('Estimate %s %s to customer%s', e.number, case p_channel when 'email' then 'emailed' else 'texted' end,
           (select case when status = 'suppressed' then ' (not delivered: ' || replace(suppressed_reason, '_', ' ') || ')'
                        when mode = 'test' then ' (test mode)' else '' end from public.messages where id = v_id)),
    jsonb_build_object('message_id', v_id, 'estimate_id', e.id));
  return v_id;
end $$;

create or replace function public.send_invoice(p_invoice_id uuid, p_channel text default 'email') returns uuid
language plpgsql security definer set search_path = '' as $$
declare inv public.invoices; v_id uuid;
begin
  select * into inv from public.invoices where id = p_invoice_id;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(inv.tenant_id);
  if p_channel not in ('email','sms') then raise exception 'invalid_channel' using errcode = '22023'; end if;
  if inv.status = 'draft' then inv := public.mark_invoice_sent(p_invoice_id); end if;
  if inv.status in ('void') then raise exception 'invoice_not_open' using errcode = '22023'; end if;
  v_id := private.enqueue_message(inv.tenant_id, 'invoice_sent', p_channel, inv.client_id, null, null, 'invoice', inv.id,
    jsonb_build_object('invoice_number', inv.number, 'invoice_total', private.money_text(inv.total),
                       'balance_due', private.money_text(inv.total - inv.amount_paid),
                       'due_date', coalesce(to_char(inv.due_at at time zone 'UTC', 'Mon FMDD'), 'on receipt'),
                       'portal_line', private.portal_line(inv.tenant_id, '/portal/invoices')),
    null);
  perform private.log_activity(inv.tenant_id, inv.client_id, null, 'message',
    format('Invoice %s %s to customer%s', coalesce(inv.number, ''), case p_channel when 'email' then 'emailed' else 'texted' end,
           (select case when status = 'suppressed' then ' (not delivered: ' || replace(suppressed_reason, '_', ' ') || ')'
                        when mode = 'test' then ' (test mode)' else '' end from public.messages where id = v_id)),
    jsonb_build_object('message_id', v_id, 'invoice_id', inv.id));
  return v_id;
end $$;

-- Invite links exist only at creation, so the app passes the link it built.
create or replace function public.send_invite_message(
  p_tenant_id uuid, p_kind text, p_to text, p_link text, p_client_id uuid default null
) returns uuid
language plpgsql security definer set search_path = '' as $$
begin
  perform private.require_manager(p_tenant_id);
  if p_kind not in ('portal_invite','team_invite') then raise exception 'invalid_template' using errcode = '22023'; end if;
  if p_link is null or p_link !~ '^(https://|crew://)' then raise exception 'invalid_link' using errcode = '22023'; end if;
  if p_to is null or p_to !~* '^[^@\s]+@[^@\s]+\.[^@\s]+$' then raise exception 'invalid_email' using errcode = '22023'; end if;
  if p_client_id is not null and not exists (select 1 from public.clients where id = p_client_id and tenant_id = p_tenant_id) then
    raise exception 'not_found' using errcode = 'P0002';
  end if;
  return private.enqueue_message(p_tenant_id, p_kind, 'email', case when p_kind = 'portal_invite' then p_client_id end, null, lower(p_to),
    case p_kind when 'portal_invite' then 'portal_invitation' else 'invitation' end, null,
    jsonb_build_object('invite_link', p_link), null);
end $$;

-- Cancel a message that hasn't gone out yet.
create or replace function public.cancel_message(p_message_id uuid) returns void
language plpgsql security definer set search_path = '' as $$
declare m public.messages;
begin
  select * into m from public.messages where id = p_message_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(m.tenant_id);
  if m.status <> 'queued' then raise exception 'message_not_queued' using errcode = '22023'; end if;
  update public.messages set status = 'canceled' where id = m.id returning * into m;
  perform private.message_event(m, 'canceled', jsonb_build_object('by', auth.uid()));
end $$;

-- ---------------------------------------------------------------------------
-- Automatic workflows (run by the scheduler as service_role; idempotent)
-- ---------------------------------------------------------------------------

-- Tomorrow's visit reminders for companies that turned them on.
create or replace function public.queue_visit_reminders(p_tenant_id uuid) returns integer
language plpgsql security definer set search_path = '' as $$
declare v record; n integer := 0; v_day date;
begin
  if auth.uid() is not null then perform private.require_manager(p_tenant_id); end if;
  if not coalesce((select visit_reminders from public.communication_settings where tenant_id = p_tenant_id), false) then return 0; end if;
  v_day := private.tenant_today(p_tenant_id) + 1;
  for v in
    select vi.id, vi.client_id, vi.scheduled_date, coalesce(sv.name, j.title) as service,
           concat_ws(', ', p.address_line1, p.city) as address,
           coalesce(pref.sms_ok, false) as sms_ok
    from public.visits vi
    join public.jobs j on j.id = vi.job_id
    left join public.services sv on sv.id = j.service_id
    left join public.properties p on p.id = vi.property_id
    left join public.contact_preferences pref on pref.client_id = vi.client_id
    where vi.tenant_id = p_tenant_id and vi.status = 'scheduled' and vi.scheduled_date = v_day and vi.client_id is not null
  loop
    perform private.enqueue_message(p_tenant_id, 'visit_reminder', case when v.sms_ok then 'sms' else 'email' end,
      v.client_id, null, null, 'visit', v.id,
      jsonb_build_object('service', v.service, 'address', v.address, 'visit_date', to_char(v.scheduled_date, 'Day, Mon FMDD')),
      'visit_reminder:' || v.id || ':' || v.scheduled_date);
    n := n + 1;
  end loop;
  return n;
end $$;

-- Reminders for unpaid invoices past due, at most once per reminder period.
create or replace function public.queue_invoice_reminders(p_tenant_id uuid) returns integer
language plpgsql security definer set search_path = '' as $$
declare i record; n integer := 0; s public.communication_settings;
begin
  if auth.uid() is not null then perform private.require_manager(p_tenant_id); end if;
  select * into s from public.communication_settings where tenant_id = p_tenant_id;
  if not coalesce(s.invoice_reminders, false) then return 0; end if;
  for i in
    select inv.* from public.invoices inv
    where inv.tenant_id = p_tenant_id and inv.status in ('sent','partial','overdue')
      and inv.total - inv.amount_paid > 0 and inv.due_at is not null and inv.due_at < now()
      and inv.client_id is not null
  loop
    perform private.enqueue_message(p_tenant_id, 'invoice_reminder', 'email', i.client_id, null, null, 'invoice', i.id,
      jsonb_build_object('invoice_number', i.number, 'balance_due', private.money_text(i.total - i.amount_paid),
                         'due_date', to_char(i.due_at at time zone 'UTC', 'Mon FMDD'),
                         'portal_line', private.portal_line(p_tenant_id, '/portal/invoices')),
      'invoice_reminder:' || i.id || ':' || floor(extract(epoch from now()) / (86400 * s.invoice_reminder_days))::bigint);
    n := n + 1;
  end loop;
  return n;
end $$;

-- ---------------------------------------------------------------------------
-- Dispatcher API (service_role only)
-- ---------------------------------------------------------------------------
create or replace function public.messages_worker_claim(p_limit integer default 25)
returns setof public.messages
language plpgsql security definer set search_path = '' as $$
begin
  return query
  with c as (
    select m.id from public.messages m
    where m.status = 'queued' and m.send_after <= now()
    order by m.send_after
    limit least(greatest(p_limit, 1), 200)
    for update skip locked
  )
  update public.messages m set status = 'sending', attempts = m.attempts + 1
  from c where m.id = c.id
  returning m.*;
end $$;

create or replace function public.messages_worker_result(
  p_message_id uuid, p_ok boolean, p_provider text, p_provider_message_id text default null,
  p_error text default null, p_retry boolean default false
) returns void
language plpgsql security definer set search_path = '' as $$
declare m public.messages;
begin
  select * into m from public.messages where id = p_message_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  if m.status <> 'sending' then raise exception 'message_not_sending' using errcode = '22023'; end if;
  if p_ok then
    update public.messages set status = 'sent', sent_at = now(), provider = p_provider,
      provider_message_id = p_provider_message_id, error = null
    where id = m.id returning * into m;
    perform private.message_event(m, 'sent', jsonb_build_object('provider', p_provider, 'id', p_provider_message_id));
  elsif p_retry and m.attempts < 5 then
    update public.messages set status = 'queued', send_after = now() + make_interval(mins => 5 * m.attempts * m.attempts),
      provider = p_provider, error = left(p_error, 500)
    where id = m.id returning * into m;
    perform private.message_event(m, 'failed', jsonb_build_object('error', left(p_error, 500), 'retry', true));
  else
    update public.messages set status = 'failed', provider = p_provider, error = left(p_error, 500)
    where id = m.id returning * into m;
    perform private.message_event(m, 'failed', jsonb_build_object('error', left(p_error, 500)));
  end if;
end $$;

-- Delivery receipts from a provider's webhook (delivered, bounced, complained).
create or replace function public.messages_worker_event(p_provider text, p_provider_message_id text, p_event text, p_detail jsonb default null)
returns void
language plpgsql security definer set search_path = '' as $$
declare m public.messages;
begin
  if p_event not in ('delivered','bounced','failed','complained') then raise exception 'invalid_event' using errcode = '22023'; end if;
  select * into m from public.messages where provider = p_provider and provider_message_id = p_provider_message_id for update;
  if not found then return; end if;
  update public.messages set status = case p_event when 'delivered' then 'delivered' when 'bounced' then 'failed'
                                               when 'failed' then 'failed' else status end,
                             error = case when p_event in ('bounced','failed') then coalesce(p_detail->>'reason', p_event) else error end
  where id = m.id returning * into m;
  perform private.message_event(m, p_event, p_detail);
  -- A complaint (marked as spam) or hard bounce stops future email to that customer.
  if p_event in ('complained','bounced') and m.client_id is not null and m.channel = 'email' and m.mode = 'live' then
    insert into public.contact_preferences (tenant_id, client_id, email_ok) values (m.tenant_id, m.client_id, false)
    on conflict (client_id) do update set email_ok = false;
  end if;
end $$;

-- Companies to run workflows for.
create or replace function public.messages_worker_tenants() returns setof uuid
language sql stable security definer set search_path = '' as $$
  select s.tenant_id from public.communication_settings s
  join public.tenants t on t.id = s.tenant_id and coalesce(t.status, 'active') = 'active'
  where s.delivery_mode <> 'off' and (s.visit_reminders or s.invoice_reminders)
$$;

-- ---------------------------------------------------------------------------
-- Customer portal: their own messages and preferences
-- ---------------------------------------------------------------------------
create or replace function public.portal_messages(p_client_id uuid)
returns table (message_id uuid, channel text, subject text, body text, sent_at timestamptz)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_portal_client(p_client_id);
  return query
  select m.id, m.channel, m.subject, m.body, m.sent_at
  from public.messages m
  where m.client_id = p_client_id and m.mode = 'live' and m.status in ('sent','delivered')
    and m.template_key <> 'portal_invite'
  order by m.sent_at desc
  limit 100;
end $$;

create or replace function public.portal_preferences(p_client_id uuid)
returns table (email_ok boolean, sms_ok boolean, visit_reminders boolean, invoice_reminders boolean)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_portal_client(p_client_id);
  return query
  select coalesce(p.email_ok, true), coalesce(p.sms_ok, false),
         not ('visit_reminder' = any (coalesce(p.kinds_off, '{}'))),
         not ('invoice_reminder' = any (coalesce(p.kinds_off, '{}')))
  from (select 1) x left join public.contact_preferences p on p.client_id = p_client_id;
end $$;

create or replace function public.portal_set_preferences(
  p_client_id uuid, p_email_ok boolean, p_sms_ok boolean, p_visit_reminders boolean, p_invoice_reminders boolean
) returns void
language plpgsql security definer set search_path = '' as $$
declare v_tenant uuid; v_off text[];
begin
  v_tenant := private.require_portal_client(p_client_id);
  v_off := array_remove(array[case when not p_visit_reminders then 'visit_reminder' end,
                              case when not p_invoice_reminders then 'invoice_reminder' end], null);
  insert into public.contact_preferences (tenant_id, client_id, email_ok, sms_ok, sms_consent_source, kinds_off)
  values (v_tenant, p_client_id, p_email_ok, p_sms_ok, case when p_sms_ok then 'portal' end, v_off)
  on conflict (client_id) do update set
    email_ok = excluded.email_ok,
    sms_ok = excluded.sms_ok,
    sms_consent_at = case when excluded.sms_ok and not public.contact_preferences.sms_ok then now()
                          when not excluded.sms_ok then null else public.contact_preferences.sms_consent_at end,
    sms_consent_source = case when excluded.sms_ok and not public.contact_preferences.sms_ok then 'portal'
                              when not excluded.sms_ok then null else public.contact_preferences.sms_consent_source end,
    kinds_off = excluded.kinds_off;
  perform private.log_activity(v_tenant, p_client_id, null, 'preferences_updated',
    format('Customer updated message preferences in the portal (email %s, texts %s)',
           case when p_email_ok then 'on' else 'off' end, case when p_sms_ok then 'on' else 'off' end));
end $$;

-- ---------------------------------------------------------------------------
-- Privileges
-- ---------------------------------------------------------------------------
revoke execute on function private.flag(text), private.communication_settings_guard(), private.contact_preferences_stamp(),
  private.message_template_check(), private.render_template(text, jsonb), private.money_text(numeric),
  private.message_event(public.messages, text, jsonb), private.portal_line(uuid, text),
  private.enqueue_message(uuid, text, text, uuid, uuid, text, text, uuid, jsonb, text, timestamptz)
  from public, anon, authenticated;

do $$
declare f text;
begin
  foreach f in array array[
    'public.default_message_templates()',
    'public.send_estimate(uuid, text)', 'public.send_invoice(uuid, text)',
    'public.send_invite_message(uuid, text, text, text, uuid)', 'public.cancel_message(uuid)',
    'public.queue_visit_reminders(uuid)', 'public.queue_invoice_reminders(uuid)',
    'public.portal_messages(uuid)', 'public.portal_preferences(uuid)',
    'public.portal_set_preferences(uuid, boolean, boolean, boolean, boolean)'] loop
    execute format('revoke execute on function %s from public, anon', f);
    execute format('grant execute on function %s to authenticated, service_role', f);
  end loop;
  foreach f in array array[
    'public.messages_worker_claim(integer)',
    'public.messages_worker_result(uuid, boolean, text, text, text, boolean)',
    'public.messages_worker_event(text, text, text, jsonb)',
    'public.messages_worker_tenants()'] loop
    execute format('revoke execute on function %s from public, anon, authenticated', f);
    execute format('grant execute on function %s to service_role', f);
  end loop;
end $$;
