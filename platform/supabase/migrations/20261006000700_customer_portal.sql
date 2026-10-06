-- Customer portal.
--
-- Security model (explicit, tested in tests/95_customer_portal.sql):
--   * A customer login is linked to one or more client records through
--     public.portal_access. Customers are NOT memberships, so every staff RLS
--     policy already returns nothing to them (default deny).
--   * Customers get NO new table policies. Everything they see or do goes
--     through portal_* functions (SECURITY DEFINER) that:
--       - verify the caller's active portal_access to that exact client
--       - return only customer-facing columns (never internal notes, crew
--         notes, gate codes, crew/assignees, pay, costs, margins, audit data)
--   * Every customer action (estimate decision, service request, contact
--     change) is written to the audit log and the customer history.

-- ---------------------------------------------------------------------------
-- Access + invitations
-- ---------------------------------------------------------------------------
create table if not exists public.portal_access (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  client_id uuid not null,
  user_id uuid not null references auth.users (id) on delete cascade,
  status text not null default 'active' check (status in ('active','revoked')),
  granted_by uuid references auth.users (id) on delete set null,
  created_at timestamptz not null default now(),
  revoked_at timestamptz,
  unique (client_id, user_id),
  foreign key (tenant_id, client_id) references public.clients (tenant_id, id)
);
create index if not exists portal_access_user_idx on public.portal_access (user_id) where status = 'active';

create table if not exists public.portal_invitations (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  client_id uuid not null,
  email text not null,
  token_hash text not null unique,
  invited_by uuid references auth.users (id) on delete set null,
  expires_at timestamptz not null,
  accepted_at timestamptz,
  accepted_by uuid references auth.users (id) on delete set null,
  revoked_at timestamptz,
  created_at timestamptz not null default now(),
  foreign key (tenant_id, client_id) references public.clients (tenant_id, id)
);
create unique index if not exists portal_invitations_one_pending_uidx on public.portal_invitations (client_id, lower(email))
  where accepted_at is null and revoked_at is null;

-- ---------------------------------------------------------------------------
-- Service requests (customer -> company)
-- ---------------------------------------------------------------------------
create table if not exists public.service_requests (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  client_id uuid not null,
  property_id uuid,
  details text not null check (length(trim(details)) between 3 and 2000),
  preferred_date date,
  status text not null default 'new' check (status in ('new','acknowledged','scheduled','closed')),
  requested_by uuid references auth.users (id) on delete set null,
  staff_note text,                                   -- internal; never returned to customers
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, id),
  foreign key (tenant_id, client_id) references public.clients (tenant_id, id),
  foreign key (tenant_id, property_id) references public.properties (tenant_id, id)
);
create index if not exists service_requests_tenant_status_idx on public.service_requests (tenant_id, status, created_at desc);

-- ---------------------------------------------------------------------------
-- Triggers, staff RLS (managers manage; customers get nothing directly)
-- ---------------------------------------------------------------------------
do $$
declare t text;
begin
  foreach t in array array['portal_access','portal_invitations','service_requests'] loop
    execute format('alter table public.%I enable row level security', t);
    execute format('drop trigger if exists prevent_tenant_change on public.%I', t);
    execute format('create trigger prevent_tenant_change before update of tenant_id on public.%I for each row execute function private.prevent_tenant_change()', t);
    execute format('drop trigger if exists audit_row_change on public.%I', t);
    execute format('create trigger audit_row_change after insert or update or delete on public.%I for each row execute function private.audit_row_change()', t);
    execute format('drop policy if exists %I on public.%I', t || '_select', t);
    execute format('create policy %I on public.%I for select to authenticated using (public.is_admin_or_owner(tenant_id))', t || '_select', t);
  end loop;
end $$;
drop trigger if exists set_updated_at on public.service_requests;
create trigger set_updated_at before update on public.service_requests for each row execute function private.set_updated_at();
drop policy if exists service_requests_update on public.service_requests;
create policy service_requests_update on public.service_requests for update to authenticated
  using (public.is_admin_or_owner(tenant_id)) with check (public.is_admin_or_owner(tenant_id));

revoke all on public.portal_access, public.portal_invitations, public.service_requests from anon, authenticated;
grant select on public.portal_access, public.portal_invitations to authenticated;
grant select on public.service_requests to authenticated;
grant update (status, staff_note) on public.service_requests to authenticated;
grant all on public.portal_access, public.portal_invitations, public.service_requests to service_role;

-- ---------------------------------------------------------------------------
-- Customer access check (the single gate for every portal function)
-- ---------------------------------------------------------------------------
create or replace function private.require_portal_client(p_client_id uuid) returns uuid
language plpgsql stable security definer set search_path = '' as $$
declare v_tenant uuid;
begin
  if auth.uid() is null then raise exception 'not_authenticated' using errcode = '42501'; end if;
  select pa.tenant_id into v_tenant
  from public.portal_access pa
  join public.tenants t on t.id = pa.tenant_id and t.status = 'active'
  where pa.client_id = p_client_id and pa.user_id = auth.uid() and pa.status = 'active';
  if v_tenant is null then raise exception 'forbidden' using errcode = '42501'; end if;
  return v_tenant;
end $$;

-- ---------------------------------------------------------------------------
-- Staff: invite a customer to the portal
-- ---------------------------------------------------------------------------
create or replace function public.invite_customer(p_client_id uuid, p_email text) returns text
language plpgsql security definer set search_path = '' as $$
declare
  v_tenant uuid;
  v_email text := lower(trim(coalesce(p_email, '')));
  v_token text;
begin
  select tenant_id into v_tenant from public.clients where id = p_client_id;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(v_tenant);
  if v_email !~ '^[^@\s]+@[^@\s]+\.[^@\s]+$' then raise exception 'invalid_email' using errcode = '22023'; end if;
  update public.portal_invitations set revoked_at = now()
   where client_id = p_client_id and lower(email) = v_email and accepted_at is null and revoked_at is null;
  v_token := encode(extensions.gen_random_bytes(24), 'hex');
  insert into public.portal_invitations (tenant_id, client_id, email, token_hash, invited_by, expires_at)
  values (v_tenant, p_client_id, v_email, encode(extensions.digest(v_token, 'sha256'), 'hex'), auth.uid(), now() + interval '14 days');
  return v_token;
end $$;

create or replace function public.revoke_portal_access(p_access_id uuid) returns void
language plpgsql security definer set search_path = '' as $$
declare v_tenant uuid;
begin
  select tenant_id into v_tenant from public.portal_access where id = p_access_id;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(v_tenant);
  update public.portal_access set status = 'revoked', revoked_at = now() where id = p_access_id;
end $$;

-- Customer accepts (email must match the invitation).
create or replace function public.accept_portal_invitation(p_token text) returns uuid
language plpgsql security definer set search_path = '' as $$
declare
  v_uid uuid := auth.uid();
  inv public.portal_invitations;
  v_email text;
begin
  if v_uid is null then raise exception 'not_authenticated' using errcode = '42501'; end if;
  select * into inv from public.portal_invitations
   where token_hash = encode(extensions.digest(coalesce(p_token, ''), 'sha256'), 'hex') for update;
  if not found or inv.revoked_at is not null then raise exception 'invitation_invalid' using errcode = '22023'; end if;
  if inv.accepted_at is not null then raise exception 'invitation_already_used' using errcode = '22023'; end if;
  if inv.expires_at < now() then raise exception 'invitation_expired' using errcode = '22023'; end if;
  select lower(email) into v_email from auth.users where id = v_uid;
  if v_email is distinct from lower(inv.email) then raise exception 'invitation_email_mismatch' using errcode = '42501'; end if;

  insert into public.portal_access (tenant_id, client_id, user_id, granted_by)
  values (inv.tenant_id, inv.client_id, v_uid, inv.invited_by)
  on conflict (client_id, user_id) do update set status = 'active', revoked_at = null;
  update public.portal_invitations set accepted_at = now(), accepted_by = v_uid where id = inv.id;
  perform private.log_activity(inv.tenant_id, inv.client_id, null, 'portal_joined', 'Customer signed in to the portal for the first time');
  return inv.client_id;
end $$;

-- ---------------------------------------------------------------------------
-- Portal read functions (customer-facing columns only)
-- ---------------------------------------------------------------------------
create or replace function public.portal_accounts()
returns table (client_id uuid, client_name text, tenant_id uuid, company_name text, company_timezone text)
language sql stable security definer set search_path = '' as $$
  select c.id, c.name, t.id, t.name, t.timezone
  from public.portal_access pa
  join public.clients c on c.id = pa.client_id
  join public.tenants t on t.id = pa.tenant_id and t.status = 'active'
  where pa.user_id = (select auth.uid()) and pa.status = 'active'
  order by t.name, c.name
$$;

create or replace function public.portal_profile(p_client_id uuid)
returns table (name text, company_name text, email text, phone text, preferred_contact text, mailing_address text)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_portal_client(p_client_id);
  return query select c.name, c.company_name, c.email, c.phone, c.preferred_contact, c.address
               from public.clients c where c.id = p_client_id;
end $$;

create or replace function public.portal_properties(p_client_id uuid)
returns table (property_id uuid, label text, address text)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_portal_client(p_client_id);
  return query select p.id, p.label, concat_ws(', ', p.address_line1, p.address_line2, p.city, p.region, p.postal_code)
               from public.properties p where p.client_id = p_client_id and p.status = 'active' order by p.created_at;
end $$;

create or replace function public.portal_services(p_client_id uuid)
returns table (job_id uuid, title text, kind text, every_weeks smallint, weekday smallint, price numeric, address text, next_visit date)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_portal_client(p_client_id);
  return query
  select j.id, j.title, j.kind, j.interval_weeks, j.weekday, j.price, p.address_line1,
         (select min(v.scheduled_date) from public.visits v where v.job_id = j.id and v.status = 'scheduled' and v.scheduled_date >= current_date)
  from public.jobs j left join public.properties p on p.id = j.property_id
  where j.client_id = p_client_id and j.status in ('scheduled','active')
  order by j.kind desc, j.title;
end $$;

-- Visits as the customer should see them: no crew, no internal reasons, no crew notes.
create or replace function public.portal_visits(p_client_id uuid, p_from date, p_to date)
returns table (visit_id uuid, visit_date date, service text, address text, status text, completed_at timestamptz)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_portal_client(p_client_id);
  if p_to < p_from or p_to - p_from > 400 then raise exception 'invalid_date_range' using errcode = '22023'; end if;
  return query
  select v.id, v.scheduled_date, j.title, p.address_line1,
         case v.status when 'in_progress' then 'in_progress' when 'completed' then 'completed'
                       when 'scheduled' then 'scheduled' else 'not_serviced' end,
         v.completed_at
  from public.visits v join public.jobs j on j.id = v.job_id
  left join public.properties p on p.id = v.property_id
  where v.client_id = p_client_id and v.scheduled_date between p_from and p_to
  order by v.scheduled_date desc;
end $$;

-- Estimates the customer has been sent (never drafts).
create or replace function public.portal_estimates(p_client_id uuid)
returns table (estimate_id uuid, number text, status text, total numeric, valid_until date, address text, sent_at timestamptz, decided_at timestamptz,
               lines jsonb)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_portal_client(p_client_id);
  return query
  select e.id, e.number, e.status, e.subtotal, e.valid_until, p.address_line1, e.sent_at, e.decided_at,
         coalesce((select jsonb_agg(jsonb_build_object(
                     'description', l.description, 'quantity', l.quantity, 'unit_price', l.unit_price,
                     'amount', l.amount, 'repeat_every_weeks', l.repeat_every_weeks) order by l.sort_order, l.created_at)
                   from public.estimate_lines l where l.estimate_id = e.id), '[]'::jsonb)
  from public.estimates e left join public.properties p on p.id = e.property_id
  where e.client_id = p_client_id and e.status in ('sent','approved','declined','converted','expired')
  order by e.sent_at desc nulls last;
end $$;

create or replace function public.portal_invoices(p_client_id uuid)
returns table (invoice_id uuid, number text, status text, issued_at timestamptz, due_at timestamptz,
               subtotal numeric, tax_amount numeric, total numeric, amount_paid numeric, balance numeric,
               lines jsonb, payments jsonb)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_portal_client(p_client_id);
  return query
  select i.id, i.number, i.status, i.issued_at, i.due_at, i.subtotal, i.tax_amount, i.total, i.amount_paid,
         i.total - i.amount_paid,
         coalesce((select jsonb_agg(jsonb_build_object('description', l.description, 'quantity', l.quantity, 'amount', l.amount) order by l.sort_order)
                   from public.invoice_lines l where l.invoice_id = i.id), '[]'::jsonb),
         coalesce((select jsonb_agg(jsonb_build_object('amount', pm.amount, 'method', pm.method, 'received_on', pm.received_on) order by pm.received_on)
                   from public.payments pm where pm.invoice_id = i.id and pm.voided_at is null), '[]'::jsonb)
  from public.invoices i
  where i.client_id = p_client_id and i.status not in ('draft','void')
  order by i.issued_at desc;
end $$;

create or replace function public.portal_requests(p_client_id uuid)
returns table (request_id uuid, details text, preferred_date date, status text, address text, created_at timestamptz)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_portal_client(p_client_id);
  return query
  select r.id, r.details, r.preferred_date, r.status, p.address_line1, r.created_at
  from public.service_requests r left join public.properties p on p.id = r.property_id
  where r.client_id = p_client_id order by r.created_at desc;
end $$;

-- ---------------------------------------------------------------------------
-- Portal actions (audited, written to customer history)
-- ---------------------------------------------------------------------------
create or replace function public.portal_respond_estimate(p_estimate_id uuid, p_decision text, p_note text default null)
returns text
language plpgsql security definer set search_path = '' as $$
declare
  e public.estimates;
  v_note text := nullif(trim(coalesce(p_note, '')), '');
begin
  select * into e from public.estimates where id = p_estimate_id for update;
  if not found then raise exception 'forbidden' using errcode = '42501'; end if;     -- don't reveal existence
  perform private.require_portal_client(e.client_id);
  if p_decision not in ('approved','declined') then raise exception 'invalid_status' using errcode = '22023'; end if;
  if e.status <> 'sent' then raise exception 'estimate_not_open' using errcode = '22023'; end if;
  if e.valid_until is not null and e.valid_until < current_date then raise exception 'estimate_expired' using errcode = '22023'; end if;
  if v_note is not null and length(v_note) > 2000 then raise exception 'note_too_long' using errcode = '22023'; end if;

  perform set_config('app.audit_reason', 'customer ' || p_decision || ' in portal' || coalesce(': ' || v_note, ''), true);
  update public.estimates set status = p_decision, decided_at = now() where id = e.id;
  perform private.log_activity(e.tenant_id, e.client_id, e.property_id, 'estimate_' || p_decision,
    format('Customer %s estimate %s in the portal ($%s)%s', p_decision, e.number, to_char(e.subtotal, 'FM999,999,990.00'),
           coalesce(': "' || v_note || '"', '')),
    jsonb_build_object('estimate_id', e.id, 'by', 'customer'));
  if p_decision = 'approved' then
    update public.clients set status = 'active' where id = e.client_id and status = 'lead';
  end if;
  return p_decision;
end $$;

create or replace function public.portal_submit_request(
  p_client_id uuid, p_details text, p_property_id uuid default null, p_preferred_date date default null
) returns uuid
language plpgsql security definer set search_path = '' as $$
declare
  v_tenant uuid := private.require_portal_client(p_client_id);
  v_id uuid;
  v_recent integer;
begin
  if p_property_id is not null and not exists (select 1 from public.properties where id = p_property_id and client_id = p_client_id) then
    raise exception 'forbidden' using errcode = '42501';
  end if;
  if p_details is null or length(trim(p_details)) < 3 then raise exception 'details_required' using errcode = '22023'; end if;
  select count(*) into v_recent from public.service_requests
   where requested_by = auth.uid() and created_at > now() - interval '1 hour';
  if v_recent >= 10 then raise exception 'too_many_requests' using errcode = '54000'; end if;

  insert into public.service_requests (tenant_id, client_id, property_id, details, preferred_date, requested_by)
  values (v_tenant, p_client_id, p_property_id, trim(p_details), p_preferred_date, auth.uid())
  returning id into v_id;
  perform private.log_activity(v_tenant, p_client_id, p_property_id, 'service_requested',
    'Customer requested: ' || left(trim(p_details), 300), jsonb_build_object('request_id', v_id));
  return v_id;
end $$;

-- Customers may update how to reach them; name and billing identity stay with the company.
create or replace function public.portal_update_contact(
  p_client_id uuid, p_phone text, p_preferred_contact text default null, p_mailing_address text default null
) returns void
language plpgsql security definer set search_path = '' as $$
declare
  v_tenant uuid := private.require_portal_client(p_client_id);
  v_phone text := nullif(trim(coalesce(p_phone, '')), '');
begin
  if v_phone is not null and v_phone !~ '^[0-9+()\-. ]{7,25}$' then raise exception 'invalid_phone' using errcode = '22023'; end if;
  if p_preferred_contact is not null and p_preferred_contact not in ('call','text','email') then
    raise exception 'invalid_status' using errcode = '22023';
  end if;
  perform set_config('app.audit_reason', 'customer updated contact details in portal', true);
  update public.clients
     set phone = v_phone,
         preferred_contact = coalesce(p_preferred_contact, preferred_contact),
         address = coalesce(nullif(trim(coalesce(p_mailing_address, '')), ''), address)
   where id = p_client_id;
  perform private.log_activity(v_tenant, p_client_id, null, 'contact_updated', 'Customer updated their contact details in the portal');
end $$;

-- ---------------------------------------------------------------------------
-- Privileges
-- ---------------------------------------------------------------------------
revoke execute on function private.require_portal_client(uuid) from public, anon, authenticated;

do $$
declare f text;
begin
  foreach f in array array[
    'public.invite_customer(uuid, text)', 'public.revoke_portal_access(uuid)', 'public.accept_portal_invitation(text)',
    'public.portal_accounts()', 'public.portal_profile(uuid)', 'public.portal_properties(uuid)', 'public.portal_services(uuid)',
    'public.portal_visits(uuid, date, date)', 'public.portal_estimates(uuid)', 'public.portal_invoices(uuid)',
    'public.portal_requests(uuid)', 'public.portal_respond_estimate(uuid, text, text)',
    'public.portal_submit_request(uuid, text, uuid, date)', 'public.portal_update_contact(uuid, text, text, text)'] loop
    execute format('revoke execute on function %s from public, anon', f);
    execute format('grant execute on function %s to authenticated, service_role', f);
  end loop;
end $$;
