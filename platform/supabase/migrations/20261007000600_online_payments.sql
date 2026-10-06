-- Online invoice payments: the model, ready for a card processor later.
--
-- Nothing here charges anyone. Online payment stays off until:
--   1. the platform owner enables it (private.platform_flags 'online_payments'), and
--   2. the company connects a processor (payment_settings.enabled).
-- The flow, once a processor is chosen:
--   portal "Pay" -> server creates a checkout with the processor and records a
--   payment_request (payments_worker_open) -> customer pays on the processor's
--   page -> processor webhook -> payments_worker_settle() records a normal
--   payments row (method 'card'), which updates the invoice like any payment.
-- Invoices, payments and the customer model do not change.

insert into private.platform_flags (key, enabled, note)
values ('online_payments', false, 'Customers paying invoices online. Requires owner approval and a processor account.')
on conflict (key) do nothing;

create table if not exists public.payment_settings (
  tenant_id uuid primary key references public.tenants (id) on delete restrict,
  provider text check (provider is null or provider ~ '^[a-z][a-z0-9_]{1,30}$'),
  account_ref text,                       -- processor's id for the company's account (never a secret key)
  enabled boolean not null default false,
  pass_fees boolean not null default false,
  updated_at timestamptz not null default now()
);

create table if not exists public.payment_requests (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  invoice_id uuid not null,
  client_id uuid,
  amount numeric(12,2) not null check (amount > 0),
  provider text not null,
  provider_ref text not null,
  checkout_url text check (checkout_url is null or checkout_url ~ '^https://'),
  status text not null default 'pending' check (status in ('pending','succeeded','failed','canceled','needs_review')),
  payment_id uuid,
  failure_reason text,
  requested_by uuid references auth.users (id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, id),
  unique (provider, provider_ref),
  foreign key (tenant_id, invoice_id) references public.invoices (tenant_id, id),
  foreign key (tenant_id, client_id) references public.clients (tenant_id, id),
  foreign key (tenant_id, payment_id) references public.payments (tenant_id, id)
);
create index if not exists payment_requests_invoice_idx on public.payment_requests (invoice_id);

do $$
declare t text;
begin
  foreach t in array array['payment_settings','payment_requests'] loop
    execute format('drop trigger if exists set_updated_at on public.%I', t);
    execute format('create trigger set_updated_at before update on public.%I for each row execute function private.set_updated_at()', t);
    execute format('drop trigger if exists prevent_tenant_change on public.%I', t);
    execute format('create trigger prevent_tenant_change before update of tenant_id on public.%I for each row execute function private.prevent_tenant_change()', t);
    execute format('drop trigger if exists audit_row_change on public.%I', t);
    execute format('create trigger audit_row_change after insert or update or delete on public.%I for each row execute function private.audit_row_change()', t);
  end loop;
end $$;

alter table public.payment_settings enable row level security;
alter table public.payment_requests enable row level security;
drop policy if exists payment_settings_select on public.payment_settings;
drop policy if exists payment_requests_select on public.payment_requests;
create policy payment_settings_select on public.payment_settings for select to authenticated using (public.is_owner(tenant_id));
create policy payment_requests_select on public.payment_requests for select to authenticated using (public.is_admin_or_owner(tenant_id));
revoke all on public.payment_settings, public.payment_requests from anon, authenticated;
grant select on public.payment_settings, public.payment_requests to authenticated;
grant all on public.payment_settings, public.payment_requests to service_role;
-- Connecting a processor is done by the server integration (service_role), not from the browser.

-- ---------------------------------------------------------------------------
-- Customer: can this invoice be paid online?
-- ---------------------------------------------------------------------------
create or replace function public.portal_payment_options(p_client_id uuid, p_invoice_id uuid)
returns table (can_pay_online boolean, balance numeric, provider text, reason text)
language plpgsql stable security definer set search_path = '' as $$
declare v_tenant uuid; i public.invoices; s public.payment_settings;
begin
  v_tenant := private.require_portal_client(p_client_id);
  select * into i from public.invoices where id = p_invoice_id and client_id = p_client_id and tenant_id = v_tenant
    and status not in ('draft','void');
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  select * into s from public.payment_settings where tenant_id = v_tenant;
  return query select
    (private.flag('online_payments') and coalesce(s.enabled, false) and s.provider is not null and i.total - i.amount_paid > 0),
    round(i.total - i.amount_paid, 2),
    case when private.flag('online_payments') and coalesce(s.enabled, false) then s.provider end,
    case when i.total - i.amount_paid <= 0 then 'paid'
         when not private.flag('online_payments') or not coalesce(s.enabled, false) or s.provider is null then 'not_available'
         else null end;
end $$;

-- ---------------------------------------------------------------------------
-- Processor integration (service_role only)
-- ---------------------------------------------------------------------------
create or replace function public.payments_worker_open(
  p_invoice_id uuid, p_amount numeric, p_provider text, p_provider_ref text, p_checkout_url text, p_requested_by uuid default null
) returns uuid
language plpgsql security definer set search_path = '' as $$
declare i public.invoices; v_id uuid;
begin
  if not private.flag('online_payments') then raise exception 'online_payments_not_enabled' using errcode = '42501'; end if;
  select * into i from public.invoices where id = p_invoice_id;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  if not coalesce((select enabled and provider = p_provider from public.payment_settings where tenant_id = i.tenant_id), false) then
    raise exception 'online_payments_not_enabled' using errcode = '42501';
  end if;
  if i.status in ('draft','void') then raise exception 'invoice_not_open' using errcode = '22023'; end if;
  if p_amount is null or p_amount <= 0 or p_amount > i.total - i.amount_paid then raise exception 'invalid_amount' using errcode = '22023'; end if;
  insert into public.payment_requests (tenant_id, invoice_id, client_id, amount, provider, provider_ref, checkout_url, requested_by)
  values (i.tenant_id, i.id, i.client_id, round(p_amount, 2), p_provider, p_provider_ref, p_checkout_url, p_requested_by)
  on conflict (provider, provider_ref) do nothing
  returning id into v_id;
  if v_id is null then select id into v_id from public.payment_requests where provider = p_provider and provider_ref = p_provider_ref; end if;
  return v_id;
end $$;

-- Webhook result. Idempotent: the processor may send the same event twice.
create or replace function public.payments_worker_settle(
  p_provider text, p_provider_ref text, p_succeeded boolean, p_amount numeric default null, p_failure_reason text default null
) returns text
language plpgsql security definer set search_path = '' as $$
declare r public.payment_requests; i public.invoices; v_pay uuid; v_amount numeric;
begin
  select * into r from public.payment_requests where provider = p_provider and provider_ref = p_provider_ref for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  if r.status <> 'pending' then return r.status; end if;               -- already handled
  if not p_succeeded then
    update public.payment_requests set status = 'failed', failure_reason = left(p_failure_reason, 300) where id = r.id;
    return 'failed';
  end if;
  v_amount := round(coalesce(p_amount, r.amount), 2);
  select * into i from public.invoices where id = r.invoice_id for update;
  if v_amount <> r.amount or v_amount > i.total - i.amount_paid then
    -- Money moved but doesn't match: never guess. The office reviews it.
    update public.payment_requests set status = 'needs_review',
      failure_reason = format('Charged %s, expected %s, balance %s', v_amount, r.amount, i.total - i.amount_paid) where id = r.id;
    perform private.log_activity(i.tenant_id, i.client_id, null, 'payment_needs_review',
      format('Online payment for %s needs review (amount mismatch)', coalesce(i.number, 'invoice')), jsonb_build_object('invoice_id', i.id));
    return 'needs_review';
  end if;
  insert into public.payments (tenant_id, invoice_id, amount, method, reference, received_on)
  values (i.tenant_id, i.id, v_amount, 'card', p_provider || ':' || p_provider_ref,
          (now() at time zone coalesce((select timezone from public.tenants where id = i.tenant_id), 'UTC'))::date)
  returning id into v_pay;
  update public.payment_requests set status = 'succeeded', payment_id = v_pay where id = r.id;
  perform private.log_activity(i.tenant_id, i.client_id, null, 'payment_received',
    format('Paid online: $%s for %s', to_char(v_amount, 'FM999,999,990.00'), coalesce(i.number, 'invoice')),
    jsonb_build_object('invoice_id', i.id, 'amount', v_amount, 'online', true));
  return 'succeeded';
end $$;

revoke execute on function public.portal_payment_options(uuid, uuid) from public, anon;
grant execute on function public.portal_payment_options(uuid, uuid) to authenticated, service_role;
revoke execute on function public.payments_worker_open(uuid, numeric, text, text, text, uuid),
  public.payments_worker_settle(text, text, boolean, numeric, text) from public, anon, authenticated;
grant execute on function public.payments_worker_open(uuid, numeric, text, text, text, uuid),
  public.payments_worker_settle(text, text, boolean, numeric, text) to service_role;
