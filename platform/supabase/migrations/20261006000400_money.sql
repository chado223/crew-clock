-- Money: estimates -> jobs, completed visits -> invoices -> payments.
-- Additive. The existing production invoice ($350, sent, no lines) is kept as is:
-- totals are recalculated only when lines change, and legacy rows have none.
--
-- Nothing here sends anything to customers. "Sent" is a status only.

-- ---------------------------------------------------------------------------
-- Per-company document numbers (EST-1001, INV-1001)
-- ---------------------------------------------------------------------------
create table if not exists public.document_counters (
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  kind text not null check (kind in ('estimate','invoice')),
  next_value integer not null default 1001,
  primary key (tenant_id, kind)
);

create or replace function private.next_document_number(p_tenant uuid, p_kind text) returns text
language plpgsql security definer set search_path = '' as $$
declare v integer;
begin
  insert into public.document_counters (tenant_id, kind) values (p_tenant, p_kind)
  on conflict (tenant_id, kind) do nothing;
  update public.document_counters set next_value = next_value + 1
   where tenant_id = p_tenant and kind = p_kind
  returning next_value - 1 into v;
  return case p_kind when 'estimate' then 'EST-' else 'INV-' end || v;
end $$;

-- ---------------------------------------------------------------------------
-- Estimates
-- ---------------------------------------------------------------------------
create table if not exists public.estimates (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  number text not null,
  client_id uuid not null,
  property_id uuid,
  status text not null default 'draft'
    check (status in ('draft','sent','approved','declined','expired','converted')),
  valid_until date,
  notes text,
  subtotal numeric(12,2) not null default 0,
  sent_at timestamptz,
  decided_at timestamptz,
  created_by uuid references auth.users (id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, id),
  unique (tenant_id, number),
  foreign key (tenant_id, client_id) references public.clients (tenant_id, id),
  foreign key (tenant_id, property_id) references public.properties (tenant_id, id)
);
create index if not exists estimates_tenant_status_idx on public.estimates (tenant_id, status);

create table if not exists public.estimate_lines (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null,
  estimate_id uuid not null,
  service_id uuid,
  description text not null check (length(trim(description)) between 1 and 500),
  quantity numeric(10,2) not null default 1 check (quantity > 0),
  unit_price numeric(10,2) not null check (unit_price >= 0),
  amount numeric(12,2) generated always as (round(quantity * unit_price, 2)) stored,
  repeat_every_weeks smallint check (repeat_every_weeks between 1 and 12),  -- null = one time
  est_minutes integer check (est_minutes between 1 and 1440),
  sort_order integer not null default 0,
  created_at timestamptz not null default now(),
  foreign key (tenant_id, estimate_id) references public.estimates (tenant_id, id) on delete cascade,
  foreign key (tenant_id, service_id) references public.services (tenant_id, id)
);
create index if not exists estimate_lines_estimate_idx on public.estimate_lines (estimate_id);

alter table public.jobs add column if not exists estimate_id uuid;
do $$ begin
  alter table public.jobs add constraint jobs_tenant_estimate_fk foreign key (tenant_id, estimate_id) references public.estimates (tenant_id, id);
exception when duplicate_object then null; end $$;

-- ---------------------------------------------------------------------------
-- Invoices (existing table) + lines + payments
-- ---------------------------------------------------------------------------
alter table public.invoices
  add column if not exists number text,
  add column if not exists subtotal numeric(12,2),
  add column if not exists tax_rate numeric(6,4) not null default 0,
  add column if not exists tax_amount numeric(12,2) not null default 0,
  add column if not exists amount_paid numeric(12,2) not null default 0,
  add column if not exists notes text,
  add column if not exists sent_at timestamptz,
  add column if not exists paid_at timestamptz,
  add column if not exists created_by uuid references auth.users (id) on delete set null,
  add column if not exists created_at timestamptz not null default now();

do $$ begin
  alter table public.invoices add constraint invoices_status_chk
    check (status in ('draft','sent','partial','paid','overdue','void')) not valid;
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.invoices validate constraint invoices_status_chk;
exception when check_violation then
  raise warning 'Some existing invoices have a status outside the new set; enforced for new writes only.';
end $$;
do $$ begin
  alter table public.invoices add constraint invoices_tax_rate_chk check (tax_rate >= 0 and tax_rate < 1);
exception when duplicate_object then null; end $$;
create unique index if not exists invoices_tenant_number_uidx on public.invoices (tenant_id, number) where number is not null;

create table if not exists public.invoice_lines (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null,
  invoice_id uuid not null,
  visit_id uuid,
  description text not null check (length(trim(description)) between 1 and 500),
  quantity numeric(10,2) not null default 1 check (quantity > 0),
  unit_price numeric(10,2) not null,
  amount numeric(12,2) generated always as (round(quantity * unit_price, 2)) stored,
  sort_order integer not null default 0,
  created_at timestamptz not null default now(),
  foreign key (tenant_id, invoice_id) references public.invoices (tenant_id, id) on delete cascade,
  foreign key (tenant_id, visit_id) references public.visits (tenant_id, id)
);
-- A visit is billed at most once.
create unique index if not exists invoice_lines_visit_uidx on public.invoice_lines (visit_id) where visit_id is not null;
create index if not exists invoice_lines_invoice_idx on public.invoice_lines (invoice_id);

create table if not exists public.payments (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null,
  invoice_id uuid not null,
  amount numeric(12,2) not null check (amount > 0),
  method text not null check (method in ('card','cash','check','ach','other')),
  reference text,
  received_on date not null,
  recorded_by uuid references auth.users (id) on delete set null,
  voided_at timestamptz,
  void_reason text,
  created_at timestamptz not null default now(),
  unique (tenant_id, id),
  foreign key (tenant_id, invoice_id) references public.invoices (tenant_id, id)
);
create index if not exists payments_invoice_idx on public.payments (invoice_id);

-- Expenses can belong to a visit too (materials on a job), for job costing.
alter table public.expenses add column if not exists visit_id uuid;
do $$ begin
  alter table public.expenses add constraint expenses_tenant_visit_fk foreign key (tenant_id, visit_id) references public.visits (tenant_id, id);
exception when duplicate_object then null; end $$;

-- ---------------------------------------------------------------------------
-- Totals are derived, never typed in
-- ---------------------------------------------------------------------------
create or replace function private.recalc_estimate(p_estimate_id uuid) returns void
language sql security definer set search_path = '' as $$
  update public.estimates e
     set subtotal = coalesce((select sum(l.amount) from public.estimate_lines l where l.estimate_id = e.id), 0)
   where e.id = p_estimate_id;
$$;

create or replace function private.recalc_invoice(p_invoice_id uuid) returns void
language plpgsql security definer set search_path = '' as $$
declare
  v_sub numeric(12,2);
  v_paid numeric(12,2);
  v public.invoices;
begin
  select * into v from public.invoices where id = p_invoice_id;
  if not found then return; end if;
  select sum(amount) into v_sub from public.invoice_lines where invoice_id = p_invoice_id;
  select coalesce(sum(amount), 0) into v_paid from public.payments where invoice_id = p_invoice_id and voided_at is null;
  if v_sub is not null then
    v.subtotal := v_sub;
    v.tax_amount := round(v_sub * v.tax_rate, 2);
    v.total := v_sub + v.tax_amount;
  end if;
  v.amount_paid := v_paid;
  if v.status <> 'void' and v.status <> 'draft' then
    v.status := case when v_paid >= v.total and v.total > 0 then 'paid'
                     when v_paid > 0 then 'partial'
                     when v.status in ('partial','paid') then 'sent'
                     else v.status end;
  end if;
  update public.invoices
     set subtotal = v.subtotal, tax_amount = v.tax_amount, total = v.total, amount_paid = v.amount_paid,
         status = v.status, paid_at = case when v.status = 'paid' then coalesce(paid_at, now()) else null end
   where id = p_invoice_id;
end $$;

create or replace function private.lines_changed() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  if tg_table_name = 'estimate_lines' then
    perform private.recalc_estimate(coalesce(new.estimate_id, old.estimate_id));
  else
    perform private.recalc_invoice(coalesce(new.invoice_id, old.invoice_id));
  end if;
  return null;
end $$;

drop trigger if exists lines_changed on public.estimate_lines;
create trigger lines_changed after insert or update or delete on public.estimate_lines
  for each row execute function private.lines_changed();
drop trigger if exists lines_changed on public.invoice_lines;
create trigger lines_changed after insert or update or delete on public.invoice_lines
  for each row execute function private.lines_changed();
drop trigger if exists lines_changed on public.payments;
create trigger lines_changed after insert or update on public.payments
  for each row execute function private.lines_changed();

-- Editing lines is only allowed while a document is a draft.
create or replace function private.guard_draft_lines() returns trigger
language plpgsql security definer set search_path = '' as $$
declare v_status text;
begin
  if tg_table_name = 'estimate_lines' then
    select status into v_status from public.estimates where id = coalesce(new.estimate_id, old.estimate_id);
  else
    select status into v_status from public.invoices where id = coalesce(new.invoice_id, old.invoice_id);
  end if;
  if v_status is distinct from 'draft' then
    raise exception 'document_not_draft' using errcode = '22023';
  end if;
  return coalesce(new, old);
end $$;
drop trigger if exists guard_draft_lines on public.estimate_lines;
create trigger guard_draft_lines before insert or update or delete on public.estimate_lines
  for each row execute function private.guard_draft_lines();
drop trigger if exists guard_draft_lines on public.invoice_lines;
create trigger guard_draft_lines before insert or update or delete on public.invoice_lines
  for each row execute function private.guard_draft_lines();

-- ---------------------------------------------------------------------------
-- Generic triggers, RLS (owners/admins only: this is money)
-- ---------------------------------------------------------------------------
do $$
declare t text;
begin
  foreach t in array array['estimates'] loop
    execute format('drop trigger if exists set_updated_at on public.%I', t);
    execute format('create trigger set_updated_at before update on public.%I for each row execute function private.set_updated_at()', t);
  end loop;
  foreach t in array array['document_counters','estimates','estimate_lines','invoice_lines','payments'] loop
    execute format('alter table public.%I enable row level security', t);
    execute format('drop trigger if exists prevent_tenant_change on public.%I', t);
    execute format('create trigger prevent_tenant_change before update of tenant_id on public.%I for each row execute function private.prevent_tenant_change()', t);
  end loop;
  foreach t in array array['estimates','estimate_lines','invoice_lines','payments'] loop
    execute format('drop trigger if exists audit_row_change on public.%I', t);
    execute format('create trigger audit_row_change after insert or update or delete on public.%I for each row execute function private.audit_row_change()', t);
    execute format('drop policy if exists %I on public.%I', t || '_select', t);
    execute format('create policy %I on public.%I for select to authenticated using (public.is_admin_or_owner(tenant_id))', t || '_select', t);
  end loop;
  foreach t in array array['estimates','estimate_lines','invoice_lines'] loop
    execute format('drop policy if exists %I on public.%I', t || '_insert', t);
    execute format('create policy %I on public.%I for insert to authenticated with check (public.is_admin_or_owner(tenant_id))', t || '_insert', t);
    execute format('drop policy if exists %I on public.%I', t || '_update', t);
    execute format('create policy %I on public.%I for update to authenticated using (public.is_admin_or_owner(tenant_id)) with check (public.is_admin_or_owner(tenant_id))', t || '_update', t);
    execute format('drop policy if exists %I on public.%I', t || '_delete', t);
    execute format('create policy %I on public.%I for delete to authenticated using (public.is_admin_or_owner(tenant_id))', t || '_delete', t);
  end loop;
end $$;
drop policy if exists document_counters_select on public.document_counters;
create policy document_counters_select on public.document_counters for select to authenticated using (public.is_admin_or_owner(tenant_id));

-- ---------------------------------------------------------------------------
-- Workflows
-- ---------------------------------------------------------------------------
create or replace function public.create_estimate(p_client_id uuid, p_property_id uuid default null, p_valid_days integer default 30)
returns public.estimates
language plpgsql security definer set search_path = '' as $$
declare
  v_tenant uuid;
  e public.estimates;
begin
  select tenant_id into v_tenant from public.clients where id = p_client_id;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(v_tenant);
  insert into public.estimates (tenant_id, number, client_id, property_id, valid_until, created_by)
  values (v_tenant, private.next_document_number(v_tenant, 'estimate'), p_client_id, p_property_id,
          current_date + greatest(1, least(coalesce(p_valid_days, 30), 365)), auth.uid())
  returning * into e;
  return e;
end $$;

-- Record the customer's decision (approval link, phone, in person).
create or replace function public.set_estimate_status(p_estimate_id uuid, p_status text) returns public.estimates
language plpgsql security definer set search_path = '' as $$
declare e public.estimates;
begin
  select * into e from public.estimates where id = p_estimate_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(e.tenant_id);
  if p_status not in ('sent','approved','declined') then raise exception 'invalid_status' using errcode = '22023'; end if;
  if e.status = 'converted' then raise exception 'estimate_converted' using errcode = '22023'; end if;
  if p_status = 'sent' and e.subtotal <= 0 then raise exception 'estimate_empty' using errcode = '22023'; end if;
  update public.estimates
     set status = p_status,
         sent_at = case when p_status = 'sent' then now() else sent_at end,
         decided_at = case when p_status in ('approved','declined') then now() else decided_at end
   where id = p_estimate_id
  returning * into e;
  perform private.log_activity(e.tenant_id, e.client_id, e.property_id, 'estimate_' || p_status,
    format('Estimate %s %s ($%s)', e.number, p_status, to_char(e.subtotal, 'FM999,999,990.00')),
    jsonb_build_object('estimate_id', e.id));
  if p_status = 'approved' then
    update public.clients set status = 'active' where id = e.client_id and status = 'lead';
  end if;
  return e;
end $$;

-- Approved estimate -> one job per line (recurring lines become recurring jobs).
create or replace function public.convert_estimate(p_estimate_id uuid, p_start_on date, p_crew_id uuid default null)
returns integer
language plpgsql security definer set search_path = '' as $$
declare
  e public.estimates;
  l record;
  v_jobs integer := 0;
  v_job uuid;
begin
  select * into e from public.estimates where id = p_estimate_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(e.tenant_id);
  if e.status <> 'approved' then raise exception 'estimate_not_approved' using errcode = '22023'; end if;
  if e.property_id is null then raise exception 'property_required' using errcode = '22023'; end if;
  if p_start_on is null then raise exception 'date_required' using errcode = '22023'; end if;

  for l in select * from public.estimate_lines where estimate_id = e.id order by sort_order, created_at loop
    insert into public.jobs (tenant_id, client_id, property_id, title, kind, status, price, est_minutes, crew_id,
                             service_id, estimate_id, starts_on, weekday, interval_weeks)
    values (e.tenant_id, e.client_id, e.property_id, l.description,
            case when l.repeat_every_weeks is null then 'one_off' else 'recurring' end, 'active',
            l.amount, l.est_minutes, p_crew_id, l.service_id, e.id,
            case when l.repeat_every_weeks is null then null else p_start_on end,
            case when l.repeat_every_weeks is null then null else extract(isodow from p_start_on)::smallint end,
            l.repeat_every_weeks)
    returning id into v_job;
    if l.repeat_every_weeks is null then
      insert into public.visits (tenant_id, job_id, scheduled_date) values (e.tenant_id, v_job, p_start_on);
    end if;
    v_jobs := v_jobs + 1;
  end loop;
  if v_jobs = 0 then raise exception 'estimate_empty' using errcode = '22023'; end if;

  perform public.generate_visits(e.tenant_id, p_start_on, p_start_on + 41);
  update public.estimates set status = 'converted' where id = e.id;
  perform private.log_activity(e.tenant_id, e.client_id, e.property_id, 'estimate_converted',
    format('Estimate %s became %s job%s starting %s', e.number, v_jobs, case when v_jobs = 1 then '' else 's' end, to_char(p_start_on, 'Mon FMDD')),
    jsonb_build_object('estimate_id', e.id));
  return v_jobs;
end $$;

-- Bill a customer's completed, not-yet-billed visits in a date range.
create or replace function public.invoice_completed_visits(
  p_client_id uuid, p_from date, p_to date, p_tax_rate numeric default 0, p_due_days integer default 30
) returns public.invoices
language plpgsql security definer set search_path = '' as $$
declare
  v_tenant uuid;
  inv public.invoices;
  v_count integer;
begin
  select tenant_id into v_tenant from public.clients where id = p_client_id;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(v_tenant);
  if p_tax_rate < 0 or p_tax_rate >= 1 then raise exception 'invalid_tax_rate' using errcode = '22023'; end if;

  select count(*) into v_count
  from public.visits v
  where v.client_id = p_client_id and v.status = 'completed' and v.scheduled_date between p_from and p_to
    and not exists (select 1 from public.invoice_lines il where il.visit_id = v.id);
  if v_count = 0 then raise exception 'nothing_to_invoice' using errcode = '22023'; end if;

  insert into public.invoices (tenant_id, client_id, number, status, tax_rate, issued_at, due_at, total, created_by)
  values (v_tenant, p_client_id, private.next_document_number(v_tenant, 'invoice'), 'draft', p_tax_rate,
          now(), now() + make_interval(days => greatest(0, coalesce(p_due_days, 30))), 0, auth.uid())
  returning * into inv;

  insert into public.invoice_lines (tenant_id, invoice_id, visit_id, description, quantity, unit_price, sort_order)
  select v_tenant, inv.id, v.id,
         j.title || ' (' || to_char(v.scheduled_date, 'Mon FMDD') || coalesce(', ' || p.address_line1, '') || ')',
         1, coalesce(v.price, 0), row_number() over (order by v.scheduled_date)
  from public.visits v
  join public.jobs j on j.id = v.job_id
  left join public.properties p on p.id = v.property_id
  where v.client_id = p_client_id and v.status = 'completed' and v.scheduled_date between p_from and p_to
    and not exists (select 1 from public.invoice_lines il where il.visit_id = v.id)
  order by v.scheduled_date;

  select * into inv from public.invoices where id = inv.id;
  return inv;
end $$;

create or replace function public.mark_invoice_sent(p_invoice_id uuid) returns public.invoices
language plpgsql security definer set search_path = '' as $$
declare inv public.invoices;
begin
  select * into inv from public.invoices where id = p_invoice_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(inv.tenant_id);
  if inv.status <> 'draft' then raise exception 'document_not_draft' using errcode = '22023'; end if;
  if inv.total <= 0 then raise exception 'invoice_empty' using errcode = '22023'; end if;
  update public.invoices set status = 'sent', sent_at = now() where id = p_invoice_id returning * into inv;
  perform private.log_activity(inv.tenant_id, inv.client_id, null, 'invoice_sent',
    format('Invoice %s sent ($%s)', coalesce(inv.number, ''), to_char(inv.total, 'FM999,999,990.00')),
    jsonb_build_object('invoice_id', inv.id));
  return inv;
end $$;

create or replace function public.record_payment(
  p_invoice_id uuid, p_amount numeric, p_method text, p_received_on date default current_date, p_reference text default null
) returns public.invoices
language plpgsql security definer set search_path = '' as $$
declare inv public.invoices;
begin
  select * into inv from public.invoices where id = p_invoice_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(inv.tenant_id);
  if inv.status in ('draft','void') then raise exception 'invoice_not_open' using errcode = '22023'; end if;
  if p_amount is null or p_amount <= 0 then raise exception 'invalid_amount' using errcode = '22023'; end if;
  if p_amount > inv.total - inv.amount_paid then raise exception 'overpayment' using errcode = '22023'; end if;
  insert into public.payments (tenant_id, invoice_id, amount, method, reference, received_on, recorded_by)
  values (inv.tenant_id, inv.id, round(p_amount, 2), p_method, nullif(trim(coalesce(p_reference, '')), ''),
          coalesce(p_received_on, current_date), auth.uid());
  select * into inv from public.invoices where id = p_invoice_id;
  perform private.log_activity(inv.tenant_id, inv.client_id, null, 'payment_received',
    format('Payment of $%s received for %s (%s)', to_char(p_amount, 'FM999,999,990.00'), coalesce(inv.number, 'invoice'), p_method),
    jsonb_build_object('invoice_id', inv.id, 'amount', p_amount));
  return inv;
end $$;

-- ---------------------------------------------------------------------------
-- Privileges
-- ---------------------------------------------------------------------------
revoke all on public.document_counters, public.estimates, public.estimate_lines, public.invoice_lines, public.payments
  from anon, authenticated;
grant select on public.document_counters, public.payments to authenticated;
grant select, insert, update, delete on public.estimates, public.estimate_lines, public.invoice_lines to authenticated;
grant all on public.document_counters, public.estimates, public.estimate_lines, public.invoice_lines, public.payments to service_role;
-- Totals, numbers and statuses are derived; users can only edit these fields.
-- Invoices are created only by invoice_completed_visits() (numbered, from real work).
revoke insert, update on public.invoices from authenticated;
grant update (due_at, notes, tax_rate) on public.invoices to authenticated;

create or replace function private.invoice_tax_changed() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  if new.status <> 'draft' and new.tax_rate is distinct from old.tax_rate then
    raise exception 'document_not_draft' using errcode = '22023';
  end if;
  perform private.recalc_invoice(new.id);
  return null;
end $$;
drop trigger if exists invoice_tax_changed on public.invoices;
create trigger invoice_tax_changed after update of tax_rate on public.invoices
  for each row when (new.tax_rate is distinct from old.tax_rate) execute function private.invoice_tax_changed();
revoke execute on function private.invoice_tax_changed() from public, anon, authenticated;

revoke execute on function private.next_document_number(uuid, text), private.recalc_estimate(uuid), private.recalc_invoice(uuid),
  private.lines_changed(), private.guard_draft_lines() from public, anon, authenticated;

revoke execute on function public.create_estimate(uuid, uuid, integer), public.set_estimate_status(uuid, text),
  public.convert_estimate(uuid, date, uuid), public.invoice_completed_visits(uuid, date, date, numeric, integer),
  public.mark_invoice_sent(uuid), public.record_payment(uuid, numeric, text, date, text) from public, anon;
grant execute on function public.create_estimate(uuid, uuid, integer), public.set_estimate_status(uuid, text),
  public.convert_estimate(uuid, date, uuid), public.invoice_completed_visits(uuid, date, date, numeric, integer),
  public.mark_invoice_sent(uuid), public.record_payment(uuid, numeric, text, date, text) to authenticated, service_role;
