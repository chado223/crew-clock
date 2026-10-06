-- Billing a residential route: company profile on documents, default tax and
-- terms, and invoicing every customer's finished work in one step.

alter table public.tenants
  add column if not exists phone text check (phone is null or length(phone) <= 40),
  add column if not exists email text check (email is null or email ~* '^[^@\s]+@[^@\s]+\.[^@\s]+$'),
  add column if not exists website text check (website is null or length(website) <= 200),
  add column if not exists address_line1 text check (address_line1 is null or length(address_line1) <= 200),
  add column if not exists city text check (city is null or length(city) <= 100),
  add column if not exists region text check (region is null or length(region) <= 50),
  add column if not exists postal_code text check (postal_code is null or length(postal_code) <= 20),
  add column if not exists default_tax_rate numeric(6,5) not null default 0 check (default_tax_rate >= 0 and default_tax_rate < 1),
  add column if not exists payment_terms_days integer not null default 30 check (payment_terms_days between 0 and 120),
  add column if not exists invoice_note text check (invoice_note is null or length(invoice_note) <= 1000),
  add column if not exists estimate_note text check (estimate_note is null or length(estimate_note) <= 1000);

grant update (phone, email, website, address_line1, city, region, postal_code, default_tax_rate, payment_terms_days,
              invoice_note, estimate_note) on public.tenants to authenticated;

-- Invoice every customer with finished, unbilled work in the period. One draft
-- per customer, using the company's tax rate and terms. Safe to run twice.
create or replace function public.invoice_all_completed(p_tenant_id uuid, p_from date, p_to date)
returns table (client_id uuid, client_name text, invoice_id uuid, number text, total numeric, visits integer)
language plpgsql security definer set search_path = '' as $$
declare t public.tenants; c record; inv public.invoices;
begin
  perform private.require_manager(p_tenant_id);
  if p_to < p_from or p_to - p_from > 400 then raise exception 'invalid_date_range' using errcode = '22023'; end if;
  select * into t from public.tenants where id = p_tenant_id;
  for c in
    select v.client_id as id, cl.name, count(*)::int as n
    from public.visits v join public.clients cl on cl.id = v.client_id
    where v.tenant_id = p_tenant_id and v.status = 'completed' and v.scheduled_date between p_from and p_to
      and not exists (select 1 from public.invoice_lines il where il.visit_id = v.id)
    group by v.client_id, cl.name
    order by cl.name
  loop
    inv := public.invoice_completed_visits(c.id, p_from, p_to, t.default_tax_rate, t.payment_terms_days);
    if t.invoice_note is not null then
      update public.invoices set notes = t.invoice_note where id = inv.id and notes is null returning * into inv;
    end if;
    client_id := c.id; client_name := c.name; invoice_id := inv.id; number := inv.number; total := inv.total; visits := c.n;
    return next;
  end loop;
end $$;

revoke execute on function public.invoice_all_completed(uuid, date, date) from public, anon;
grant execute on function public.invoice_all_completed(uuid, date, date) to authenticated, service_role;
