-- Expenses and material costs tied to the work they belong to.
--
-- An expense can be tied to a visit (materials used on that day), a job
-- (e.g. a pallet of sod for a project), or a customer (e.g. a permit), or be
-- general overhead. Links are filled in from the most specific one, so a visit
-- expense also knows its job and customer.
--
-- One rule everywhere: an expense counts in the period of its date
-- (spent_at). Profitability groups by customer / property / service / job /
-- crew include every tied expense in the period; general overhead appears only
-- in the company totals (Business health). Tests prove the two add up.

alter table public.expenses add column if not exists job_id uuid;
alter table public.expenses add column if not exists client_id uuid;
alter table public.expenses add column if not exists created_by uuid references auth.users (id) on delete set null;
alter table public.expenses add column if not exists created_at timestamptz not null default now();

do $$ begin
  alter table public.expenses add constraint expenses_tenant_job_fk foreign key (tenant_id, job_id) references public.jobs (tenant_id, id);
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.expenses add constraint expenses_tenant_client_fk foreign key (tenant_id, client_id) references public.clients (tenant_id, id);
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.expenses add constraint expenses_amount_positive check (amount > 0) not valid;
exception when duplicate_object then null; end $$;
create index if not exists expenses_job_idx on public.expenses (job_id) where job_id is not null;
create index if not exists expenses_client_idx on public.expenses (client_id) where client_id is not null;
create index if not exists expenses_tenant_spent_idx on public.expenses (tenant_id, spent_at);

create or replace function private.expense_links() returns trigger
language plpgsql security definer set search_path = '' as $$
declare v public.visits; j public.jobs;
begin
  if new.visit_id is not null then
    select * into v from public.visits where id = new.visit_id and tenant_id = new.tenant_id;
    if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
    new.job_id := v.job_id;
    new.client_id := v.client_id;
  elsif new.job_id is not null then
    select * into j from public.jobs where id = new.job_id and tenant_id = new.tenant_id;
    if not found then raise exception 'job_not_found' using errcode = 'P0002'; end if;
    new.client_id := j.client_id;
  end if;
  if tg_op = 'INSERT' then new.created_by := coalesce(auth.uid(), new.created_by); end if;
  return new;
end $$;
drop trigger if exists expense_links on public.expenses;
create trigger expense_links before insert or update of visit_id, job_id, client_id on public.expenses
  for each row execute function private.expense_links();
revoke execute on function private.expense_links() from public, anon, authenticated;

-- ---------------------------------------------------------------------------
-- Profitability (replaces the earlier version): revenue and labor from job
-- costing, plus every tied expense dated in the period.
-- ---------------------------------------------------------------------------
create or replace function public.profitability(p_tenant_id uuid, p_from date, p_to date, p_by text)
returns table (
  key uuid, label text, sub text, visits integer, revenue numeric, labor_cost numeric, expenses numeric,
  margin numeric, margin_pct numeric, onsite_minutes integer, missing_rates integer
)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_manager(p_tenant_id);
  if p_by not in ('customer','property','service','job','crew') then raise exception 'invalid_group' using errcode = '22023'; end if;
  return query
  with vc as (select * from public.visit_costing(p_tenant_id, p_from, p_to)),
  rev as (
    select case p_by when 'customer' then vc.client_id when 'property' then vc.property_id when 'service' then j.service_id
                     when 'job' then vc.job_id else v.crew_id end as k,
           count(*)::int as n, sum(vc.revenue) as rev, sum(vc.labor_cost) as lab,
           sum(vc.onsite_minutes)::int as mins, sum(vc.missing_rates)::int as miss
    from vc join public.visits v on v.id = vc.visit_id join public.jobs j on j.id = vc.job_id
    group by 1
  ),
  ex as (
    select case p_by when 'customer' then e.client_id
                     when 'property' then coalesce(v.property_id, j.property_id)
                     when 'service' then j.service_id
                     when 'job' then e.job_id
                     else coalesce(v.crew_id, j.crew_id) end as k,
           sum(e.amount) as amt
    from public.expenses e
    left join public.visits v on v.id = e.visit_id
    left join public.jobs j on j.id = e.job_id
    where e.tenant_id = p_tenant_id and e.spent_at between p_from and p_to
      and (e.visit_id is not null or e.job_id is not null or e.client_id is not null)
    group by 1
  ),
  g as (
    select u.k, sum(u.n)::int as n, sum(u.rev) as rev, sum(u.lab) as lab, sum(u.ex) as ex,
           sum(u.mins)::int as mins, sum(u.miss)::int as miss
    from (
      select r.k, r.n, r.rev, r.lab, 0::numeric as ex, r.mins, r.miss from rev r
      union all
      select x.k, 0, 0, 0, x.amt, 0, 0 from ex x
    ) u
    group by u.k
  )
  select g.k,
         case p_by
           when 'customer' then coalesce((select c.name from public.clients c where c.id = g.k), 'No customer')
           when 'property' then coalesce((select p.address_line1 from public.properties p where p.id = g.k), 'Not tied to a property')
           when 'service' then coalesce((select s.name from public.services s where s.id = g.k), 'No service set')
           when 'job' then coalesce((select j.title from public.jobs j where j.id = g.k), 'Not tied to a job')
           else coalesce((select cr.name from public.crews cr where cr.id = g.k), 'No crew') end,
         case p_by
           when 'property' then (select c.name from public.properties p join public.clients c on c.id = p.client_id where p.id = g.k)
           when 'job' then (select c.name from public.jobs j join public.clients c on c.id = j.client_id where j.id = g.k)
           else null end,
         g.n, round(g.rev, 2), round(g.lab, 2), round(g.ex, 2),
         round(g.rev - g.lab - g.ex, 2),
         case when g.rev > 0 then round((g.rev - g.lab - g.ex) / g.rev * 100, 1) end,
         g.mins, g.miss
  from g
  order by (g.rev - g.lab - g.ex) desc;
end $$;
