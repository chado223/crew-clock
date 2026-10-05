-- Simulates the CURRENT production Supabase project as described in the handoff:
-- helper functions, RLS enabled on the listed tables, typical first-draft
-- policies (including the common flaws the audit warns about), and the dev
-- seed data. Used to prove the foundation migration upgrades an existing
-- database without losing rows. Test-only.

-- Helpers as commonly first written: SECURITY INVOKER, no search_path.
create or replace function public.in_tenant(tid uuid) returns boolean language sql stable as $$
  select exists (select 1 from public.memberships m where m.tenant_id = tid and m.user_id = auth.uid())
$$;
create or replace function public.is_admin_or_owner(tid uuid) returns boolean language sql stable as $$
  select exists (select 1 from public.memberships m where m.tenant_id = tid and m.user_id = auth.uid() and m.role in ('owner','admin'))
$$;
create or replace function public.is_owner(tid uuid) returns boolean language sql stable as $$
  select exists (select 1 from public.memberships m where m.tenant_id = tid and m.user_id = auth.uid() and m.role = 'owner')
$$;

alter table public.tenants enable row level security;
alter table public.memberships enable row level security;
alter table public.clients enable row level security;
alter table public.jobs enable row level security;
alter table public.time_entries enable row level security;
alter table public.invoices enable row level security;
alter table public.expenses enable row level security;
-- profiles: RLS NOT enabled (as in handoff list)

create policy tenants_select on public.tenants for select using (in_tenant(id));
create policy memberships_select on public.memberships for select using (in_tenant(tenant_id));      -- recursive via in_tenant
create policy memberships_admin_update on public.memberships for update using (is_admin_or_owner(tenant_id)); -- no WITH CHECK, admin can self-promote
create policy clients_all_select on public.clients for select using (in_tenant(tenant_id));
create policy clients_insert on public.clients for insert with check (in_tenant(tenant_id));
create policy clients_update on public.clients for update using (in_tenant(tenant_id));                -- no WITH CHECK
create policy jobs_select on public.jobs for select using (in_tenant(tenant_id));
create policy te_select on public.time_entries for select using (in_tenant(tenant_id));                -- crew sees everyone's time
create policy te_insert on public.time_entries for insert with check (in_tenant(tenant_id));
create policy te_update on public.time_entries for update using (in_tenant(tenant_id));                -- crew can edit any time
create policy invoices_select on public.invoices for select using (in_tenant(tenant_id));              -- crew sees revenue
create policy expenses_select on public.expenses for select using (in_tenant(tenant_id));

-- Dev seed data from the handoff
insert into auth.users (id, email, raw_user_meta_data) values
  ('11111111-1111-1111-1111-111111111111', 'chadwasham64@gmail.com', '{"full_name":"Chad Washam"}');
insert into public.profiles (id, full_name) values ('11111111-1111-1111-1111-111111111111', 'Chad Washam');
insert into public.tenants (id, name, plan) values ('055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Chad Washam Lawncare', 'pro');
insert into public.memberships (tenant_id, user_id, role) values
  ('055bdb3c-c8d0-47d4-aa70-a77739054d7e', '11111111-1111-1111-1111-111111111111', 'owner');
insert into public.clients (id, tenant_id, name, email, phone, address) values
  ('c0000000-0000-0000-0000-000000000001', '055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Cool Springs HOA', 'board@coolsprings.example', '865-555-0100', '100 Cool Springs Dr, Seymour, TN');
insert into public.jobs (id, tenant_id, client_id, title, schedule, status) values
  ('b0000000-0000-0000-0000-000000000001', '055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'c0000000-0000-0000-0000-000000000001', 'Weekly Mow & Edge', '{"freq":"weekly","dow":"tue"}', 'scheduled');
insert into public.invoices (tenant_id, client_id, total, status, issued_at, due_at) values
  ('055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'c0000000-0000-0000-0000-000000000001', 350.00, 'sent', '2026-09-30', '2026-10-15');
insert into public.time_entries (tenant_id, user_id, job_id, clock_in, clock_out, notes) values
  ('055bdb3c-c8d0-47d4-aa70-a77739054d7e', '11111111-1111-1111-1111-111111111111', 'b0000000-0000-0000-0000-000000000001',
   '2026-10-01 12:00:00+00', '2026-10-01 14:00:00+00', 'seeded shift');
insert into public.expenses (tenant_id, category, amount, note) values
  ('055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Fuel', 62.40, 'seed'),
  ('055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Equipment', 189.99, 'seed'),
  ('055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Supplies', 24.15, 'seed'),
  ('055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Maintenance', 75.00, 'seed');
