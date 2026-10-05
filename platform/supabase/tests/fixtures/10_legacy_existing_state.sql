-- Reproduces production (Supabase iwowjrnrbjiydckhjsfi, inspected 2026-10-05)
-- exactly: helper functions, every policy, and the real rows (ids, names,
-- amounts, dates). Production has 2 auth users, 1 tenant, NO memberships, NO
-- profiles and NO time entries. Test-only.

create or replace function public.in_tenant(tid uuid) returns boolean language sql stable as $$
  SELECT EXISTS (SELECT 1 FROM public.memberships m WHERE m.tenant_id = tid AND m.user_id = auth.uid());
$$;
create or replace function public.is_admin_or_owner(tid uuid) returns boolean language sql stable as $$
  SELECT EXISTS (SELECT 1 FROM public.memberships m WHERE m.tenant_id = tid AND m.user_id = auth.uid() AND m.role IN ('admin','owner'));
$$;
create or replace function public.is_owner(tid uuid) returns boolean language sql stable as $$
  SELECT EXISTS (SELECT 1 FROM public.memberships m WHERE m.tenant_id = tid AND m.user_id = auth.uid() AND m.role = 'owner');
$$;

create policy clients_delete on public.clients for delete using (is_admin_or_owner(tenant_id));
create policy clients_insert on public.clients for insert with check (is_admin_or_owner(tenant_id));
create policy clients_select on public.clients for select using (in_tenant(tenant_id));
create policy clients_update on public.clients for update using (is_admin_or_owner(tenant_id)) with check (is_admin_or_owner(tenant_id));
create policy expenses_delete on public.expenses for delete using (is_admin_or_owner(tenant_id));
create policy expenses_insert on public.expenses for insert with check (is_admin_or_owner(tenant_id));
create policy expenses_select on public.expenses for select using (in_tenant(tenant_id));
create policy expenses_update on public.expenses for update using (is_admin_or_owner(tenant_id)) with check (is_admin_or_owner(tenant_id));
create policy invoices_delete on public.invoices for delete using (is_admin_or_owner(tenant_id));
create policy invoices_insert on public.invoices for insert with check (is_admin_or_owner(tenant_id));
create policy invoices_select on public.invoices for select using (in_tenant(tenant_id));
create policy invoices_update on public.invoices for update using (is_admin_or_owner(tenant_id)) with check (is_admin_or_owner(tenant_id));
create policy jobs_delete on public.jobs for delete using (is_admin_or_owner(tenant_id));
create policy jobs_insert on public.jobs for insert with check (is_admin_or_owner(tenant_id));
create policy jobs_select on public.jobs for select using (in_tenant(tenant_id));
create policy jobs_update on public.jobs for update using (is_admin_or_owner(tenant_id)) with check (is_admin_or_owner(tenant_id));
create policy memberships_delete on public.memberships for delete using (is_owner(tenant_id));
create policy memberships_insert on public.memberships for insert with check (is_owner(tenant_id));
create policy memberships_select on public.memberships for select using ((user_id = auth.uid()) or is_admin_or_owner(tenant_id));
create policy memberships_update on public.memberships for update using (is_owner(tenant_id)) with check (is_owner(tenant_id));
create policy "profiles insert self" on public.profiles for insert with check (auth.uid() = id);
create policy "profiles self" on public.profiles for select using (auth.uid() = id);
create policy tenants_insert on public.tenants for insert to authenticated with check (true);
create policy tenants_select on public.tenants for select using (in_tenant(id));
-- time_entries: RLS on, no policies (production)

-- Production rows
insert into auth.users (id, email, created_at) values
  ('31d54ab5-62a5-4aa8-b1d4-08c4772e96db', 'chadwasham@gmail.com',   '2025-08-22 13:27:29.585511+00'),
  ('56942ef9-b819-4343-9b35-dedcd4c854bd', 'chadwasham64@gmail.com', '2025-08-22 13:27:57.739084+00');
-- Shim's auth trigger would have created profiles; production has none.
delete from public.profiles;

insert into public.tenants (id, name, plan, created_at) values
  ('055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Chad Washam Lawncare', 'pro', '2025-08-30 13:36:22.902987+00');
insert into public.clients (id, tenant_id, name, created_at) values
  ('cc1588c7-0a3d-4188-bd36-2a5dbd109163', '055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Cool Springs HOA', '2025-08-30 13:39:40.790716+00');
insert into public.jobs (id, tenant_id, client_id, title, created_at) values
  ('bf955a8f-206d-493d-bfca-11b9e887d3a1', '055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'cc1588c7-0a3d-4188-bd36-2a5dbd109163', 'Weekly Mow & Edge', '2025-08-30 13:39:40.790716+00');
insert into public.invoices (id, tenant_id, client_id, total, status, issued_at) values
  ('5efa660b-bf18-4273-ba9f-8de0ac321074', '055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'cc1588c7-0a3d-4188-bd36-2a5dbd109163', 350.00, 'sent', '2025-08-30 13:42:32.359848+00');
insert into public.expenses (id, tenant_id, category, amount, spent_at) values
  ('a9c47778-6ab2-4933-ad18-5a000f444cfe', '055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Fuel', 45.00, '2025-08-30'),
  ('81634274-4d0f-435e-94a4-96e9fbd131e7', '055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Equipment', 120.00, '2025-08-29'),
  ('93439a89-8800-43cc-b863-63e73f3f2202', '055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Supplies', 18.50, '2025-08-28'),
  ('33153a35-5338-4e6b-bb61-88077b51ba20', '055bdb3c-c8d0-47d4-aa70-a77739054d7e', 'Maintenance', 75.00, '2025-08-27');

-- The other app's data must survive untouched; add one row to prove it.
insert into public.scenarios (id, user_id, name, payload) values
  ('5ce00000-0000-0000-0000-000000000001', '56942ef9-b819-4343-9b35-dedcd4c854bd', 'other app row', '{"k":1}');
