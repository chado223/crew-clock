-- Baseline: the production schema as it actually exists in Supabase project
-- iwowjrnrbjiydckhjsfi (inspected 2026-10-05, Postgres 17).
--
-- Every statement is IF NOT EXISTS / guarded, so on production this file is a
-- no-op; on staging, local and CI it recreates production's starting point.
-- Policies and helper functions are NOT here: production's versions are
-- reproduced in tests/fixtures/10_legacy_existing_state.sql, and
-- 20261005000100_foundation.sql replaces all of them.
--
-- Note: public.scenarios and profiles.email/stripe_customer_id/is_pro belong to
-- a different app sharing this project. They are reproduced so tests prove the
-- migrations leave them working.

do $$ begin
  create type public.user_role as enum ('owner', 'admin', 'crew');
exception when duplicate_object then null; end $$;

create table if not exists public.tenants (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  plan text not null default 'free',
  created_at timestamptz not null default now()
);

create table if not exists public.profiles (
  id uuid primary key references auth.users (id) on delete cascade,
  email text,
  stripe_customer_id text,
  is_pro boolean default false,
  created_at timestamptz default now()
);

create table if not exists public.scenarios (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references auth.users (id) on delete cascade,
  name text,
  payload jsonb,
  created_at timestamptz default now()
);

create table if not exists public.memberships (
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  user_id uuid not null references auth.users (id) on delete cascade,
  role public.user_role not null default 'crew',
  created_at timestamptz not null default now(),
  primary key (tenant_id, user_id)
);

create table if not exists public.clients (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  name text not null,
  email text,
  phone text,
  address text,
  created_at timestamptz not null default now()
);

create table if not exists public.jobs (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  client_id uuid references public.clients (id) on delete set null,
  title text not null,
  schedule jsonb,
  crew_id uuid,
  status text not null default 'scheduled',
  created_at timestamptz not null default now()
);

create table if not exists public.time_entries (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  user_id uuid not null references auth.users (id) on delete cascade,
  job_id uuid references public.jobs (id) on delete set null,
  clock_in timestamptz not null,
  clock_out timestamptz,
  notes text,
  created_at timestamptz not null default now()
);

create table if not exists public.invoices (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  client_id uuid not null references public.clients (id) on delete cascade,
  total numeric not null default 0,
  status text not null default 'draft',
  pdf_url text,
  issued_at timestamptz not null default now(),
  due_at timestamptz
);

create table if not exists public.expenses (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  category text not null,
  amount numeric not null,
  spent_at date not null default current_date,
  note text
);

alter table public.tenants enable row level security;
alter table public.profiles enable row level security;
alter table public.scenarios enable row level security;
alter table public.memberships enable row level security;
alter table public.clients enable row level security;
alter table public.jobs enable row level security;
alter table public.time_entries enable row level security;
alter table public.invoices enable row level security;
alter table public.expenses enable row level security;

-- The other app's policies on its own table (production, unchanged by this platform)
do $$ begin
  create policy "scenarios owner" on public.scenarios for select using (auth.uid() = user_id);
exception when duplicate_object then null; end $$;
do $$ begin
  create policy "scenarios write owner" on public.scenarios for insert with check (auth.uid() = user_id);
exception when duplicate_object then null; end $$;
