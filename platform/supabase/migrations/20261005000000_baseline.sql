-- Baseline: the schema as described in the project handoff (2026-10-05).
--
-- Every statement is IF NOT EXISTS, so on the existing production project this
-- file changes nothing; on a fresh project (staging, local, CI) it creates the
-- starting tables. Once Supabase is connected, this file will be checked against
-- the real production schema and corrected to match it exactly.
--
-- No policies here: 20261005000100_foundation.sql replaces all policies.

create table if not exists public.tenants (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  plan text not null default 'trial',
  created_at timestamptz not null default now()
);

create table if not exists public.profiles (
  id uuid primary key references auth.users (id) on delete cascade,
  full_name text,
  created_at timestamptz not null default now()
);

create table if not exists public.memberships (
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  user_id uuid not null references auth.users (id) on delete cascade,
  role text not null,
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
  client_id uuid references public.clients (id),
  title text not null,
  schedule jsonb,
  crew_id uuid,
  status text not null default 'scheduled',
  created_at timestamptz not null default now()
);

create table if not exists public.time_entries (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  user_id uuid references auth.users (id),
  job_id uuid references public.jobs (id),
  clock_in timestamptz not null,
  clock_out timestamptz,
  notes text,
  created_at timestamptz not null default now()
);

create table if not exists public.invoices (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  client_id uuid references public.clients (id),
  total numeric(12,2) not null default 0,
  status text not null default 'draft',
  pdf_url text,
  issued_at timestamptz,
  due_at timestamptz
);

create table if not exists public.expenses (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  category text not null,
  amount numeric(12,2) not null,
  spent_at timestamptz not null default now(),
  note text
);
