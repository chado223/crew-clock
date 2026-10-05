-- Time clock: punches, breaks, corrections, and THE hours calculation.
-- See docs/decisions/0002-time-tracking-model.md.
--
-- Rules enforced here for every client (web, mobile, kiosk, imports):
--   * one open shift per employee; no overlapping shifts
--   * idempotent punches (client_event_id) so offline retries never duplicate
--   * employees can't edit time; owners/admins correct it with a required reason
--   * every change lands in audit_log with before/after
--   * hours are computed only by timesheet() and weekly_hours()

-- ---------------------------------------------------------------------------
-- Columns
-- ---------------------------------------------------------------------------
alter table public.time_entries
  add column if not exists source text,
  add column if not exists client_event_id uuid,
  add column if not exists clock_out_event_id uuid,
  add column if not exists received_at timestamptz,
  add column if not exists clock_out_received_at timestamptz,
  add column if not exists needs_review boolean not null default false,
  add column if not exists review_note text,
  add column if not exists created_by uuid references auth.users (id) on delete set null,
  add column if not exists updated_at timestamptz not null default now(),
  add column if not exists voided_at timestamptz,
  add column if not exists voided_by uuid references auth.users (id) on delete set null,
  add column if not exists void_reason text,
  add column if not exists legacy_ref text;

-- Rows that existed before this migration are labeled, never altered.
update public.time_entries set source = 'pre_migration' where source is null;
alter table public.time_entries alter column source set default 'app';
alter table public.time_entries alter column source set not null;
do $$ begin
  alter table public.time_entries add constraint time_entries_source_chk
    check (source in ('app','mobile','web','kiosk','admin','pre_migration','legacy_sqlite','legacy_sheets'));
exception when duplicate_object then null; end $$;

do $$ begin
  alter table public.time_entries add constraint time_entries_out_after_in_chk
    check (clock_out is null or clock_out > clock_in) not valid;
exception when duplicate_object then null; end $$;
do $$ begin
  alter table public.time_entries validate constraint time_entries_out_after_in_chk;
exception when check_violation then
  update public.time_entries set needs_review = true,
         review_note = coalesce(review_note, 'clock_out is not after clock_in')
   where clock_out is not null and clock_out <= clock_in;
  raise warning 'Some existing time entries end before they start; flagged needs_review (times unchanged).';
end $$;

drop trigger if exists set_updated_at on public.time_entries;
create trigger set_updated_at before update on public.time_entries
  for each row execute function private.set_updated_at();

-- Flag (never change) existing overlapping or duplicate-open shifts so the
-- integrity constraints below can be created over legacy data.
update public.time_entries a
set needs_review = true,
    review_note = coalesce(a.review_note, 'overlaps another shift for this employee')
where a.voided_at is null and a.employee_id is not null and exists (
  select 1 from public.time_entries b
  where b.id <> a.id and b.employee_id = a.employee_id and b.voided_at is null
    and tstzrange(a.clock_in, coalesce(a.clock_out, 'infinity')) && tstzrange(b.clock_in, coalesce(b.clock_out, 'infinity'))
);

create unique index if not exists time_entries_one_open_uidx on public.time_entries (employee_id)
  where clock_out is null and voided_at is null and not needs_review;
create unique index if not exists time_entries_client_event_uidx on public.time_entries (tenant_id, client_event_id)
  where client_event_id is not null;
create unique index if not exists time_entries_clock_out_event_uidx on public.time_entries (tenant_id, clock_out_event_id)
  where clock_out_event_id is not null;

do $$ begin
  alter table public.time_entries add constraint time_entries_no_overlap
    exclude using gist (employee_id with =, tstzrange(clock_in, coalesce(clock_out, 'infinity'::timestamptz)) with &&)
    where (voided_at is null and not needs_review);
exception when duplicate_object or duplicate_table then null; end $$;

-- ---------------------------------------------------------------------------
-- Breaks (unpaid unless paid = true)
-- ---------------------------------------------------------------------------
create table if not exists public.time_entry_breaks (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null,
  time_entry_id uuid not null,
  employee_id uuid not null,
  started_at timestamptz not null,
  ended_at timestamptz,
  paid boolean not null default false,
  client_event_id uuid,
  created_at timestamptz not null default now(),
  foreign key (tenant_id, time_entry_id) references public.time_entries (tenant_id, id),
  foreign key (tenant_id, employee_id) references public.employees (tenant_id, id),
  check (ended_at is null or ended_at > started_at)
);
create unique index if not exists time_entry_breaks_one_open_uidx on public.time_entry_breaks (time_entry_id) where ended_at is null;
create unique index if not exists time_entry_breaks_event_uidx on public.time_entry_breaks (tenant_id, client_event_id) where client_event_id is not null;
create index if not exists time_entry_breaks_entry_idx on public.time_entry_breaks (time_entry_id);

alter table public.time_entry_breaks enable row level security;
drop policy if exists time_entry_breaks_select on public.time_entry_breaks;
create policy time_entry_breaks_select on public.time_entry_breaks for select to authenticated
  using (public.is_admin_or_owner(tenant_id) or employee_id = public.my_employee_id(tenant_id));

drop trigger if exists prevent_tenant_change on public.time_entry_breaks;
create trigger prevent_tenant_change before update of tenant_id on public.time_entry_breaks
  for each row execute function private.prevent_tenant_change();
drop trigger if exists audit_row_change on public.time_entry_breaks;
create trigger audit_row_change after insert or update or delete on public.time_entry_breaks
  for each row execute function private.audit_row_change();

-- ---------------------------------------------------------------------------
-- Internal helpers
-- ---------------------------------------------------------------------------
-- Offline punches carry device time. Accept up to 72 h old, at most 5 min ahead.
create or replace function private.check_punch_time(p_at timestamptz) returns timestamptz
language plpgsql stable set search_path = '' as $$
declare v timestamptz := coalesce(p_at, now());
begin
  if v > now() + interval '5 minutes' then
    raise exception 'punch_in_future' using errcode = '22023';
  end if;
  if v < now() - interval '72 hours' then
    raise exception 'punch_too_old: ask a manager to add this time' using errcode = '22023';
  end if;
  return v;
end $$;

create or replace function private.require_self_employee(p_tenant_id uuid) returns uuid
language plpgsql stable security definer set search_path = '' as $$
declare v uuid;
begin
  if auth.uid() is null then
    raise exception 'not_authenticated' using errcode = '42501';
  end if;
  v := public.my_employee_id(p_tenant_id);
  if v is null then
    raise exception 'not_an_active_employee' using errcode = '42501';
  end if;
  return v;
end $$;

create or replace function private.require_manager(p_tenant_id uuid) returns void
language plpgsql stable security definer set search_path = '' as $$
begin
  if auth.uid() is null then
    raise exception 'not_authenticated' using errcode = '42501';
  end if;
  if not public.is_admin_or_owner(p_tenant_id) then
    raise exception 'forbidden' using errcode = '42501';
  end if;
end $$;

create or replace function private.require_reason(p_reason text) returns text
language plpgsql immutable set search_path = '' as $$
begin
  if p_reason is null or length(trim(p_reason)) < 3 then
    raise exception 'reason_required' using errcode = '22023';
  end if;
  return trim(p_reason);
end $$;

-- ---------------------------------------------------------------------------
-- Employee punches
-- ---------------------------------------------------------------------------
create or replace function public.clock_in(
  p_tenant_id uuid,
  p_client_event_id uuid default null,
  p_at timestamptz default null,
  p_job_id uuid default null,
  p_notes text default null,
  p_source text default 'app'
) returns public.time_entries
language plpgsql security definer set search_path = '' as $$
declare
  v_emp uuid := private.require_self_employee(p_tenant_id);
  v_at timestamptz;
  v_row public.time_entries;
begin
  if p_client_event_id is not null then
    select * into v_row from public.time_entries
     where tenant_id = p_tenant_id and client_event_id = p_client_event_id;
    if found then
      if v_row.employee_id <> v_emp then raise exception 'forbidden' using errcode = '42501'; end if;
      return v_row;                                   -- retry of an already-recorded punch
    end if;
  end if;

  if p_source not in ('app','mobile','web','kiosk') then
    raise exception 'invalid_source' using errcode = '22023';
  end if;
  v_at := private.check_punch_time(p_at);

  if exists (select 1 from public.time_entries where employee_id = v_emp
             and clock_out is null and voided_at is null and not needs_review) then
    raise exception 'already_clocked_in' using errcode = '23P01';
  end if;

  perform set_config('app.audit_reason', 'clock_in', true);
  begin
    insert into public.time_entries
      (tenant_id, user_id, employee_id, job_id, clock_in, notes, source, client_event_id, received_at, created_by)
    values
      (p_tenant_id, auth.uid(), v_emp, p_job_id, v_at, p_notes, p_source, p_client_event_id, now(), auth.uid())
    returning * into v_row;
  exception
    when unique_violation then
      if p_client_event_id is not null then
        select * into v_row from public.time_entries where tenant_id = p_tenant_id and client_event_id = p_client_event_id;
        if found and v_row.employee_id = v_emp then return v_row; end if;
      end if;
      raise exception 'already_clocked_in' using errcode = '23P01';
    when exclusion_violation then
      raise exception 'overlaps_existing_shift' using errcode = '23P01';
  end;
  return v_row;
end $$;

create or replace function public.clock_out(
  p_tenant_id uuid,
  p_client_event_id uuid default null,
  p_at timestamptz default null,
  p_notes text default null
) returns public.time_entries
language plpgsql security definer set search_path = '' as $$
declare
  v_emp uuid := private.require_self_employee(p_tenant_id);
  v_at timestamptz;
  v_row public.time_entries;
begin
  if p_client_event_id is not null then
    select * into v_row from public.time_entries
     where tenant_id = p_tenant_id and clock_out_event_id = p_client_event_id;
    if found then
      if v_row.employee_id <> v_emp then raise exception 'forbidden' using errcode = '42501'; end if;
      return v_row;
    end if;
  end if;

  v_at := private.check_punch_time(p_at);

  select * into v_row from public.time_entries
   where employee_id = v_emp and clock_out is null and voided_at is null and not needs_review
   for update;
  if not found then
    raise exception 'not_clocked_in' using errcode = '22023';
  end if;
  if v_at <= v_row.clock_in then
    raise exception 'clock_out_before_clock_in' using errcode = '22023';
  end if;

  perform set_config('app.audit_reason', 'clock_out', true);
  update public.time_entry_breaks set ended_at = greatest(v_at, started_at + interval '1 second')
   where time_entry_id = v_row.id and ended_at is null;

  update public.time_entries
     set clock_out = v_at,
         clock_out_event_id = p_client_event_id,
         clock_out_received_at = now(),
         notes = coalesce(p_notes, notes)
   where id = v_row.id
  returning * into v_row;
  return v_row;
end $$;

create or replace function public.start_break(
  p_tenant_id uuid, p_client_event_id uuid default null, p_at timestamptz default null, p_paid boolean default false
) returns public.time_entry_breaks
language plpgsql security definer set search_path = '' as $$
declare
  v_emp uuid := private.require_self_employee(p_tenant_id);
  v_at timestamptz;
  v_entry public.time_entries;
  v_row public.time_entry_breaks;
begin
  if p_client_event_id is not null then
    select * into v_row from public.time_entry_breaks where tenant_id = p_tenant_id and client_event_id = p_client_event_id;
    if found and v_row.employee_id = v_emp then return v_row; end if;
  end if;
  v_at := private.check_punch_time(p_at);
  select * into v_entry from public.time_entries
   where employee_id = v_emp and clock_out is null and voided_at is null and not needs_review;
  if not found then raise exception 'not_clocked_in' using errcode = '22023'; end if;
  if v_at < v_entry.clock_in then raise exception 'break_before_clock_in' using errcode = '22023'; end if;
  begin
    insert into public.time_entry_breaks (tenant_id, time_entry_id, employee_id, started_at, paid, client_event_id)
    values (p_tenant_id, v_entry.id, v_emp, v_at, p_paid, p_client_event_id)
    returning * into v_row;
  exception when unique_violation then
    raise exception 'already_on_break' using errcode = '23P01';
  end;
  return v_row;
end $$;

create or replace function public.end_break(
  p_tenant_id uuid, p_at timestamptz default null
) returns public.time_entry_breaks
language plpgsql security definer set search_path = '' as $$
declare
  v_emp uuid := private.require_self_employee(p_tenant_id);
  v_at timestamptz := private.check_punch_time(p_at);
  v_row public.time_entry_breaks;
begin
  select b.* into v_row from public.time_entry_breaks b
   where b.employee_id = v_emp and b.tenant_id = p_tenant_id and b.ended_at is null
   for update;
  if not found then raise exception 'not_on_break' using errcode = '22023'; end if;
  if v_at <= v_row.started_at then raise exception 'break_end_before_start' using errcode = '22023'; end if;
  update public.time_entry_breaks set ended_at = v_at where id = v_row.id returning * into v_row;
  return v_row;
end $$;

-- ---------------------------------------------------------------------------
-- Manager corrections (reason required, fully audited, nothing deleted)
-- ---------------------------------------------------------------------------
create or replace function public.correct_time_entry(
  p_entry_id uuid, p_clock_in timestamptz, p_clock_out timestamptz, p_reason text
) returns public.time_entries
language plpgsql security definer set search_path = '' as $$
declare
  v_row public.time_entries;
  v_reason text := private.require_reason(p_reason);
begin
  select * into v_row from public.time_entries where id = p_entry_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(v_row.tenant_id);
  if v_row.voided_at is not null then raise exception 'entry_voided' using errcode = '22023'; end if;
  if p_clock_in is null then raise exception 'clock_in_required' using errcode = '22023'; end if;
  if p_clock_out is not null and p_clock_out <= p_clock_in then
    raise exception 'clock_out_before_clock_in' using errcode = '22023';
  end if;
  if p_clock_in > now() + interval '5 minutes' or p_clock_out > now() + interval '5 minutes' then
    raise exception 'punch_in_future' using errcode = '22023';
  end if;

  perform set_config('app.audit_reason', 'correct_time_entry: ' || v_reason, true);
  begin
    update public.time_entries
       set clock_in = p_clock_in, clock_out = p_clock_out,
           needs_review = false, review_note = null
     where id = p_entry_id
    returning * into v_row;
  exception
    when exclusion_violation then raise exception 'overlaps_existing_shift' using errcode = '23P01';
    when unique_violation then raise exception 'employee_already_has_open_shift' using errcode = '23P01';
  end;
  return v_row;
end $$;

create or replace function public.add_time_entry(
  p_tenant_id uuid, p_employee_id uuid, p_clock_in timestamptz, p_clock_out timestamptz,
  p_reason text, p_job_id uuid default null, p_notes text default null
) returns public.time_entries
language plpgsql security definer set search_path = '' as $$
declare
  v_reason text := private.require_reason(p_reason);
  v_row public.time_entries;
begin
  perform private.require_manager(p_tenant_id);
  if not exists (select 1 from public.employees where id = p_employee_id and tenant_id = p_tenant_id) then
    raise exception 'employee_not_found' using errcode = 'P0002';
  end if;
  if p_clock_in is null or p_clock_out is null then
    raise exception 'clock_in_and_clock_out_required' using errcode = '22023';
  end if;
  if p_clock_out <= p_clock_in then raise exception 'clock_out_before_clock_in' using errcode = '22023'; end if;
  if p_clock_out > now() + interval '5 minutes' then raise exception 'punch_in_future' using errcode = '22023'; end if;

  perform set_config('app.audit_reason', 'add_time_entry: ' || v_reason, true);
  begin
    insert into public.time_entries
      (tenant_id, employee_id, user_id, job_id, clock_in, clock_out, notes, source, received_at, clock_out_received_at, created_by)
    values
      (p_tenant_id, p_employee_id, (select user_id from public.employees where id = p_employee_id),
       p_job_id, p_clock_in, p_clock_out, p_notes, 'admin', now(), now(), auth.uid())
    returning * into v_row;
  exception when exclusion_violation then
    raise exception 'overlaps_existing_shift' using errcode = '23P01';
  end;
  return v_row;
end $$;

create or replace function public.void_time_entry(p_entry_id uuid, p_reason text) returns public.time_entries
language plpgsql security definer set search_path = '' as $$
declare
  v_reason text := private.require_reason(p_reason);
  v_row public.time_entries;
begin
  select * into v_row from public.time_entries where id = p_entry_id for update;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(v_row.tenant_id);
  if v_row.voided_at is not null then return v_row; end if;
  perform set_config('app.audit_reason', 'void_time_entry: ' || v_reason, true);
  update public.time_entries
     set voided_at = now(), voided_by = auth.uid(), void_reason = v_reason
   where id = p_entry_id
  returning * into v_row;
  return v_row;
end $$;

-- ---------------------------------------------------------------------------
-- THE hours calculation. Every screen, export and report uses these.
-- SECURITY INVOKER: callers see only rows RLS allows (crew: their own).
-- ---------------------------------------------------------------------------
create or replace function public.timesheet(
  p_tenant_id uuid, p_from date, p_to date, p_include_voided boolean default false
) returns table (
  entry_id uuid,
  employee_id uuid,
  employee_name text,
  work_date date,
  clock_in timestamptz,
  clock_out timestamptz,
  break_seconds bigint,
  worked_seconds bigint,
  status text,
  job_id uuid,
  source text
)
language sql stable security invoker set search_path = '' as $$
  with tz as (
    select t.timezone from public.tenants t where t.id = p_tenant_id
  )
  select
    te.id,
    te.employee_id,
    e.display_name,
    (te.clock_in at time zone tz.timezone)::date,
    te.clock_in,
    te.clock_out,
    coalesce(b.unpaid_seconds, 0)::bigint,
    case when te.clock_out is null or te.voided_at is not null then null
         else greatest(0, extract(epoch from (te.clock_out - te.clock_in))::bigint - coalesce(b.unpaid_seconds, 0)::bigint)
    end,
    case when te.voided_at is not null then 'voided'
         when te.needs_review then 'needs_review'
         when te.clock_out is null then 'open'
         else 'closed' end,
    te.job_id,
    te.source
  from public.time_entries te
  cross join tz
  left join public.employees e on e.id = te.employee_id
  left join lateral (
    select sum(extract(epoch from (coalesce(br.ended_at, te.clock_out) - br.started_at)))::bigint as unpaid_seconds
    from public.time_entry_breaks br
    where br.time_entry_id = te.id and not br.paid and coalesce(br.ended_at, te.clock_out) is not null
  ) b on true
  where te.tenant_id = p_tenant_id
    and te.clock_in >= (p_from::timestamp at time zone tz.timezone)
    and te.clock_in <  ((p_to + 1)::timestamp at time zone tz.timezone)
    and (p_include_voided or te.voided_at is null)
  order by te.clock_in
$$;

create or replace function public.weekly_hours(p_tenant_id uuid, p_week_start date)
returns table (
  employee_id uuid,
  employee_name text,
  week_start date,
  week_end date,
  total_seconds bigint,
  regular_seconds bigint,
  overtime_seconds bigint,
  total_hours numeric,
  regular_hours numeric,
  overtime_hours numeric,
  closed_shifts integer,
  open_shifts integer,
  needs_review_shifts integer
)
language plpgsql stable security invoker set search_path = '' as $$
declare
  v_tenant public.tenants;
  v_threshold bigint;
begin
  select * into v_tenant from public.tenants where id = p_tenant_id;
  if not found then return; end if;                       -- not visible to caller
  if extract(isodow from p_week_start) <> v_tenant.week_start_day then
    raise exception 'week_start_mismatch: this company''s week starts on ISO day %', v_tenant.week_start_day
      using errcode = '22023';
  end if;
  v_threshold := (v_tenant.overtime_weekly_hours * 3600)::bigint;

  return query
  with s as (
    select * from public.timesheet(p_tenant_id, p_week_start, p_week_start + 6)
  ), agg as (
    select s.employee_id, max(s.employee_name) as employee_name,
           coalesce(sum(s.worked_seconds) filter (where s.status in ('closed','needs_review')), 0)::bigint as total,
           count(*) filter (where s.status = 'closed')::int as closed_n,
           count(*) filter (where s.status = 'open')::int as open_n,
           count(*) filter (where s.status = 'needs_review')::int as review_n
    from s group by s.employee_id
  )
  select a.employee_id, a.employee_name, p_week_start, p_week_start + 6,
         a.total,
         least(a.total, v_threshold),
         greatest(a.total - v_threshold, 0),
         round(a.total / 3600.0, 2),
         round(least(a.total, v_threshold) / 3600.0, 2),
         round(greatest(a.total - v_threshold, 0) / 3600.0, 2),
         a.closed_n, a.open_n, a.review_n
  from agg a
  order by a.employee_name;
end $$;
