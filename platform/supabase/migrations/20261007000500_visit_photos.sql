-- Visit photos (before / after / issue) taken by crews.
--
-- Files live in the private Storage bucket "visit-photos" at
--   <tenant_id>/<visit_id>/<file name>
-- Every read and upload is checked by the database:
--   * office (owner/admin): all of their company's photos
--   * crew: photos on visits they are assigned to
--   * customers: only photos the office marked customer-visible, on their own
--     completed visits (through portal_visit_photos and signed links)
-- Photos are never deleted by the app; the office can hide one.

insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values ('visit-photos', 'visit-photos', false, 15728640, array['image/jpeg','image/png','image/heic','image/heif','image/webp'])
on conflict (id) do nothing;

create table if not exists public.visit_photos (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete restrict,
  visit_id uuid not null,
  client_id uuid,
  storage_path text not null unique,
  kind text not null default 'after' check (kind in ('before','after','issue','other')),
  caption text check (caption is null or length(caption) <= 300),
  customer_visible boolean not null default false,
  taken_by uuid references auth.users (id) on delete set null,
  taken_at timestamptz not null default now(),
  hidden_at timestamptz,
  hidden_by uuid references auth.users (id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, id),
  foreign key (tenant_id, visit_id) references public.visits (tenant_id, id),
  foreign key (tenant_id, client_id) references public.clients (tenant_id, id)
);
create index if not exists visit_photos_visit_idx on public.visit_photos (visit_id);
create index if not exists visit_photos_client_idx on public.visit_photos (client_id, taken_at desc);

drop trigger if exists set_updated_at on public.visit_photos;
create trigger set_updated_at before update on public.visit_photos for each row execute function private.set_updated_at();
drop trigger if exists prevent_tenant_change on public.visit_photos;
create trigger prevent_tenant_change before update of tenant_id on public.visit_photos for each row execute function private.prevent_tenant_change();
drop trigger if exists audit_row_change on public.visit_photos;
create trigger audit_row_change after insert or update or delete on public.visit_photos for each row execute function private.audit_row_change();

alter table public.visit_photos enable row level security;
drop policy if exists visit_photos_select on public.visit_photos;
create policy visit_photos_select on public.visit_photos for select to authenticated
  using (public.is_admin_or_owner(tenant_id) or private.is_assigned_to_visit(visit_id));
revoke all on public.visit_photos from anon, authenticated;
grant select on public.visit_photos to authenticated;
grant all on public.visit_photos to service_role;

-- ---------------------------------------------------------------------------
-- Storage checks
-- ---------------------------------------------------------------------------
create or replace function private.can_upload_visit_photo(p_name text) returns boolean
language plpgsql stable security definer set search_path = '' as $$
declare v_tenant uuid; v_visit uuid;
begin
  if p_name is null or p_name !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/[A-Za-z0-9._-]{1,100}$' then
    return false;
  end if;
  v_tenant := split_part(p_name, '/', 1)::uuid;
  v_visit := split_part(p_name, '/', 2)::uuid;
  return exists (select 1 from public.visits v where v.id = v_visit and v.tenant_id = v_tenant and v.status <> 'canceled')
     and (public.is_admin_or_owner(v_tenant) or private.is_assigned_to_visit(v_visit));
end $$;

create or replace function private.can_view_visit_photo(p_name text) returns boolean
language sql stable security definer set search_path = '' as $$
  select exists (
    select 1 from public.visit_photos p
    where p.storage_path = p_name
      and (
        public.is_admin_or_owner(p.tenant_id)
        or private.is_assigned_to_visit(p.visit_id)
        or (p.customer_visible and p.hidden_at is null
            and exists (select 1 from public.visits v where v.id = p.visit_id and v.status = 'completed')
            and exists (select 1 from public.portal_access a
                        where a.client_id = p.client_id and a.tenant_id = p.tenant_id
                          and a.user_id = (select auth.uid()) and a.status = 'active'))
      )
  )
$$;

drop policy if exists visit_photos_upload on storage.objects;
drop policy if exists visit_photos_read on storage.objects;
create policy visit_photos_upload on storage.objects for insert to authenticated
  with check (bucket_id = 'visit-photos' and private.can_upload_visit_photo(name));
create policy visit_photos_read on storage.objects for select to authenticated
  using (bucket_id = 'visit-photos' and (private.can_view_visit_photo(name) or (owner = (select auth.uid()) and private.can_upload_visit_photo(name))));
-- No update/delete policies: photos are kept.

-- ---------------------------------------------------------------------------
-- Actions
-- ---------------------------------------------------------------------------
create or replace function public.add_visit_photo(p_visit_id uuid, p_path text, p_kind text default 'after', p_caption text default null)
returns uuid
language plpgsql security definer set search_path = '' as $$
declare v public.visits; v_id uuid;
begin
  select * into v from public.visits where id = p_visit_id;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  if not (public.is_admin_or_owner(v.tenant_id) or private.is_assigned_to_visit(v.id)) then
    raise exception 'forbidden' using errcode = '42501';
  end if;
  if p_path is null or split_part(p_path, '/', 1) <> v.tenant_id::text or split_part(p_path, '/', 2) <> v.id::text
     or not private.can_upload_visit_photo(p_path) then
    raise exception 'invalid_photo_path' using errcode = '22023';
  end if;
  if not exists (select 1 from storage.objects o where o.bucket_id = 'visit-photos' and o.name = p_path) then
    raise exception 'photo_not_uploaded' using errcode = '22023';
  end if;
  if p_kind not in ('before','after','issue','other') then raise exception 'invalid_kind' using errcode = '22023'; end if;
  insert into public.visit_photos (tenant_id, visit_id, client_id, storage_path, kind, caption, taken_by)
  values (v.tenant_id, v.id, v.client_id, p_path, p_kind, nullif(btrim(coalesce(p_caption, '')), ''), auth.uid())
  on conflict (storage_path) do nothing
  returning id into v_id;
  if v_id is null then select id into v_id from public.visit_photos where storage_path = p_path; end if;
  return v_id;
end $$;

create or replace function public.set_photo_visibility(p_photo_id uuid, p_customer_visible boolean) returns void
language plpgsql security definer set search_path = '' as $$
declare p public.visit_photos;
begin
  select * into p from public.visit_photos where id = p_photo_id;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(p.tenant_id);
  update public.visit_photos set customer_visible = coalesce(p_customer_visible, false) where id = p.id;
end $$;

create or replace function public.hide_visit_photo(p_photo_id uuid, p_hidden boolean default true) returns void
language plpgsql security definer set search_path = '' as $$
declare p public.visit_photos;
begin
  select * into p from public.visit_photos where id = p_photo_id;
  if not found then raise exception 'not_found' using errcode = 'P0002'; end if;
  perform private.require_manager(p.tenant_id);
  update public.visit_photos
     set hidden_at = case when p_hidden then now() end,
         hidden_by = case when p_hidden then auth.uid() end,
         customer_visible = case when p_hidden then false else customer_visible end
   where id = p.id;
end $$;

-- Customer: photos the office chose to share, on their own completed visits.
create or replace function public.portal_visit_photos(p_client_id uuid, p_visit_id uuid default null)
returns table (photo_id uuid, visit_id uuid, visit_date date, service text, kind text, caption text, storage_path text, taken_at timestamptz)
language plpgsql stable security definer set search_path = '' as $$
begin
  perform private.require_portal_client(p_client_id);
  return query
  select p.id, p.visit_id, v.scheduled_date, coalesce(s.name, j.title), p.kind, p.caption, p.storage_path, p.taken_at
  from public.visit_photos p
  join public.visits v on v.id = p.visit_id and v.status = 'completed'
  join public.jobs j on j.id = v.job_id
  left join public.services s on s.id = j.service_id
  where p.client_id = p_client_id and p.customer_visible and p.hidden_at is null
    and (p_visit_id is null or p.visit_id = p_visit_id)
  order by v.scheduled_date desc, p.kind desc, p.taken_at
  limit 200;
end $$;

revoke execute on function private.can_upload_visit_photo(text), private.can_view_visit_photo(text) from public, anon;
grant execute on function private.can_upload_visit_photo(text), private.can_view_visit_photo(text) to authenticated;  -- used in storage policies
do $$
declare f text;
begin
  foreach f in array array['public.add_visit_photo(uuid, text, text, text)', 'public.set_photo_visibility(uuid, boolean)',
                           'public.hide_visit_photo(uuid, boolean)', 'public.portal_visit_photos(uuid, uuid)'] loop
    execute format('revoke execute on function %s from public, anon', f);
    execute format('grant execute on function %s to authenticated, service_role', f);
  end loop;
end $$;
