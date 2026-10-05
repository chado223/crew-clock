-- Customer history must keep the real order of events, even when several are
-- written in one transaction (e.g. approve + convert). now() is fixed per
-- transaction, so entries tied and sorted arbitrarily.
--   * system entries use the actual clock time
--   * a sequence number breaks any remaining ties

alter table public.activity add column if not exists seq bigint generated always as identity;
create index if not exists activity_client_timeline_seq_idx on public.activity (tenant_id, client_id, occurred_at desc, seq desc);

create or replace function private.log_activity(
  p_tenant uuid, p_client uuid, p_property uuid, p_kind text, p_summary text, p_data jsonb default '{}'::jsonb
) returns void
language plpgsql security definer set search_path = '' as $$
begin
  insert into public.activity (tenant_id, client_id, property_id, kind, summary, data, actor_user_id, occurred_at)
  values (p_tenant, p_client, p_property, p_kind, p_summary, coalesce(p_data, '{}'::jsonb), auth.uid(), clock_timestamp());
end $$;
revoke execute on function private.log_activity(uuid, uuid, uuid, text, text, jsonb) from public, anon, authenticated;
