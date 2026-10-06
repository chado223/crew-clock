-- The web app saves contact preferences as an upsert (PostgREST writes
-- ON CONFLICT DO UPDATE SET client_id = ...), so client_id needs the update
-- grant. A preference row can never move to a different customer.

grant update (client_id) on public.contact_preferences to authenticated;

create or replace function private.contact_preferences_pin_client() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  if new.client_id is distinct from old.client_id then
    raise exception 'client_change_not_allowed' using errcode = '42501';
  end if;
  return new;
end $$;
drop trigger if exists contact_preferences_pin_client on public.contact_preferences;
create trigger contact_preferences_pin_client before update of client_id on public.contact_preferences
  for each row execute function private.contact_preferences_pin_client();
revoke execute on function private.contact_preferences_pin_client() from public, anon, authenticated;
