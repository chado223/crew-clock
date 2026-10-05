-- Tiny assertion library for SQL tests (no pgTAP dependency).
-- Test-only schema; never applied to a Supabase project.

create schema if not exists tests;
create table if not exists tests.results (
  id serial primary key,
  ok boolean not null,
  name text not null,
  detail text
);
grant usage on schema tests to anon, authenticated, service_role;
grant all on tests.results to anon, authenticated, service_role;
grant usage on sequence tests.results_id_seq to anon, authenticated, service_role;

-- Become a signed-in user (then: SET ROLE authenticated). NULL = anonymous.
create or replace function tests.login(p_user uuid, p_email text default null) returns void
language plpgsql as $$
begin
  if p_user is null then
    perform set_config('request.jwt.claims', '{"role":"anon"}', false);
  else
    perform set_config('request.jwt.claims',
      jsonb_build_object('sub', p_user, 'role', 'authenticated',
        'email', coalesce(p_email, (select email from auth.users where id = p_user)))::text, false);
  end if;
end $$;

create or replace function tests.ok(p_cond boolean, p_name text, p_detail text default null) returns void
language plpgsql as $$
begin
  insert into tests.results (ok, name, detail) values (coalesce(p_cond, false), p_name, p_detail);
end $$;

create or replace function tests.is(p_got anyelement, p_want anyelement, p_name text) returns void
language plpgsql as $$
begin
  insert into tests.results (ok, name, detail)
  values (p_got is not distinct from p_want, p_name,
          case when p_got is distinct from p_want then format('got %s, want %s', p_got, p_want) end);
end $$;

-- Expect the statement to raise an error whose message matches p_pattern (ILIKE).
create or replace function tests.throws(p_sql text, p_pattern text, p_name text) returns void
language plpgsql as $$
begin
  begin
    execute p_sql;
  exception when others then
    insert into tests.results (ok, name, detail)
    values (sqlerrm ilike p_pattern, p_name,
            case when not (sqlerrm ilike p_pattern) then 'raised: ' || sqlerrm end);
    return;
  end;
  insert into tests.results (ok, name, detail) values (false, p_name, 'did not raise');
end $$;

-- Expect the statement to succeed.
create or replace function tests.lives(p_sql text, p_name text) returns void
language plpgsql as $$
begin
  execute p_sql;
  insert into tests.results (ok, name) values (true, p_name);
exception when others then
  insert into tests.results (ok, name, detail) values (false, p_name, 'raised: ' || sqlerrm);
end $$;

-- Number of rows a statement affects (for UPDATE/DELETE checks under RLS).
create or replace function tests.affected(p_sql text) returns bigint
language plpgsql as $$
declare n bigint;
begin
  execute p_sql;
  get diagnostics n = row_count;
  return n;
exception when insufficient_privilege then
  return 0;
end $$;

-- Count rows visible from a query.
create or replace function tests.count(p_sql text) returns bigint
language plpgsql as $$
declare n bigint;
begin
  execute format('select count(*) from (%s) q', p_sql) into n;
  return n;
exception when insufficient_privilege then
  return 0;
end $$;

grant execute on all functions in schema tests to anon, authenticated, service_role;
