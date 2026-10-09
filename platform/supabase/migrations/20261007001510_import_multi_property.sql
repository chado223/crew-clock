-- CSV import: a customer listed on several rows with different service
-- addresses (landlords, commercial accounts) gets one customer record and a
-- property (and job) per address, instead of the extra rows being skipped.

create or replace function public.import_customers(p_tenant_id uuid, p_rows jsonb, p_dry_run boolean default true)
returns jsonb
language plpgsql security definer set search_path = '' as $$
declare
  r jsonb;
  i integer := 0;
  v_client uuid;
  v_property uuid;
  v_job uuid;
  v_freq integer;
  v_day integer;
  v_start date;
  v_price numeric;
  v_sqft integer;
  v_crew uuid;
  v_email text;
  v_phone text;
  v_name text;
  v_addr text;
  v_dup text;
  v_today date;
  n_clients integer := 0;
  n_props integer := 0;
  n_jobs integer := 0;
  v_skipped jsonb := '[]';
  v_errors jsonb := '[]';
  v_seen jsonb := '{}';   -- customer key -> client id created from this file
  v_addr_seen text[] := '{}';
  v_key text;
  v_summary jsonb;
begin
  perform private.require_manager(p_tenant_id);
  if jsonb_typeof(p_rows) <> 'array' then raise exception 'invalid_import' using errcode = '22023'; end if;
  if jsonb_array_length(p_rows) > 2000 then raise exception 'import_too_large' using errcode = '22023'; end if;
  v_today := private.tenant_today(p_tenant_id);

  begin  -- everything below is undone at the end of a dry run
    perform set_config('app.audit_reason', 'CSV import', true);
    for r in select value from jsonb_array_elements(p_rows) loop
      i := i + 1;
      v_name := nullif(btrim(r->>'name'), '');
      v_email := lower(nullif(btrim(r->>'email'), ''));
      v_phone := nullif(regexp_replace(coalesce(r->>'phone', ''), '[^0-9]', '', 'g'), '');
      v_addr := nullif(btrim(r->>'address_line1'), '');

      -- Row checks
      if v_name is null then
        v_errors := v_errors || jsonb_build_object('row', i, 'reason', 'Name is missing'); continue;
      end if;
      if v_email is not null and v_email !~ '^[^@\s]+@[^@\s]+\.[^@\s]+$' then
        v_errors := v_errors || jsonb_build_object('row', i, 'reason', 'Email doesn''t look right: ' || v_email); continue;
      end if;
      v_price := null;
      if nullif(btrim(r->>'price'), '') is not null then
        begin
          v_price := round(regexp_replace(r->>'price', '[$,\s]', '', 'g')::numeric, 2);
          if v_price < 0 then raise exception 'neg'; end if;
        exception when others then
          v_errors := v_errors || jsonb_build_object('row', i, 'reason', 'Price isn''t a number: ' || (r->>'price')); continue;
        end;
      end if;
      v_sqft := null;
      if nullif(btrim(r->>'lawn_sqft'), '') is not null then
        begin
          v_sqft := round(regexp_replace(r->>'lawn_sqft', '[,\s]|sq ?ft', '', 'gi')::numeric)::int;
        exception when others then
          v_errors := v_errors || jsonb_build_object('row', i, 'reason', 'Lawn size isn''t a number: ' || (r->>'lawn_sqft')); continue;
        end;
      end if;
      v_freq := private.parse_frequency(r->>'frequency');
      if v_freq = -1 then
        v_errors := v_errors || jsonb_build_object('row', i, 'reason', 'Frequency not understood (use weekly, biweekly, every 3 weeks, monthly or once): ' || (r->>'frequency')); continue;
      end if;
      v_start := null;
      if nullif(btrim(r->>'start_date'), '') is not null then
        begin
          v_start := (r->>'start_date')::date;
        exception when others then
          v_errors := v_errors || jsonb_build_object('row', i, 'reason', 'Start date not understood (use YYYY-MM-DD or MM/DD/YYYY): ' || (r->>'start_date')); continue;
        end;
      end if;
      v_day := private.parse_weekday(r->>'day');
      if nullif(btrim(r->>'day'), '') is not null and v_day is null then
        v_errors := v_errors || jsonb_build_object('row', i, 'reason', 'Day not understood: ' || (r->>'day')); continue;
      end if;
      v_crew := null;
      if nullif(btrim(r->>'crew'), '') is not null then
        select id into v_crew from public.crews where tenant_id = p_tenant_id and lower(name) = lower(btrim(r->>'crew')) and active;
        if v_crew is null then
          v_errors := v_errors || jsonb_build_object('row', i, 'reason', 'No crew named "' || btrim(r->>'crew') || '"'); continue;
        end if;
      end if;
      if (nullif(btrim(r->>'service'), '') is not null or v_freq is not null) and v_addr is null then
        v_errors := v_errors || jsonb_build_object('row', i, 'reason', 'Work needs a service address'); continue;
      end if;

      -- Duplicates: in this file, then already in the company
      v_key := coalesce(v_email, v_phone, lower(v_name) || '|' || lower(coalesce(v_addr, '')));
      v_client := null;
      if v_seen ? v_key then
        -- Same customer again: a second service address becomes another property.
        if v_addr is null or (v_key || '|' || lower(v_addr)) = any (v_addr_seen) then
          v_skipped := v_skipped || jsonb_build_object('row', i, 'reason', 'Same customer and address appear earlier in the file'); continue;
        end if;
        v_client := (v_seen->>v_key)::uuid;
      end if;
      v_dup := null;
      if v_client is null then
      select c.name into v_dup from public.clients c
      where c.tenant_id = p_tenant_id and (
        (v_email is not null and lower(c.email) = v_email)
        or (v_phone is not null and length(v_phone) >= 7 and regexp_replace(coalesce(c.phone, ''), '[^0-9]', '', 'g') = v_phone)
        or (lower(c.name) = lower(v_name) and v_addr is not null and exists (
              select 1 from public.properties p where p.client_id = c.id and lower(p.address_line1) = lower(v_addr))))
      limit 1;
      end if;
      if v_dup is not null then
        v_skipped := v_skipped || jsonb_build_object('row', i, 'reason', 'Already a customer: ' || v_dup); continue;
      end if;

      -- Insert
      if v_client is null then
      insert into public.clients (tenant_id, name, email, phone, company_name, kind, status, lead_source, tags, internal_notes)
      values (p_tenant_id, v_name, v_email, nullif(btrim(r->>'phone'), ''), nullif(btrim(r->>'company_name'), ''),
              case when lower(coalesce(r->>'kind', '')) like 'comm%' then 'commercial' else 'residential' end,
              case when lower(coalesce(r->>'status', '')) in ('lead','prospect') then 'lead'
                   when lower(coalesce(r->>'status', '')) in ('inactive','former','past') then 'inactive' else 'active' end,
              nullif(btrim(r->>'lead_source'), ''),
              coalesce((select array_agg(btrim(t)) from unnest(string_to_array(coalesce(r->>'tags', ''), ',')) t where btrim(t) <> ''), '{}'),
              nullif(btrim(r->>'notes'), ''))
      returning id into v_client;
      n_clients := n_clients + 1;
      v_seen := v_seen || jsonb_build_object(v_key, v_client);
      end if;
      if v_addr is not null then v_addr_seen := v_addr_seen || (v_key || '|' || lower(v_addr)); end if;

      v_property := null;
      if v_addr is not null then
        insert into public.properties (tenant_id, client_id, address_line1, city, region, postal_code, access_notes, lawn_sqft)
        values (p_tenant_id, v_client, v_addr, nullif(btrim(r->>'city'), ''), upper(nullif(btrim(r->>'region'), '')),
                nullif(btrim(r->>'postal_code'), ''), nullif(btrim(r->>'access_notes'), ''), v_sqft)
        returning id into v_property;
        n_props := n_props + 1;
      end if;

      if v_property is not null and (nullif(btrim(r->>'service'), '') is not null or v_freq is not null) then
        if coalesce(v_freq, 1) = 0 then
          insert into public.jobs (tenant_id, client_id, property_id, title, kind, status, price, crew_id)
          values (p_tenant_id, v_client, v_property, coalesce(nullif(btrim(r->>'service'), ''), 'Service'), 'one_off', 'active', v_price, v_crew)
          returning id into v_job;
          insert into public.visits (tenant_id, job_id, scheduled_date) values (p_tenant_id, v_job, coalesce(v_start, v_today));
        else
          v_start := coalesce(v_start, v_today);
          insert into public.jobs (tenant_id, client_id, property_id, title, kind, status, price, crew_id, starts_on, weekday, interval_weeks)
          values (p_tenant_id, v_client, v_property, coalesce(nullif(btrim(r->>'service'), ''), 'Lawn service'), 'recurring', 'active',
                  v_price, v_crew, v_start, coalesce(v_day, extract(isodow from v_start)::int), coalesce(v_freq, 1));
        end if;
        n_jobs := n_jobs + 1;
      end if;
    end loop;

    v_summary := jsonb_build_object('dry_run', p_dry_run, 'rows', i, 'clients', n_clients, 'properties', n_props, 'jobs', n_jobs,
                                    'skipped', v_skipped, 'errors', v_errors);
    if p_dry_run then
      raise exception 'import_dry_run' using errcode = 'P0001', detail = v_summary::text;
    end if;
    perform public.generate_visits(p_tenant_id, v_today, v_today + 21);
    if n_clients > 0 then
      perform private.log_activity(p_tenant_id, null, null, 'import',
        format('Imported %s customers, %s properties, %s jobs from a CSV file', n_clients, n_props, n_jobs), v_summary);
    end if;
    perform set_config('app.audit_reason', '', true);
  exception when sqlstate 'P0001' then
    if sqlerrm = 'import_dry_run' then
      get stacked diagnostics v_key = pg_exception_detail;
      return v_key::jsonb;   -- every insert above has been rolled back
    end if;
    raise;
  end;
  return v_summary;
end $$;
