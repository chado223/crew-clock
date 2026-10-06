-- Weather automation: company rules, worker API, alerts, office decisions, isolation.
-- Dates are relative to each company's local "today" so the file works any day.

create temp table wd as select private.tenant_today('aaaaaaaa-0000-0000-0000-000000000000') as d;
grant select on wd to authenticated, service_role;

-- A visit tomorrow for A (weather-sensitive mow), and one for B.
insert into public.visits (id, tenant_id, job_id, scheduled_date) values
  ('7a500000-0000-0000-0000-0000000000f1', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', (select d + 1 from wd)),
  ('7b500000-0000-0000-0000-0000000000f1', 'bbbbbbbb-0000-0000-0000-000000000000', 'f1b00000-0000-0000-0000-000000000001', (select d + 1 from wd));
-- A second A property/job on a service that ignores weather.
insert into public.properties (id, tenant_id, client_id, address_line1, city, region, postal_code, latitude, longitude) values
  ('d1a00000-0000-0000-0000-0000000000f2', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001',
   '2 Gutter Ln', 'Seymour', 'TN', '37865', 35.89, -83.77);
insert into public.services (id, tenant_id, name, weather_sensitive) values
  ('5a000000-0000-0000-0000-0000000000f2', 'aaaaaaaa-0000-0000-0000-000000000000', 'Gutter cleaning', false);
insert into public.jobs (id, tenant_id, client_id, property_id, title, service_id) values
  ('f1a00000-0000-0000-0000-0000000000f2', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001',
   'd1a00000-0000-0000-0000-0000000000f2', 'Gutters', '5a000000-0000-0000-0000-0000000000f2');
insert into public.visits (id, tenant_id, job_id, scheduled_date) values
  ('7a500000-0000-0000-0000-0000000000f2', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-0000000000f2', (select d + 1 from wd));
update public.properties set city = 'Seymour', region = 'TN', postal_code = '37865' where id = 'd1a00000-0000-0000-0000-000000000001';

-- ===================================================== settings
select tests.login('a0000000-0000-0000-0000-000000000002');   -- A admin
set role authenticated;
select tests.lives($$insert into public.weather_settings (tenant_id, enabled, rain_chance_pct) values ('aaaaaaaa-0000-0000-0000-000000000000', true, 60)$$,
  'Admin turns weather on for their company');
select tests.is((select updated_by::text from public.weather_settings), 'a0000000-0000-0000-0000-000000000002', 'Who changed settings is recorded');
select tests.throws($$update public.weather_settings set updated_by = 'a0000000-0000-0000-0000-000000000003'$$, '%permission denied%',
  'updated_by cannot be forged');
select tests.throws($$insert into public.weather_settings (tenant_id, enabled) values ('bbbbbbbb-0000-0000-0000-000000000000', true)$$,
  '%row-level security%', 'Cannot change another company''s weather settings');
select tests.throws($$update public.weather_settings set min_temp_f = 90, max_temp_f = 80$$, '%check%', 'Nonsense temperature range rejected');
select tests.lives($$insert into public.weather_settings (tenant_id, enabled, rain_chance_pct) values ('aaaaaaaa-0000-0000-0000-000000000000', true, 60)
  on conflict (tenant_id) do update set tenant_id = excluded.tenant_id, enabled = excluded.enabled, rain_chance_pct = excluded.rain_chance_pct$$,
  'Settings save as an upsert (how the web app writes them)');
select tests.throws($$update public.weather_settings set tenant_id = 'bbbbbbbb-0000-0000-0000-000000000000'$$, '%', 'Settings cannot be moved to another company');
reset role;
select tests.ok((select count(*) from public.audit_log where entity_type = 'weather_settings') >= 1, 'Settings changes are audited');

-- ===================================================== worker API is service-only
select tests.login('a0000000-0000-0000-0000-000000000001');   -- A owner
set role authenticated;
select tests.throws($$select * from public.weather_worker_targets()$$, '%permission denied%', 'Owner cannot call worker targets');
select tests.throws($$select public.weather_worker_save_forecast('d1a00000-0000-0000-0000-000000000001', '[]')$$, '%permission denied%',
  'Owner cannot write forecasts directly');
select tests.throws($$select public.weather_worker_save_coords('d1a00000-0000-0000-0000-000000000001', 1, 1)$$, '%permission denied%',
  'Owner cannot call geocode writer');
select tests.throws($$insert into public.weather_forecasts (tenant_id, property_id, forecast_date) values ('aaaaaaaa-0000-0000-0000-000000000000', 'd1a00000-0000-0000-0000-000000000001', current_date)$$,
  '%permission denied%', 'No direct forecast inserts from the app');
select tests.throws($$insert into public.weather_alerts (tenant_id, visit_id, forecast_date, reasons) values ('aaaaaaaa-0000-0000-0000-000000000000', '7a500000-0000-0000-0000-0000000000f1', current_date, '{rain}')$$,
  '%permission denied%', 'No direct alert inserts from the app');
reset role;
select tests.login(null);
set role anon;
select tests.throws($$select * from public.weather_worker_targets()$$, '%permission denied%', 'Anonymous cannot call worker targets');
reset role;

set role service_role;
select tests.is((select count(*) from public.weather_worker_targets()), 1::bigint,
  'Worker targets: only the enabled company''s weather-sensitive property (not B, not the gutter job)');
select tests.is((select property_id::text from public.weather_worker_targets()), 'd1a00000-0000-0000-0000-000000000001', 'Right property');
select tests.ok((select address from public.weather_worker_targets()) like '1 A St, Seymour, TN 37865', 'Target carries a geocodable address');
select tests.is((select string_agg(t::text, ',') from public.weather_worker_tenants() t), 'aaaaaaaa-0000-0000-0000-000000000000', 'Only enabled companies are evaluated');
select public.weather_worker_save_coords('d1a00000-0000-0000-0000-000000000001', 35.88, -83.76);
select public.weather_worker_save_coords('d1a00000-0000-0000-0000-000000000001', 1, 1);   -- second call must not overwrite
select tests.is((select latitude from public.properties where id = 'd1a00000-0000-0000-0000-000000000001'), 35.88::float8, 'Geocoder fills empty coordinates once');
select tests.is((select geocode_source from public.properties where id = 'd1a00000-0000-0000-0000-000000000001'), 'census', 'Coordinate source recorded');
select public.weather_worker_save_point('d1a00000-0000-0000-0000-000000000001', 'MRX', 47, 61);
select tests.is((select grid_office || grid_x || ',' || grid_y from public.weather_worker_targets()), 'MRX47,61', 'Grid point cached for next run');
select tests.is(public.weather_worker_save_forecast('d1a00000-0000-0000-0000-000000000001',
  jsonb_build_array(jsonb_build_object('date', (select d + 1 from wd), 'precip_pct', 80, 'wind_mph', 8, 'temp_high_f', 70, 'summary', 'Showers Likely'),
                    jsonb_build_object('date', (select d + 2 from wd), 'precip_pct', 10, 'wind_mph', 5, 'temp_high_f', 72, 'summary', 'Sunny'))), 2,
  'Worker saves daily forecast');
select tests.throws($$select public.weather_worker_save_forecast('d1a00000-0000-0000-0000-000000000001', '{"x":1}')$$, '%invalid_forecast%', 'Malformed forecast rejected');
-- Also store rain for the gutter property; it must not alert (service ignores weather).
select public.weather_worker_save_forecast('d1a00000-0000-0000-0000-0000000000f2',
  jsonb_build_array(jsonb_build_object('date', (select d + 1 from wd), 'precip_pct', 95, 'wind_mph', 40, 'temp_high_f', 70)));

-- ===================================================== evaluation
select tests.is(public.weather_evaluate('aaaaaaaa-0000-0000-0000-000000000000'), 1, 'Rain over the company threshold opens one alert');
select tests.is(public.weather_evaluate('aaaaaaaa-0000-0000-0000-000000000000'), 0, 'Running again opens nothing new (idempotent)');
select tests.is((select reasons from public.weather_alerts where visit_id = '7a500000-0000-0000-0000-0000000000f1'), '{rain}'::text[], 'Alert says why');
select tests.is((select count(*) from public.weather_alerts where visit_id = '7a500000-0000-0000-0000-0000000000f2'), 0::bigint,
  'Weather-insensitive service never alerts');
select tests.is(public.weather_evaluate('bbbbbbbb-0000-0000-0000-000000000000'), 0, 'Company without weather on gets no alerts');

-- Forecast improves -> alert clears; worsens again -> same alert reopens.
select public.weather_worker_save_forecast('d1a00000-0000-0000-0000-000000000001',
  jsonb_build_array(jsonb_build_object('date', (select d + 1 from wd), 'precip_pct', 20, 'wind_mph', 8, 'temp_high_f', 70)));
select public.weather_evaluate('aaaaaaaa-0000-0000-0000-000000000000');
select tests.is((select status from public.weather_alerts where visit_id = '7a500000-0000-0000-0000-0000000000f1'), 'cleared', 'Better forecast clears the alert');
select public.weather_worker_save_forecast('d1a00000-0000-0000-0000-000000000001',
  jsonb_build_array(jsonb_build_object('date', (select d + 1 from wd), 'precip_pct', 30, 'wind_mph', 31, 'temp_high_f', 70)));
select tests.is(public.weather_evaluate('aaaaaaaa-0000-0000-0000-000000000000'), 0, 'Worse again reuses the alert');
select tests.is((select status || ':' || array_to_string(reasons, ',') from public.weather_alerts where visit_id = '7a500000-0000-0000-0000-0000000000f1'),
  'open:wind', 'Reopened with the new reason (wind)');
reset role;

-- Company thresholds matter: raise the wind limit and it clears.
update public.weather_settings set wind_mph = 40 where tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';
select public.weather_evaluate('aaaaaaaa-0000-0000-0000-000000000000');
select tests.is((select status from public.weather_alerts where visit_id = '7a500000-0000-0000-0000-0000000000f1'), 'cleared', 'Company''s own wind limit is respected');
update public.weather_settings set wind_mph = 25 where tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';
select public.weather_evaluate('aaaaaaaa-0000-0000-0000-000000000000');
-- Heat and cold
select tests.is(private.weather_reasons(s, row(null, null, current_date, 0, 0, 0, 101, 70, null, 'nws', now())::public.weather_forecasts), '{heat}'::text[], 'Heat rule')
  from public.weather_settings s where s.tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';
select tests.is(private.weather_reasons(s, row(null, null, current_date, 0, 0, 0, 30, 20, null, 'nws', now())::public.weather_forecasts), '{cold}'::text[], 'Cold rule')
  from public.weather_settings s where s.tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';

-- ===================================================== who can see it
create temp table a_alert as select id from public.weather_alerts where visit_id = '7a500000-0000-0000-0000-0000000000f1';
grant select on a_alert to authenticated;
select tests.login('a0000000-0000-0000-0000-000000000003');   -- A crew
set role authenticated;
select tests.is((select count(*) from public.weather_alerts) + (select count(*) from public.weather_forecasts)
  + (select count(*) from public.weather_settings) + (select count(*) from public.weather_points), 0::bigint, 'Crew sees no weather admin data');
select tests.throws($$select * from public.weather_outlook('aaaaaaaa-0000-0000-0000-000000000000')$$, '%forbidden%', 'Crew cannot open the weather outlook');
select tests.throws($$select public.weather_evaluate('aaaaaaaa-0000-0000-0000-000000000000')$$, '%forbidden%', 'Crew cannot run evaluation');
select tests.throws($$select public.handle_weather_alert((select id from a_alert), 'dismiss')$$, '%forbidden%', 'Crew cannot handle alerts');
reset role;

select tests.login('b0000000-0000-0000-0000-000000000001');   -- B owner
set role authenticated;
select tests.is((select count(*) from public.weather_alerts), 0::bigint, 'Other company sees none of A''s alerts');
select tests.throws($$select * from public.weather_outlook('aaaaaaaa-0000-0000-0000-000000000000')$$, '%forbidden%', 'Other company cannot open A''s outlook');
select tests.throws($$select public.weather_evaluate('aaaaaaaa-0000-0000-0000-000000000000')$$, '%forbidden%', 'Other company cannot evaluate A');
reset role;
set role authenticated;
select tests.throws($$select public.handle_weather_alert((select id from a_alert), 'dismiss')$$, '%forbidden%', 'Other company cannot dismiss A''s alert');
reset role;

-- Customer of A sees nothing weather-related
insert into auth.users (id, email) values ('c0570000-0000-0000-0000-0000000000f1', 'wcust@customer.test');
insert into public.portal_access (tenant_id, client_id, user_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'c0570000-0000-0000-0000-0000000000f1');
select tests.login('c0570000-0000-0000-0000-0000000000f1');
set role authenticated;
select tests.is((select count(*) from public.weather_alerts) + (select count(*) from public.weather_forecasts)
  + (select count(*) from public.weather_settings), 0::bigint, 'Customer sees no forecasts, alerts or settings');
select tests.throws($$select * from public.weather_outlook('aaaaaaaa-0000-0000-0000-000000000000')$$, '%forbidden%', 'Customer cannot open the outlook');
select tests.throws($$select public.handle_weather_alert((select id from a_alert), 'dismiss')$$, '%forbidden%', 'Customer cannot handle alerts');
reset role;

-- ===================================================== office decisions
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is((select count(*) from public.weather_outlook('aaaaaaaa-0000-0000-0000-000000000000') where alert_status = 'open'), 1::bigint,
  'Owner outlook shows the open alert');
select tests.is((select precip_pct from public.weather_outlook('aaaaaaaa-0000-0000-0000-000000000000') where visit_id = '7a500000-0000-0000-0000-0000000000f1'), 30,
  'Outlook shows the forecast');
select tests.throws($$select public.handle_weather_alert((select id from a_alert), 'move')$$, '%date_required%', 'Moving needs a new date');
select tests.throws($$select public.handle_weather_alert((select id from a_alert), 'explode')$$, '%invalid_action%', 'Unknown action rejected');
select tests.lives($$select public.handle_weather_alert((select id from a_alert), 'move', (select d + 2 from wd), 'Too windy')$$,
  'Owner moves the visit off the windy day');
select tests.is((select scheduled_date from public.visits where id = '7a500000-0000-0000-0000-0000000000f1'), (select d + 2 from wd), 'Visit moved');
select tests.is((select status from public.weather_alerts where id = (select id from a_alert)), 'moved', 'Alert closed as moved');
select tests.ok((select handled_by from public.weather_alerts where id = (select id from a_alert)) = 'a0000000-0000-0000-0000-000000000001', 'Who decided is recorded');
select tests.ok((select count(*) from public.activity where kind = 'visit_rescheduled' and summary like '%Weather: wind%Too windy%') = 1,
  'Customer history shows the weather reschedule');
select tests.throws($$select public.handle_weather_alert((select id from a_alert), 'dismiss')$$, '%alert_closed%', 'Closed alert cannot be handled again');
reset role;

-- Dismissed stays dismissed
set role service_role;
select public.weather_worker_save_forecast('d1a00000-0000-0000-0000-000000000001',
  jsonb_build_array(jsonb_build_object('date', (select d + 2 from wd), 'precip_pct', 90, 'wind_mph', 5, 'temp_high_f', 70)));
select tests.is(public.weather_evaluate('aaaaaaaa-0000-0000-0000-000000000000'), 1, 'Rain on the new day raises a fresh alert for that day');
reset role;
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.lives(format($$select public.handle_weather_alert(%L, 'dismiss', null, 'Light rain is fine')$$,
  (select id from public.weather_alerts where visit_id = '7a500000-0000-0000-0000-0000000000f1' and status = 'open')), 'Owner dismisses it');
reset role;
set role service_role;
select public.weather_evaluate('aaaaaaaa-0000-0000-0000-000000000000');
select tests.is((select count(*) from public.weather_alerts where visit_id = '7a500000-0000-0000-0000-0000000000f1' and status in ('open','acknowledged')), 0::bigint,
  'A dismissed alert is not reopened by the next run');
reset role;

-- Turning weather off clears open alerts
update public.weather_settings set rain_chance_pct = 10 where tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';
select public.weather_worker_save_forecast('d1a00000-0000-0000-0000-000000000001',
  jsonb_build_array(jsonb_build_object('date', (select d + 3 from wd), 'precip_pct', 50)));
insert into public.visits (tenant_id, job_id, scheduled_date) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', (select d + 3 from wd));
select public.weather_evaluate('aaaaaaaa-0000-0000-0000-000000000000');
select tests.ok((select count(*) from public.weather_alerts where status = 'open') >= 1, 'Open alert exists before turning off');
update public.weather_settings set enabled = false where tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';
select public.weather_evaluate('aaaaaaaa-0000-0000-0000-000000000000');
select tests.is((select count(*) from public.weather_alerts where status = 'open'), 0::bigint, 'Turning weather off clears open alerts');

-- ===================================================== coordinates
update public.properties set latitude = 36.0 where id = 'd1a00000-0000-0000-0000-000000000001';
select tests.is((select geocode_source from public.properties where id = 'd1a00000-0000-0000-0000-000000000001'), 'manual', 'Hand-entered coordinates marked manual');
select tests.is((select count(*) from public.weather_points where property_id = 'd1a00000-0000-0000-0000-000000000001'), 0::bigint,
  'Changing coordinates drops the cached grid point');
update public.properties set latitude = null, longitude = null, geocode_source = null where id = 'd1a00000-0000-0000-0000-0000000000f2';
select public.weather_worker_save_coords('d1a00000-0000-0000-0000-0000000000f2', 35.9, -83.8);
update public.properties set address_line1 = '3 New Rd' where id = 'd1a00000-0000-0000-0000-0000000000f2';
select tests.is((select latitude from public.properties where id = 'd1a00000-0000-0000-0000-0000000000f2'), null::float8,
  'Address change clears geocoded (not manual) coordinates so they are looked up again');
