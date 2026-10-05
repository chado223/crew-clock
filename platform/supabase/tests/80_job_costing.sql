-- Job costing with hand-checked numbers.
--
-- Mon 2026-09-21, Company A. Cy Crew ($18/h) works 8:00 paid hours (11:00-19:00 UTC).
--   Visit X: "A Mow" (crew 1 = Cy), on site 12:00-13:00, price $45
--   Visit Y: one-time "Mulch beds", Cy + Al directly assigned, on site 14:00-15:00, price $80
-- Al Admin has NO pay rate and works 13:30-15:30.
--
-- Cy: on site 60 + 60 = 120 min of 480 paid -> 360 min drive/prep, split 180/180.
--   X: (60 + 180) min x $18/h = $72.00 -> margin -$27.00 (-60.0%)
--   Y: Cy $72.00 + Al (no rate) $0 -> $72.00, margin $8.00 (10.0%), 1 missing rate
--      Al: on site 60, paid 120 -> overhead 60. Y on site 120, overhead 240.

insert into public.time_entries (tenant_id, employee_id, clock_in, clock_out, source) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000003', '2026-09-21 11:00+00', '2026-09-21 19:00+00', 'admin'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000002', '2026-09-21 13:30+00', '2026-09-21 15:30+00', 'admin');

insert into public.visits (id, tenant_id, job_id, scheduled_date, status, started_at, completed_at) values
  ('7a600000-0000-0000-0000-00000000000a', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001',
   '2026-09-21', 'completed', '2026-09-21 12:00+00', '2026-09-21 13:00+00');

insert into public.jobs (id, tenant_id, client_id, property_id, title, kind, price)
values ('f1a00000-0000-0000-0000-0000000000b0', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001',
        'd1a00000-0000-0000-0000-000000000001', 'Mulch beds', 'one_off', 80);
insert into public.visits (id, tenant_id, job_id, scheduled_date, status, started_at, completed_at) values
  ('7a600000-0000-0000-0000-00000000000b', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-0000000000b0',
   '2026-09-21', 'completed', '2026-09-21 14:00+00', '2026-09-21 15:00+00');
insert into public.visit_assignments (tenant_id, visit_id, employee_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', '7a600000-0000-0000-0000-00000000000b', 'ea000000-0000-0000-0000-000000000003'),
  ('aaaaaaaa-0000-0000-0000-000000000000', '7a600000-0000-0000-0000-00000000000b', 'ea000000-0000-0000-0000-000000000002');

-- A scheduled (not completed) visit must not appear in costing
insert into public.visits (tenant_id, job_id, scheduled_date) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', '2026-09-22');

select tests.login('a0000000-0000-0000-0000-000000000001');  -- owner
set role authenticated;
create temp table c as select * from public.visit_costing('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-21', '2026-09-22');

select tests.is((select count(*) from c), 2::bigint, 'Only completed visits are costed');
select tests.is((select onsite_minutes from c where visit_id = '7a600000-0000-0000-0000-00000000000a'), 60, 'X: 60 min on site');
select tests.is((select overhead_minutes from c where visit_id = '7a600000-0000-0000-0000-00000000000a'), 180, 'X: 180 min drive/prep allocated');
select tests.is((select labor_cost from c where visit_id = '7a600000-0000-0000-0000-00000000000a'), 72.00::numeric, 'X: labor $72.00');
select tests.is((select margin from c where visit_id = '7a600000-0000-0000-0000-00000000000a'), -27.00::numeric, 'X: margin -$27.00');
select tests.is((select margin_pct from c where visit_id = '7a600000-0000-0000-0000-00000000000a'), -60.0::numeric, 'X: margin -60.0%');
select tests.is((select workers from c where visit_id = '7a600000-0000-0000-0000-00000000000b'), 2, 'Y: two people worked it');
select tests.is((select onsite_minutes from c where visit_id = '7a600000-0000-0000-0000-00000000000b'), 120, 'Y: 120 person-minutes on site');
select tests.is((select overhead_minutes from c where visit_id = '7a600000-0000-0000-0000-00000000000b'), 240, 'Y: 240 min drive/prep allocated');
select tests.is((select labor_cost from c where visit_id = '7a600000-0000-0000-0000-00000000000b'), 72.00::numeric, 'Y: labor $72.00 (one worker has no rate)');
select tests.is((select margin_pct from c where visit_id = '7a600000-0000-0000-0000-00000000000b'), 10.0::numeric, 'Y: margin 10.0%');
select tests.is((select missing_rates from c where visit_id = '7a600000-0000-0000-0000-00000000000b'), 1, 'Y: flags the missing pay rate');
select tests.is((select sum(labor_cost) from c), 144.00::numeric, 'Cy''s full 8 paid hours ($144) are accounted for, none lost or double-counted');
reset role;

-- A raise takes effect from its date only
insert into public.employee_pay_rates (tenant_id, employee_id, hourly_rate, effective_from)
values ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000003', 24.00, '2026-09-22');
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is((select labor_cost from public.visit_costing('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-21', '2026-09-21')
                 where visit_id = '7a600000-0000-0000-0000-00000000000a'), 72.00::numeric, 'Later raise does not change past costs');
reset role;

-- Pay is confidential
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.throws($$select * from public.visit_costing('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-21', '2026-09-21')$$, '%forbidden%',
  'Crew cannot see job costing');
reset role;
select tests.login('b0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select * from public.visit_costing('aaaaaaaa-0000-0000-0000-000000000000', '2026-09-21', '2026-09-21')$$, '%forbidden%',
  'Other company cannot see A''s costing');
reset role;
