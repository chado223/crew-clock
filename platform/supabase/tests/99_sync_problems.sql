-- Refused phone actions reach the office; only managers resolve them.

select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select public.report_sync_problem('aaaaaaaa-0000-0000-0000-000000000000', 'out', now() - interval '4 days', 'punch_too_old', '00000000-0000-0000-0000-00000000aaa1');
select public.report_sync_problem('aaaaaaaa-0000-0000-0000-000000000000', 'out', now() - interval '4 days', 'punch_too_old', '00000000-0000-0000-0000-00000000aaa1');
select tests.is((select count(*) from public.sync_problems), 1::bigint, 'Reported once even if the phone retries');
select tests.throws($$select public.resolve_sync_problem((select id from public.sync_problems limit 1), 'fixed it myself')$$, '%forbidden%', 'Crew cannot resolve');
select tests.throws($$select public.report_sync_problem('bbbbbbbb-0000-0000-0000-000000000000', 'in', now(), 'x')$$, '%', 'Cannot report into another company');
reset role;

select tests.login('a0000000-0000-0000-0000-000000000004');
set role authenticated;
select tests.is((select count(*) from public.sync_problems), 0::bigint, 'Coworkers don''t see each other''s problems');
reset role;

select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is((select count(*) from public.owner_attention('aaaaaaaa-0000-0000-0000-000000000000') where kind = 'sync_problem'), 1::bigint,
  'Owner sees it on Today');
select tests.ok((select title from public.owner_attention('aaaaaaaa-0000-0000-0000-000000000000') where kind = 'sync_problem') like '%Cy Crew%', 'Names who');
select public.resolve_sync_problem((select id from public.sync_problems limit 1), 'Added the clock-out by hand');
select tests.is((select count(*) from public.owner_attention('aaaaaaaa-0000-0000-0000-000000000000') where kind = 'sync_problem'), 0::bigint,
  'Resolved problems leave Today');
reset role;

select tests.login('b0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.is((select count(*) from public.sync_problems), 0::bigint, 'Other company sees nothing');
select tests.throws($$select public.owner_attention('aaaaaaaa-0000-0000-0000-000000000000')$$, '%forbidden%', 'Other company cannot read A''s Today');
reset role;
