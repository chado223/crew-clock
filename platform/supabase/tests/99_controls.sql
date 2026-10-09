-- Second audit: pay and hours controls, break corrections, expenses voided not deleted, invoice void flag.

select now() - interval '1 minute' as t0 \gset

-- ------------------------------------------------------------------ pay
select tests.login('a0000000-0000-0000-0000-000000000002');   -- admin
set role authenticated;
update public.employees set status = 'inactive' where id = 'ea000000-0000-0000-0000-000000000002';
select tests.throws($$insert into public.employee_pay_rates (tenant_id, employee_id, hourly_rate, effective_from)
  values ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000002', 99, current_date)$$,
  '%row-level security%', 'Admin cannot set own pay, even after deactivating own record');
select tests.throws($$insert into public.employee_pay_rates (tenant_id, employee_id, hourly_rate, effective_from)
  values ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000001', 99, current_date)$$,
  '%row-level security%', 'Admin cannot set the owner''s pay');
select tests.lives($$insert into public.employee_pay_rates (tenant_id, employee_id, hourly_rate, effective_from)
  values ('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000003', 20, current_date)$$,
  'Admin can set a crew member''s pay');

-- ---------------------------------------------------------------- hours
select tests.throws(format($$select public.add_time_entry('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000002',
  %L::timestamptz - interval '30 hours', %L::timestamptz - interval '20 hours', 'forgot')$$, :'t0', :'t0'), '%owner_must_approve%',
  'Admin cannot add hours for themselves');
select tests.throws(format($$select public.add_time_entry('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000001',
  %L::timestamptz - interval '30 hours', %L::timestamptz - interval '20 hours', 'forgot')$$, :'t0', :'t0'), '%owner_must_approve%',
  'Admin cannot add hours for the owner');
select tests.lives(format($$select public.add_time_entry('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000004',
  %L::timestamptz - interval '30 hours', %L::timestamptz - interval '22 hours', 'forgot to clock in')$$, :'t0', :'t0'),
  'Admin can add hours for crew');
reset role;
update public.employees set status = 'active' where id = 'ea000000-0000-0000-0000-000000000002';

-- Owner can fix their admin's hours.
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.lives(format($$select public.add_time_entry('aaaaaaaa-0000-0000-0000-000000000000', 'ea000000-0000-0000-0000-000000000002',
  %L::timestamptz - interval '54 hours', %L::timestamptz - interval '46 hours', 'approved')$$, :'t0', :'t0'), 'Owner can add an admin''s hours');
reset role;

-- --------------------------------------------------------------- breaks
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select (public.clock_in('aaaaaaaa-0000-0000-0000-000000000000', '00000000-0000-0000-0000-0000000000c1', :'t0'::timestamptz - interval '4 hours')).id as sh \gset
select (public.start_break('aaaaaaaa-0000-0000-0000-000000000000', null, :'t0'::timestamptz - interval '3 hours')).id as br \gset
select public.clock_out('aaaaaaaa-0000-0000-0000-000000000000', '00000000-0000-0000-0000-0000000000c2', :'t0'::timestamptz);
select tests.is((select worked_seconds from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', current_date - 2, current_date + 1) where entry_id = :'sh'),
  3600::bigint, 'Forgotten break: only 1 h counted before the fix');
select tests.throws(format($$select public.correct_break(%L, %L::timestamptz - interval '3 hours', %L::timestamptz - interval '150 minutes', false, 'lunch')$$, :'br', :'t0', :'t0'),
  '%forbidden%', 'Crew cannot correct breaks');
reset role;
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select tests.throws(format($$select public.correct_break(%L, %L::timestamptz - interval '3 hours', null, false, 'no end time')$$, :'br', :'t0'),
  '%break_outside_shift%', 'An open-ended break on a finished shift is refused');
select tests.lives(format($$select public.correct_break(%L, %L::timestamptz - interval '3 hours', %L::timestamptz - interval '150 minutes', false, 'took 30 min lunch')$$, :'br', :'t0', :'t0'),
  'Manager ends the forgotten break at the right time');
select tests.is((select worked_seconds from public.timesheet('aaaaaaaa-0000-0000-0000-000000000000', current_date - 2, current_date + 1) where entry_id = :'sh'),
  12600::bigint, '4 h shift minus 30 min break = 3.5 h after the fix');
reset role;
select tests.ok((select count(*) from public.audit_log where entity_type = 'time_entry_breaks' and reason like 'correct_break:%') = 1, 'Break fix is audited');

-- ------------------------------------------------------------- expenses
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select (public.business_health('aaaaaaaa-0000-0000-0000-000000000000', current_date, current_date)->'money'->>'expenses')::numeric as base \gset
insert into public.expenses (tenant_id, category, amount, spent_at) values ('aaaaaaaa-0000-0000-0000-000000000000', 'Fuel', 40, current_date);
select id as ex from public.expenses where category = 'Fuel' and amount = 40 \gset
select tests.throws(format($$delete from public.expenses where id = %L$$, :'ex'), '%permission denied%', 'Expenses cannot be deleted');
select tests.throws(format($$update public.expenses set created_by = 'a0000000-0000-0000-0000-000000000001' where id = %L$$, :'ex'),
  '%permission denied%', 'Who recorded it cannot be changed');
select tests.lives(format($$update public.expenses set amount = 45 where id = %L$$, :'ex'), 'Amount can be corrected');
select tests.is((public.business_health('aaaaaaaa-0000-0000-0000-000000000000', current_date, current_date)->'money'->>'expenses')::numeric, :base + 45,
  'Expense counted');
select public.void_expense(:'ex', 'duplicate receipt');
select tests.is((public.business_health('aaaaaaaa-0000-0000-0000-000000000000', current_date, current_date)->'money'->>'expenses')::numeric, :base::numeric,
  'Voided expense no longer counted');
select tests.is((select count(*) from public.expenses where id = :'ex'), 1::bigint, 'Voided expense is kept');
select tests.throws(format($$select public.void_expense(%L, '')$$, :'ex'), '%reason_required%', 'Void needs a reason');
reset role;
select tests.login('a0000000-0000-0000-0000-000000000003');
set role authenticated;
select tests.throws(format($$select public.void_expense(%L, 'not mine to void')$$, :'ex'), '%forbidden%', 'Crew cannot void expenses');
reset role;

-- ------------------------------------------------- invoice void flag spoof
insert into public.visits (id, tenant_id, job_id, scheduled_date, status, completed_at)
values ('77777777-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001', current_date - 1, 'completed', now());
insert into public.invoices (id, tenant_id, client_id, total, status) values
  ('77777777-0000-0000-0000-0000000000a1', 'aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 0, 'draft');
insert into public.invoice_lines (tenant_id, invoice_id, visit_id, description, quantity, unit_price)
values ('aaaaaaaa-0000-0000-0000-000000000000', '77777777-0000-0000-0000-0000000000a1', '77777777-0000-0000-0000-000000000001', 'Mow', 1, 50);
update public.invoices set status = 'sent', sent_at = now() where id = '77777777-0000-0000-0000-0000000000a1';
select tests.login('a0000000-0000-0000-0000-000000000002');
set role authenticated;
select set_config('app.voiding_invoice', '77777777-0000-0000-0000-0000000000a1', true);
select tests.throws($$update public.invoice_lines set voided_visit_id = visit_id, visit_id = null where invoice_id = '77777777-0000-0000-0000-0000000000a1'$$,
  '%document_not_draft%', 'A session flag cannot release visits from a sent invoice');
select public.void_invoice('77777777-0000-0000-0000-0000000000a1', 'wrong customer');
reset role;
select tests.is((select visit_id from public.invoice_lines where invoice_id = '77777777-0000-0000-0000-0000000000a1')::text, null,
  'Real void still releases the visit for re-billing');
