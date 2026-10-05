-- Self-serve: a stranger signs up, creates a company, invites a crew member,
-- who accepts and can clock in. No developer steps involved.

-- New user signs up (auth trigger creates their profile)
insert into auth.users (id, email, raw_user_meta_data) values
  ('c0ffee00-0000-0000-0000-000000000001', 'newowner@green.test', '{"full_name":"Gina Green"}'),
  ('c0ffee00-0000-0000-0000-000000000002', 'newhire@green.test',  '{"full_name":"Hank Hire"}'),
  ('c0ffee00-0000-0000-0000-000000000003', 'sneaky@else.test',    '{}');
select tests.is((select full_name from public.profiles where id = 'c0ffee00-0000-0000-0000-000000000001'), 'Gina Green', 'Sign-up creates a profile');

select tests.login('c0ffee00-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select public.create_tenant(' ')$$, '%invalid_company_name%', 'Company name required');
select tests.throws($$select public.create_tenant('Green Co', 'Not/AZone')$$, '%invalid_timezone%', 'Invalid timezone rejected at sign-up');
select public.create_tenant('Green Co Lawn', 'America/Denver') as green \gset
select tests.ok(:'green' is not null, 'Owner creates their company');
select tests.is((select role from public.my_companies() where tenant_id = :'green'), 'owner', 'Creator is owner');
select tests.ok((select employee_id is not null from public.my_companies() where tenant_id = :'green'), 'Owner gets an employee record (can clock in)');
select tests.is((select plan from public.tenants where id = :'green'), 'trial', 'New company starts on trial');
select tests.is(tests.count('select 1 from public.clients'), 0::bigint, 'New company sees no one else''s clients');

select tests.throws(format($$select public.invite_member(%L, 'not-an-email', 'crew')$$, :'green'), '%invalid_email%', 'Invite needs a valid email');
select public.invite_member(:'green', 'NewHire@Green.test', 'crew', null, 'Hank') as tok \gset
select tests.is(length(:'tok'), 48, 'Invite returns a token');
select tests.is(tests.count(format('select 1 from public.invitations where tenant_id = %L and token_hash = %L', :'green', :'tok')), 0::bigint,
  'Raw token is never stored');
select tests.is(tests.count(format($$select 1 from public.employees where tenant_id = %L and display_name = 'Hank' and user_id is null$$, :'green')), 1::bigint,
  'Invited crew member can be scheduled before accepting');
select public.invite_member(:'green', 'newhire@green.test', 'crew') as tok2 \gset
select tests.is(tests.count(format($$select 1 from public.invitations where tenant_id = %L and revoked_at is null and accepted_at is null$$, :'green')), 1::bigint,
  'Re-inviting replaces the old pending invite');
select tests.is(tests.count(format($$select 1 from public.employees where tenant_id = %L and lower(email) = 'newhire@green.test'$$, :'green')), 1::bigint,
  'Re-inviting does not create a duplicate employee');
reset role;

-- Wrong person tries the link
select tests.login('c0ffee00-0000-0000-0000-000000000003');
set role authenticated;
select tests.throws(format($$select public.accept_invitation(%L)$$, :'tok2'), '%invitation_email_mismatch%', 'Invite only works for the invited email');
select tests.throws($$select public.accept_invitation('deadbeef')$$, '%invitation_invalid%', 'Bogus token rejected');
reset role;

-- Old (replaced) token no longer works; current one does
select tests.login('c0ffee00-0000-0000-0000-000000000002');
set role authenticated;
select tests.throws(format($$select public.accept_invitation(%L)$$, :'tok'), '%invitation_invalid%', 'Replaced invite token is dead');
select tests.is(public.accept_invitation(:'tok2'), :'green'::uuid, 'Invited user accepts');
select tests.throws(format($$select public.accept_invitation(%L)$$, :'tok2'), '%invitation_already_used%', 'Invite cannot be reused');
select tests.is((select role from public.my_companies() where tenant_id = :'green'), 'crew', 'New hire is crew');
select tests.is(tests.count(format('select 1 from public.employees where tenant_id = %L and user_id = auth.uid()', :'green')), 1::bigint,
  'New hire linked to the employee record created at invite (no duplicate)');
select tests.lives(format($$select public.clock_in(%L)$$, :'green'), 'New hire can clock in right away');
reset role;

-- Expired invites
select tests.login('c0ffee00-0000-0000-0000-000000000001');
set role authenticated;
select public.invite_member(:'green', 'late@green.test', 'crew') as tok3 \gset
reset role;
update public.invitations set expires_at = now() - interval '1 minute' where email = 'late@green.test';
insert into auth.users (id, email) values ('c0ffee00-0000-0000-0000-000000000004', 'late@green.test');
select tests.login('c0ffee00-0000-0000-0000-000000000004');
set role authenticated;
select tests.throws(format($$select public.accept_invitation(%L)$$, :'tok3'), '%invitation_expired%', 'Expired invite rejected');
reset role;

-- Roles and removal
select tests.login('c0ffee00-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws(format($$select public.set_member_role(%L, auth.uid(), 'admin')$$, :'green'), '%cannot_remove_last_owner%', 'Last owner cannot demote themselves');
select tests.throws(format($$select public.remove_member(%L, auth.uid())$$, :'green'), '%cannot_remove_last_owner%', 'Last owner cannot be removed');
select tests.lives(format($$select public.set_member_role(%L, 'c0ffee00-0000-0000-0000-000000000002', 'admin')$$, :'green'), 'Owner promotes crew to admin');
select tests.lives(format($$select public.remove_member(%L, 'c0ffee00-0000-0000-0000-000000000002')$$, :'green'), 'Owner removes a member');
select tests.is(tests.count(format($$select 1 from public.employees where tenant_id = %L and display_name = 'Hank' and status = 'inactive'$$, :'green')), 1::bigint,
  'Removed member''s employee record is kept (inactive)');
select tests.is(tests.count(format($$select 1 from public.time_entries where tenant_id = %L$$, :'green')), 1::bigint,
  'Removed member''s time history is kept');
select tests.ok(exists (select 1 from public.audit_log where tenant_id = :'green' and entity_type = 'memberships' and action = 'delete' and reason = 'remove_member'),
  'Member removal is audited');
reset role;

select tests.login('c0ffee00-0000-0000-0000-000000000002');
set role authenticated;
select tests.is(tests.count('select 1 from public.tenants'), 0::bigint, 'Removed member loses access immediately');
select tests.throws(format($$select public.clock_out(%L)$$, :'green'), '%not_an_active_employee%', 'Removed member cannot punch');
reset role;
