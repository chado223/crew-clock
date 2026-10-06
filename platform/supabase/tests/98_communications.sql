-- Communications: test-mode safety, live gate, preferences, templates, workflows,
-- dispatcher API, delivery records, portal view and isolation.

update public.clients set email = 'customer-a@example.test', phone = '(865) 555-0101' where id = 'c1a00000-0000-0000-0000-000000000001';
update public.clients set email = 'customer-b@example.test' where id = 'c1b00000-0000-0000-0000-000000000001';
update public.invoices set due_at = now() - interval '10 days' where id = '1a000000-0000-0000-0000-000000000001';

create temp table ids (k text primary key, id uuid);
grant select, insert, update on ids to authenticated, service_role;

-- ===================================================== nothing configured = safe
select tests.login('a0000000-0000-0000-0000-000000000001');   -- A owner
set role authenticated;
insert into ids select 'est', public.send_estimate('e5a00000-0000-0000-0000-000000000001');
select tests.is((select status from public.estimates where id = 'e5a00000-0000-0000-0000-000000000001'), 'sent', 'Sending a draft estimate marks it sent');
select tests.is((select status || ':' || suppressed_reason from public.messages where id = (select id from ids where k = 'est')),
  'suppressed:test_mode_no_test_recipient', 'New company: test mode with no test address, so nothing can go out');
select tests.is((select to_address from public.messages where id = (select id from ids where k = 'est')), 'customer-a@example.test',
  'The intended recipient is recorded for review');
select tests.ok((select summary from public.activity where kind = 'message' order by seq desc limit 1) like 'Estimate EST-9001 emailed to customer (not delivered:%',
  'Customer history shows the attempt honestly');

-- ===================================================== test mode
select tests.lives($$insert into public.communication_settings (tenant_id, delivery_mode, test_email, test_phone, portal_url)
  values ('aaaaaaaa-0000-0000-0000-000000000000', 'test', 'office@acme.test', '+18655550199', 'https://app.example.test')$$, 'Owner sets test mode + test recipients');
insert into ids select 'inv', public.send_invoice('1a000000-0000-0000-0000-000000000001');
select tests.is((select mode || ':' || status || ':' || delivered_to || ':' || to_address from public.messages where id = (select id from ids where k = 'inv')),
  'test:queued:office@acme.test:customer-a@example.test', 'In test mode the message goes to the test address, never the customer');
select tests.ok((select body from public.messages where id = (select id from ids where k = 'inv')) like '%Invoice INV-9001 for $45.00%Acme Lawn%',
  'Template filled with invoice and company details');
select tests.ok((select body from public.messages where id = (select id from ids where k = 'inv')) like '%https://app.example.test/portal/invoices%',
  'Message links to the customer portal');
select tests.is((select count(*) from public.messages where body like '%{{%' or subject like '%{{%'), 0::bigint, 'No unfilled placeholders');

-- ===================================================== the live gate
select tests.throws($$update public.communication_settings set delivery_mode = 'live'$$, '%live_messaging_not_enabled%',
  'Owner cannot switch to live messaging until the platform owner enables it');
select tests.throws($$update public.communication_settings set updated_by = null$$, '%permission denied%', 'Audit fields cannot be written');
reset role;
select tests.is((select count(*) from public.audit_log where entity_type = 'communication_settings'), 1::bigint, 'Settings changes are audited');

update private.platform_flags set enabled = true where key = 'live_messaging';
update public.communication_settings set delivery_mode = 'live' where tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
insert into ids select 'live', public.send_invoice('1a000000-0000-0000-0000-000000000001');
select tests.is((select mode || ':' || delivered_to from public.messages where id = (select id from ids where k = 'live')), 'live:customer-a@example.test',
  'With the platform switch on and the company live, it goes to the customer');
reset role;
update private.platform_flags set enabled = false where key = 'live_messaging';
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
insert into ids select 'flagoff', public.send_invoice('1a000000-0000-0000-0000-000000000001');
select tests.is((select mode || ':' || delivered_to from public.messages where id = (select id from ids where k = 'flagoff')), 'test:office@acme.test',
  'Turning the platform switch off sends everything back to test mode immediately');
reset role;
update public.communication_settings set delivery_mode = 'test' where tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';

-- ===================================================== preferences
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.lives($$insert into public.contact_preferences (tenant_id, client_id, kinds_off) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', '{invoice_reminder}')$$, 'Office records a preference');
select tests.lives($$insert into public.contact_preferences (tenant_id, client_id, kinds_off) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', '{invoice_reminder}')
  on conflict (client_id) do update set tenant_id = excluded.tenant_id, client_id = excluded.client_id, kinds_off = excluded.kinds_off$$,
  'Preferences save as an upsert (how the web app writes them)');
select tests.throws($$update public.contact_preferences set client_id = 'c1a00000-0000-0000-0000-0000000000dd'$$, '%client_change_not_allowed%',
  'Preferences cannot be moved to another customer');
reset role;
update public.communication_settings set invoice_reminders = true, visit_reminders = true where tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';
select public.queue_invoice_reminders('aaaaaaaa-0000-0000-0000-000000000000');
select tests.is((select string_agg(status || ':' || suppressed_reason, ',') from public.messages where template_key = 'invoice_reminder'),
  'suppressed:customer_turned_off_invoice_reminder', 'Customer who turned reminders off gets none');
update public.contact_preferences set kinds_off = '{}' where client_id = 'c1a00000-0000-0000-0000-000000000001';
select public.queue_invoice_reminders('aaaaaaaa-0000-0000-0000-000000000000');
select public.queue_invoice_reminders('aaaaaaaa-0000-0000-0000-000000000000');
select tests.is((select count(*) from public.messages where template_key = 'invoice_reminder'), 1::bigint,
  'Reminders are idempotent within the reminder period (the earlier suppressed one counts)');

-- Visit reminders: tomorrow's visit, email unless the customer agreed to texts.
insert into public.visits (id, tenant_id, job_id, scheduled_date) values
  ('7a500000-0000-0000-0000-0000000000c1', 'aaaaaaaa-0000-0000-0000-000000000000', 'f1a00000-0000-0000-0000-000000000001',
   private.tenant_today('aaaaaaaa-0000-0000-0000-000000000000') + 1);
select tests.is(public.queue_visit_reminders('aaaaaaaa-0000-0000-0000-000000000000'), 1, 'One reminder for tomorrow''s visit');
select tests.is(public.queue_visit_reminders('aaaaaaaa-0000-0000-0000-000000000000'), 1, 'Running again...');
select tests.is((select count(*) from public.messages where template_key = 'visit_reminder'), 1::bigint, '...does not duplicate it');
select tests.is((select channel from public.messages where template_key = 'visit_reminder'), 'email', 'No text consent: email');
select tests.ok((select body from public.messages where template_key = 'visit_reminder') like '%Mow & edge at 1 A St%', 'Reminder names the service and address');

-- Texts need consent; the consent time is recorded automatically.
update public.contact_preferences set sms_ok = true, sms_consent_source = 'office (verbal)' where client_id = 'c1a00000-0000-0000-0000-000000000001';
select tests.ok((select sms_consent_at from public.contact_preferences where client_id = 'c1a00000-0000-0000-0000-000000000001') is not null, 'Text consent time recorded');
select tests.throws($$update public.contact_preferences set sms_consent_at = null where client_id = 'c1a00000-0000-0000-0000-000000000001'$$, '%check%',
  'Texts cannot be on without a consent record');
insert into ids select 'sms', private.enqueue_message('aaaaaaaa-0000-0000-0000-000000000000', 'invoice_sent', 'sms',
  'c1a00000-0000-0000-0000-000000000001', null, null, 'invoice', '1a000000-0000-0000-0000-000000000001', '{}', null);
select tests.is((select status from public.messages where id = (select id from ids where k = 'sms')), 'queued', 'Text allowed after consent');
select tests.is((select delivered_to from public.messages where channel = 'sms' order by created_at desc limit 1), '+18655550199', 'Test texts go to the test phone');
update public.contact_preferences set unsubscribed_at = now() where client_id = 'c1a00000-0000-0000-0000-000000000001';
insert into ids select 'unsub', private.enqueue_message('aaaaaaaa-0000-0000-0000-000000000000', 'invoice_sent', 'email',
  'c1a00000-0000-0000-0000-000000000001', null, null, null, null, '{}', null);
select tests.is((select suppressed_reason from public.messages where id = (select id from ids where k = 'unsub')), 'unsubscribed', 'Unsubscribed customers get nothing');
update public.contact_preferences set unsubscribed_at = null where client_id = 'c1a00000-0000-0000-0000-000000000001';

-- ===================================================== templates
select tests.login('a0000000-0000-0000-0000-000000000002');   -- A admin
set role authenticated;
select tests.lives($$insert into public.message_templates (tenant_id, template_key, channel, subject, body) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'invoice_sent', 'email', 'Your bill {{invoice_number}}', 'Howdy {{customer_name}}, you owe {{balance_due}}. {{nonsense}}')$$,
  'Admin customizes a template');
select tests.throws($$insert into public.message_templates (tenant_id, template_key, channel, subject, body) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'made_up', 'email', 'x', 'y')$$, '%invalid_template%', 'Unknown template rejected');
select tests.throws($$insert into public.message_templates (tenant_id, template_key, channel, body) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'estimate_sent', 'email', 'y')$$, '%subject_required%', 'Email templates need a subject');
insert into ids select 'tpl', public.send_invoice('1a000000-0000-0000-0000-000000000001');
select tests.is((select subject || ' / ' || body from public.messages where id = (select id from ids where k = 'tpl')),
  'Your bill INV-9001 / Howdy A Client, you owe $25.00. ', 'Company template used; unknown placeholders removed');
select tests.is((select count(*) from public.default_message_templates()), 12::bigint, 'Built-in templates available to the office');
reset role;

-- ===================================================== invites
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select public.send_invite_message('aaaaaaaa-0000-0000-0000-000000000000', 'team_invite', 'new@crew.test', 'http://evil.test/x')$$,
  '%invalid_link%', 'Invite links must be https or the app');
select tests.lives($$select public.send_invite_message('aaaaaaaa-0000-0000-0000-000000000000', 'team_invite', 'New@Crew.test', 'https://app.example.test/invite/abc')$$,
  'Team invite queued');
select tests.is((select delivered_to || ':' || to_address from public.messages where template_key = 'team_invite'), 'office@acme.test:new@crew.test',
  'Employee invites obey test mode too');
reset role;

-- ===================================================== who can see / do what
select tests.login('a0000000-0000-0000-0000-000000000003');   -- A crew
set role authenticated;
select tests.is((select count(*) from public.messages) + (select count(*) from public.message_events) + (select count(*) from public.communication_settings)
  + (select count(*) from public.contact_preferences) + (select count(*) from public.message_templates), 0::bigint, 'Crew sees no communications data');
select tests.throws($$select public.send_invoice('1a000000-0000-0000-0000-000000000001')$$, '%forbidden%', 'Crew cannot send invoices');
select tests.throws($$select public.messages_worker_claim()$$, '%permission denied%', 'Crew cannot run the dispatcher');
reset role;
select tests.login('b0000000-0000-0000-0000-000000000001');   -- B owner
set role authenticated;
select tests.is((select count(*) from public.messages) + (select count(*) from public.communication_settings), 0::bigint, 'Other company sees none of A''s messages');
select tests.throws($$select public.send_invoice('1a000000-0000-0000-0000-000000000001')$$, '%forbidden%', 'Other company cannot send A''s invoice');
select tests.throws($$select public.cancel_message((select id from ids where k = 'inv'))$$, '%forbidden%', 'Other company cannot cancel A''s messages');
select tests.throws($$select public.send_invite_message('aaaaaaaa-0000-0000-0000-000000000000', 'team_invite', 'x@y.test', 'https://a.test')$$, '%forbidden%',
  'Other company cannot send as A');
select tests.throws($$insert into public.communication_settings (tenant_id, delivery_mode) values ('aaaaaaaa-0000-0000-0000-000000000000', 'off')$$,
  '%row-level security%', 'Other company cannot change A''s settings');
reset role;
select tests.login(null);
set role anon;
select tests.throws($$select public.queue_visit_reminders('aaaaaaaa-0000-0000-0000-000000000000')$$, '%permission denied%', 'Anonymous cannot trigger reminders');
reset role;

-- ===================================================== dispatcher
set role service_role;
create temp table claimed as select * from public.messages_worker_claim(100);
select tests.ok((select count(*) from claimed) >= 3, 'Dispatcher claims queued messages');
select tests.is((select count(*) from claimed where status <> 'sending'), 0::bigint, 'Claimed messages are marked sending');
select tests.is((select count(*) from public.messages_worker_claim(100)), 0::bigint, 'Nothing is claimed twice');
select public.messages_worker_result((select id from ids where k = 'inv'), true, 'log', 'log-1');
select tests.is((select status || ':' || provider from public.messages where id = (select id from ids where k = 'inv')), 'sent:log', 'Sent result recorded');
select public.messages_worker_result((select id from ids where k = 'flagoff'), false, 'log', null, 'timeout', true);
select tests.is((select status from public.messages where id = (select id from ids where k = 'flagoff')), 'queued', 'Temporary failure goes back in the queue');
select tests.ok((select send_after from public.messages where id = (select id from ids where k = 'flagoff')) > now(), '...with a delay');
select public.messages_worker_result((select id from ids where k = 'tpl'), false, 'log', null, 'bad address', false);
select tests.is((select status from public.messages where id = (select id from ids where k = 'tpl')), 'failed', 'Permanent failure recorded');
select tests.throws(format('select public.messages_worker_result(%L, true, %L)', (select id from ids where k = 'inv'), 'log'), '%message_not_sending%',
  'A finished message cannot be reported twice');
-- Delivery receipts
update public.messages set status = 'sent', provider = 'acme_mail', provider_message_id = 'pm-1' where id = (select id from ids where k = 'live');
select public.messages_worker_event('acme_mail', 'pm-1', 'delivered');
select tests.is((select status from public.messages where id = (select id from ids where k = 'live')), 'delivered', 'Delivery receipt recorded');
select public.messages_worker_event('acme_mail', 'pm-1', 'complained');
select tests.is((select email_ok from public.contact_preferences where client_id = 'c1a00000-0000-0000-0000-000000000001'), false,
  'A spam complaint on a live email stops future email to that customer');
select tests.is((select string_agg(event, ',' order by id) from public.message_events where message_id = (select id from ids where k = 'inv')),
  'queued,sent', 'Delivery history kept per message');
reset role;

select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select tests.throws($$select public.cancel_message((select id from ids where k = 'inv'))$$, '%message_not_queued%', 'Sent messages cannot be canceled');
select tests.lives($$select public.cancel_message((select id from ids where k = 'flagoff'))$$, 'Queued message can be canceled');
reset role;

-- ===================================================== portal
insert into auth.users (id, email) values
  ('c0570000-0000-0000-0000-0000000000c1', 'mcust@customer.test'), ('c0570000-0000-0000-0000-0000000000c2', 'mcust2@customer.test');
insert into public.clients (id, tenant_id, name, email) values
  ('c1a00000-0000-0000-0000-0000000000c2', 'aaaaaaaa-0000-0000-0000-000000000000', 'Other Customer', 'other@example.test');
insert into public.portal_access (tenant_id, client_id, user_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'c0570000-0000-0000-0000-0000000000c1'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000c2', 'c0570000-0000-0000-0000-0000000000c2');
update private.platform_flags set enabled = true where key = 'live_messaging';
update public.communication_settings set delivery_mode = 'live' where tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';
select private.enqueue_message('aaaaaaaa-0000-0000-0000-000000000000', 'invoice_sent', 'email', 'c1a00000-0000-0000-0000-0000000000c2', null, null, null, null,
  '{"invoice_number":"OTHER-SECRET"}', null);
update public.messages set status = 'sent', sent_at = now() where status in ('queued','sending');
update private.platform_flags set enabled = false where key = 'live_messaging';

select tests.login('c0570000-0000-0000-0000-0000000000c1');
set role authenticated;
select tests.is((select count(*) from public.portal_messages('c1a00000-0000-0000-0000-000000000001')), 1::bigint,
  'Customer sees only real messages sent to them (not test-mode copies, not other customers)');
select tests.is((select count(*) from public.portal_messages('c1a00000-0000-0000-0000-000000000001') where body like '%OTHER-SECRET%'), 0::bigint,
  'Never another customer''s messages');
select tests.throws($$select * from public.portal_messages('c1a00000-0000-0000-0000-0000000000c2')$$, '%forbidden%', 'Cannot open another customer''s messages');
select tests.throws($$select * from public.portal_messages('c1b00000-0000-0000-0000-000000000001')$$, '%forbidden%', 'Cannot open another company''s messages');
select tests.is((select count(*) from public.messages) + (select count(*) from public.contact_preferences), 0::bigint, 'No direct table access');
select tests.lives($$select public.portal_set_preferences('c1a00000-0000-0000-0000-000000000001', true, false, false, true)$$, 'Customer updates preferences');
select tests.is((select sms_ok::text || ':' || visit_reminders::text || ':' || invoice_reminders::text from public.portal_preferences('c1a00000-0000-0000-0000-000000000001')),
  'false:false:true', 'Preferences saved');
select tests.throws($$select public.portal_set_preferences('c1a00000-0000-0000-0000-0000000000c2', false, false, false, false)$$, '%forbidden%',
  'Cannot change another customer''s preferences');
select tests.throws($$select public.send_invoice('1a000000-0000-0000-0000-000000000001')$$, '%forbidden%', 'Customer cannot send messages');
reset role;
select tests.is((select sms_consent_at from public.contact_preferences where client_id = 'c1a00000-0000-0000-0000-000000000001'), null::timestamptz,
  'Turning texts off clears consent');
select tests.ok((select count(*) from public.activity where kind = 'preferences_updated') = 1, 'Preference change is in customer history');
