-- Visit photos: storage + table access for office, crew, customers, other companies.

-- Visit 7a5…01 (A, Crew 1 = Cy; Cam assigned directly). Mark it completed.
update public.visits set status = 'completed', started_at = now() - interval '2 hours', completed_at = now() - interval '1 hour'
  where id = '7a500000-0000-0000-0000-000000000001';
create temp table p (k text primary key, v text);
grant select, insert on p to authenticated;
insert into p values
  ('path1', 'aaaaaaaa-0000-0000-0000-000000000000/7a500000-0000-0000-0000-000000000001/after-1.jpg'),
  ('path2', 'aaaaaaaa-0000-0000-0000-000000000000/7a500000-0000-0000-0000-000000000001/issue-1.jpg'),
  ('bpath', 'bbbbbbbb-0000-0000-0000-000000000000/7b500000-0000-0000-0000-000000000001/b.jpg');

-- ===================================================== crew uploads
select tests.login('a0000000-0000-0000-0000-000000000003');   -- Cy (on Crew 1)
set role authenticated;
select tests.lives($$insert into storage.objects (bucket_id, name, owner) values ('visit-photos', (select v from p where k = 'path1'), auth.uid())$$,
  'Crew uploads a photo to their visit');
select tests.lives($$insert into storage.objects (bucket_id, name, owner) values ('visit-photos', (select v from p where k = 'path2'), auth.uid())$$,
  'Crew uploads a second photo');
select tests.throws($$insert into storage.objects (bucket_id, name, owner) values ('visit-photos', (select v from p where k = 'bpath'), auth.uid())$$,
  '%row-level security%', 'Crew cannot upload into another company''s visit');
select tests.throws($$insert into storage.objects (bucket_id, name, owner) values ('visit-photos', 'aaaaaaaa-0000-0000-0000-000000000000/../../x.jpg', auth.uid())$$,
  '%row-level security%', 'Malformed paths refused');
select tests.throws($$insert into storage.objects (bucket_id, name, owner) values ('visit-photos', 'aaaaaaaa-0000-0000-0000-000000000000/7a500000-0000-0000-0000-0000000000ff/x.jpg', auth.uid())$$,
  '%row-level security%', 'Unknown visit refused');
insert into p select 'id1', public.add_visit_photo('7a500000-0000-0000-0000-000000000001', (select v from p where k = 'path1'), 'after', 'Front yard done');
insert into p select 'id2', public.add_visit_photo('7a500000-0000-0000-0000-000000000001', (select v from p where k = 'path2'), 'issue', 'Broken sprinkler head');
select tests.throws($$select public.add_visit_photo('7a500000-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000000/7a500000-0000-0000-0000-000000000001/never-uploaded.jpg')$$,
  '%photo_not_uploaded%', 'Cannot record a photo that was not uploaded');
select tests.throws($$select public.add_visit_photo('7a500000-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000000/7a500000-0000-0000-0000-0000000000b1/x.jpg')$$,
  '%invalid_photo_path%', 'Path must belong to that visit');
select tests.is((select count(*) from public.visit_photos), 2::bigint, 'Crew sees photos on their visit');
select tests.throws($$select public.set_photo_visibility((select v::uuid from p where k = 'id1'), true)$$, '%forbidden%', 'Crew cannot share photos with customers');
select tests.is(tests.affected($$delete from storage.objects where bucket_id = 'visit-photos'$$), 0::bigint, 'Crew cannot delete photos');
select tests.throws($$update public.visit_photos set caption = 'x'$$, '%permission denied%', 'No direct edits');
reset role;

select tests.login('a0000000-0000-0000-0000-000000000004');   -- Cam, assigned directly too
set role authenticated;
select tests.is((select count(*) from storage.objects where bucket_id = 'visit-photos'), 2::bigint, 'Directly assigned worker can view the files');
reset role;

-- ===================================================== customer: nothing until the office shares
insert into auth.users (id, email) values ('c0570000-0000-0000-0000-0000000000f7', 'photo@customer.test'), ('c0570000-0000-0000-0000-0000000000f8', 'other@customer.test');
insert into public.clients (id, tenant_id, name) values ('c1a00000-0000-0000-0000-0000000000f8', 'aaaaaaaa-0000-0000-0000-000000000000', 'Neighbor');
insert into public.portal_access (tenant_id, client_id, user_id) values
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-000000000001', 'c0570000-0000-0000-0000-0000000000f7'),
  ('aaaaaaaa-0000-0000-0000-000000000000', 'c1a00000-0000-0000-0000-0000000000f8', 'c0570000-0000-0000-0000-0000000000f8');
select tests.login('c0570000-0000-0000-0000-0000000000f7');
set role authenticated;
select tests.is((select count(*) from public.portal_visit_photos('c1a00000-0000-0000-0000-000000000001')), 0::bigint, 'Customer sees no photos before the office shares');
select tests.is((select count(*) from storage.objects where bucket_id = 'visit-photos'), 0::bigint, 'Customer cannot open unshared files');
reset role;

select tests.login('a0000000-0000-0000-0000-000000000001');   -- owner shares the "after" photo only
set role authenticated;
select tests.lives($$select public.set_photo_visibility((select v::uuid from p where k = 'id1'), true)$$, 'Owner shares a photo with the customer');
reset role;
select tests.ok((select count(*) from public.audit_log where entity_type = 'visit_photos' and action = 'update') >= 1, 'Sharing is audited');

select tests.login('c0570000-0000-0000-0000-0000000000f7');
set role authenticated;
select tests.is((select string_agg(caption, ',') from public.portal_visit_photos('c1a00000-0000-0000-0000-000000000001')), 'Front yard done',
  'Customer sees only the shared photo (not the internal issue photo)');
select tests.is((select string_agg(name, ',') from storage.objects where bucket_id = 'visit-photos'), (select v from p where k = 'path1'),
  'Storage lets the customer open only the shared file');
select tests.is((select count(*) from public.visit_photos), 0::bigint, 'No direct table access for customers');
select tests.throws($$insert into storage.objects (bucket_id, name, owner) values ('visit-photos', 'aaaaaaaa-0000-0000-0000-000000000000/7a500000-0000-0000-0000-000000000001/cust.jpg', auth.uid())$$,
  '%row-level security%', 'Customers cannot upload');
reset role;
select tests.login('c0570000-0000-0000-0000-0000000000f8');   -- a different customer of the same company
set role authenticated;
select tests.is((select count(*) from storage.objects where bucket_id = 'visit-photos'), 0::bigint, 'Another customer cannot open those files');
select tests.throws($$select * from public.portal_visit_photos('c1a00000-0000-0000-0000-000000000001')$$, '%forbidden%', 'Another customer cannot list them');
reset role;
select tests.login('b0000000-0000-0000-0000-000000000001');   -- other company
set role authenticated;
select tests.is((select count(*) from storage.objects where bucket_id = 'visit-photos') + (select count(*) from public.visit_photos), 0::bigint,
  'Other company sees none');
select tests.throws($$select public.set_photo_visibility((select v::uuid from p where k = 'id1'), false)$$, '%forbidden%', 'Other company cannot change sharing');
reset role;

-- Hiding removes it from the customer immediately; nothing is deleted.
select tests.login('a0000000-0000-0000-0000-000000000001');
set role authenticated;
select public.hide_visit_photo((select v::uuid from p where k = 'id1'));
reset role;
select tests.login('c0570000-0000-0000-0000-0000000000f7');
set role authenticated;
select tests.is((select count(*) from public.portal_visit_photos('c1a00000-0000-0000-0000-000000000001')) + (select count(*) from storage.objects where bucket_id = 'visit-photos'),
  0::bigint, 'Hidden photo disappears for the customer');
reset role;
select tests.is((select count(*) from storage.objects where bucket_id = 'visit-photos'), 2::bigint, 'Files are kept');

-- A photo on a visit that is not completed is never shown to customers.
update public.visit_photos set hidden_at = null, customer_visible = true where storage_path = (select v from p where k = 'path1');
update public.visits set status = 'scheduled', completed_at = null, started_at = null where id = '7a500000-0000-0000-0000-000000000001';
select tests.login('c0570000-0000-0000-0000-0000000000f7');
set role authenticated;
select tests.is((select count(*) from public.portal_visit_photos('c1a00000-0000-0000-0000-000000000001')), 0::bigint, 'Only completed visits show photos');
reset role;
