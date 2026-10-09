-- In the DEDICATED production project only: lock down the other app's empty
-- leftovers that came along with the baseline (public.scenarios had every
-- privilege granted to anonymous visitors in the old shared project).
--
-- Runs only where the database is marked production AND the table is empty,
-- so it can never affect the old shared project, staging or tests. Nothing is
-- dropped; access is removed.
do $$
begin
  if coalesce((select enabled from private.platform_flags where key = 'production'), false)
     and to_regclass('public.scenarios') is not null
     and not exists (select 1 from public.scenarios) then
    revoke all on public.scenarios from anon, authenticated;
    raise notice 'Dedicated project: removed anon/authenticated access to the unused scenarios table.';
  end if;
end $$;
