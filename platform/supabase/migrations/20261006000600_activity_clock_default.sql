-- Every history entry, however it's written, defaults to the real clock time
-- (completes 20261006000500; add_client_note() relied on the column default).
alter table public.activity alter column occurred_at set default clock_timestamp();
