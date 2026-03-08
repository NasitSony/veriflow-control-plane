alter table jobs
  add column if not exists next_run_at timestamptz not null default now();

create index if not exists jobs_next_run_at_idx
  on jobs (next_run_at);

-- (optional) helpful for filtering claim candidates efficiently
create index if not exists jobs_claim_idx
  on jobs (state, queue, next_run_at, priority desc, created_at asc);