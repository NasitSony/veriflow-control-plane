-- Day 2 schema: jobs + runs + events

create type job_state as enum (
  'PENDING', 'SCHEDULED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED'
);

create type run_state as enum (
  'CREATED', 'STARTING', 'RUNNING', 'SUCCEEDED', 'FAILED'
);

create table if not exists jobs (
  job_id uuid primary key,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  state job_state not null,

  -- the whole job request (image/args/resources/queue/priority/max_retries etc.)
  spec jsonb not null,

  queue text not null default 'default',
  priority int not null default 0,
  max_retries int not null default 0,

  -- DB-backed idempotency
  idempotency_key text not null unique
);

create index if not exists jobs_state_queue_priority_idx
  on jobs (state, queue, priority desc, created_at asc);

create table if not exists runs (
  run_id uuid primary key,
  job_id uuid not null references jobs(job_id) on delete cascade,
  attempt int not null,

  state run_state not null,

  created_at timestamptz not null default now(),
  started_at timestamptz,
  finished_at timestamptz,

  node text,
  k8s_job_name text,

  exit_code int,
  error_reason text,

  heartbeat_at timestamptz
);

create unique index if not exists runs_job_attempt_uniq
  on runs(job_id, attempt);

create table if not exists events (
  event_id bigserial primary key,
  job_id uuid not null references jobs(job_id) on delete cascade,
  run_id uuid references runs(run_id) on delete set null,
  ts timestamptz not null default now(),
  type text not null,
  payload jsonb not null default '{}'::jsonb
);

create index if not exists events_job_ts_idx on events(job_id, ts);

-- auto-update updated_at
create or replace function set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists trg_jobs_updated_at on jobs;
create trigger trg_jobs_updated_at
before update on jobs
for each row execute function set_updated_at();