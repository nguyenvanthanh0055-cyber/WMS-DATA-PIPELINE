-- watermark table
create table if not exists etl_watermark (
  pipeline_name text not null,
  entity text not null,
  last_success_time timestamptz not null,
  last_success_run_id text,
  updated_at timestamptz not null default now(),
  primary key (pipeline_name, entity)
);

create index if not exists idx_etl_watermark_updated_at
on etl_watermark(updated_at);

-- create run logs
create table if not exists pipeline_run_log (
  run_id text primary key,
  pipeline_name text not null,
  entity text not null,
  started_at timestamptz not null default now(),
  ended_at timestamptz,
  status text not null default 'running',
  rows_in int not null default 0,
  rows_inserted_history int not null default 0,
  rows_upserted_latest int not null default 0,
  error text
);

create index if not exists idx_pipeline_run_log_entity_started
on pipeline_run_log(entity, started_at);

create table if not exists stg_ib_receipts_history (
  id uuid not null,
  updated_at timestamptz not null,
  payload jsonb not null,
  payload_hash text not null,
  _run_id text,
  _extracted_at timestamptz,
  _watermark_effective timestamptz,
  primary key (id, updated_at, payload_hash)
);

create table if not exists stg_ib_receipts(
  id uuid primary key,
  updated_at timestamptz not null,
  payload jsonb not null,
  payload_hash text not null,
  _run_id text,
  _extracted_at timestamptz,
  _watermark_effective timestamptz
);

create index if not exists idx_stg_ib_receipts_latest_updated_at
on stg_ib_receipts(updated_at);

create table if not exists stg_ob_orders_history (
  id uuid not null,
  updated_at timestamptz not null,
  payload jsonb not null,
  payload_hash text not null,
  _run_id text,
  _extracted_at timestamptz,
  _watermark_effective timestamptz,
  primary key (id, updated_at, payload_hash)
);

create table if not exists stg_ob_orders (
  id uuid primary key,
  updated_at timestamptz not null,
  payload jsonb not null,
  payload_hash text not null,
  _run_id text,
  _extracted_at timestamptz,
  _watermark_effective timestamptz
);

create index if not exists idx_stg_ob_orders_latest_updated_at
on stg_ob_orders(updated_at);