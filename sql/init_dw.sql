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
