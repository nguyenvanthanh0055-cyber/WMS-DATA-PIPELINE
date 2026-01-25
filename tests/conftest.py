import os
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from services.common.db import build_engine


DDL_WATERMARK = """
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
"""


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    dsn = os.getenv("TEST_PG_DSN")
    if not dsn or "wms_dw_test" not in dsn:
        raise RuntimeError(f"Refusing to run integration tests without TEST_PG_DSN to wms_dw_test. Got: {dsn}")
    return dsn


@pytest.fixture(scope="session")
def engine(pg_dsn) -> Engine:
    eng = build_engine(pg_dsn)
    with eng.begin() as conn:
        for st in [ s for s in DDL_WATERMARK.split(";") if s.strip()]:
            conn.execute(text(st))
    return eng



@pytest.fixture(autouse=True)
def _clean_watermark_table(engine):
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE etl_watermark;"))
        conn.execute(text("TRUNCATE stg_ib_receipts_history, stg_ib_receipts;"))
        conn.execute(text("TRUNCATE pipeline_run_log;"))
    
