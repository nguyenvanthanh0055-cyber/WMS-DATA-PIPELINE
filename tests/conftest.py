import os
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from services.extractor.app.db import build_engine


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
