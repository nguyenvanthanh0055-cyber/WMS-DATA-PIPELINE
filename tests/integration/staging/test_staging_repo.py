import pytest
from sqlalchemy import text
import uuid
from services.staging.app.staging_repo import insert_history, upsert_stg_latest
from services.staging.app.pipeline_run_logs_repo import start_run_log, finish_run_success

def test_history_global_dedup(engine):
    records = [{
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "updated_at": "2026-01-23T10:00:00Z",
        "payload": '{"id": "550e8400-e29b-41d4-a716-446655440000","status": "running","updated_at": "2026-01-23T04:33:31+00:00"}',
        "payload_hash": "h1",
        "_run_id": "r1",
        "_extracted_at": "2026-01-23T10:01:00Z",
        "_watermark_effective": None,
    }]

    insert_history(engine, "ib_receipts", records, batch_size=1000)
    insert_history(engine, "ib_receipts", records, batch_size=1000)  # rerun

    with engine.begin() as conn:
        n = conn.execute(
            text("select count(*) from stg_ib_receipts_history where id=:id and payload_hash=:payload_hash"),
                 {"id": "550e8400-e29b-41d4-a716-446655440000","payload_hash":"h1"}
        ).scalar_one()
        
    assert n == 1

def test_latest_newer_wins(engine):
    r_old = [{
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "updated_at": "2026-01-23T10:00:00Z",
        "payload": '{"id": "550e8400-e29b-41d4-a716-446655440000","status": "running","updated_at": "2026-01-23T04:33:31+00:00"}',
        "payload_hash": "h_old",
        "_run_id": "r1",
        "_extracted_at": "2026-01-23T10:01:00Z",
        "_watermark_effective": None,
    }]
    r_new = [{
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "updated_at": "2026-01-23T10:05:00Z",
        "payload": '{"id": "550e8400-e29b-41d4-a716-446655440000","status": "running","updated_at": "2026-01-23T04:33:31+00:00"}',
        "payload_hash": "h_old",
        "_run_id": "r1",
        "_extracted_at": "2026-01-23T10:06:00Z",
        "_watermark_effective": None,
    }]
    r_older = [{
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "updated_at": "2026-01-23T09:55:00Z",
        "payload": '{"id": "550e8400-e29b-41d4-a716-446655440000","status": "running","updated_at": "2026-01-23T04:33:31+00:00"}',
        "payload_hash": "h1",
        "_run_id": "r1",
        "_extracted_at": "2026-01-23T10:07:00Z",
        "_watermark_effective": None,
    }]

    upsert_stg_latest(engine, "ib_receipts", r_old, batch_size=1000)
    upsert_stg_latest(engine, "ib_receipts", r_new, batch_size=1000)

    with engine.begin() as conn:
        ts = conn.execute(
            text("select updated_at from stg_ib_receipts where id=:id"),{"id":"550e8400-e29b-41d4-a716-446655440000"}
        ).scalar_one()
    assert str(ts).startswith("2026-01-23 10:05:00")

    upsert_stg_latest(engine, "ib_receipts", r_older, batch_size=1000)

    with engine.begin() as conn:
        ts2 = conn.execute(
            text("select updated_at from stg_ib_receipts where id=:id"),{"id":"550e8400-e29b-41d4-a716-446655440000"}
        ).scalar_one()
    assert str(ts2).startswith("2026-01-23 10:05:00")



def test_run_log_success(engine):
    start_run_log(engine, run_id="r1", pipeline_name="wms", entity="ib_receipts")
    finish_run_success(engine, run_id="r1", rows_in=10, inserted_history=7, upserted_latest=5)

    with engine.begin() as conn:
        status = conn.execute(
            text("select status from pipeline_run_log where run_id='r1'")
        ).scalar_one()

    assert status == "success"
    