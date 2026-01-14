from datetime import datetime, timezone, timedelta
from sqlalchemy import text

from services.extractor.app.config import load_config
from services.extractor.app.watermark_repo import get_watermark, upsert_watermark

cfg = load_config()

pipeline_name = cfg.pipeline_name
def _utc(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc)


def test_get_watermark_returns_default_when_empty(engine):
    wm = get_watermark(
        engine=engine,
        pipeline_name=pipeline_name,
        entity="ib_receipts",
        default_time_start="2026-01-01T00:00:00Z",
    )
    assert wm == _utc(2026, 1, 1, 0, 0, 0)


def test_upsert_inserts_first_time(engine):
    upsert_watermark(
        engine=engine,
        pipeline_name=pipeline_name,
        entity="ib_receipts",
        new_wm=_utc(2026, 1, 2, 3, 4, 5),
        run_id="run-1",
    )

    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT last_success_time, last_success_run_id
                FROM etl_watermark
                WHERE pipeline_name=:p AND entity=:e
            """),
            {"p": pipeline_name, "e": "ib_receipts"},
        ).fetchone()

    assert row is not None
    assert row[0] == _utc(2026, 1, 2, 3, 4, 5)
    assert row[1] == "run-1"


def test_upsert_does_not_move_watermark_backwards_greatest(engine):

    upsert_watermark(engine, pipeline_name, "ib_receipts", _utc(2026, 1, 5, 0, 0, 0), "run-new")

    upsert_watermark(engine, pipeline_name, "ib_receipts", _utc(2026, 1, 3, 0, 0, 0), "run-old")

    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT last_success_time, last_success_run_id
                FROM etl_watermark
                WHERE pipeline_name=:p AND entity=:e
            """),
            {"p": pipeline_name, "e": "ib_receipts"},
        ).fetchone()

    assert row[0] == _utc(2026, 1, 5, 0, 0, 0)
    assert row[1] == "run-old"


def test_get_watermark_returns_db_value_after_upsert(engine):
    upsert_watermark(engine, pipeline_name, "ob_orders", _utc(2026, 1, 10, 12, 0, 0), "run-9")

    wm = get_watermark(
        engine=engine,
        pipeline_name=pipeline_name,
        entity="ob_orders",
        default_time_start="2026-01-01T00:00:00Z",
    )
    assert wm == _utc(2026, 1, 10, 12, 0, 0)


def test_updated_at_changes_on_upsert(engine):
    upsert_watermark(engine, pipeline_name, "ib_receipts", _utc(2026, 1, 2, 0, 0, 0), "run-1")

    with engine.connect() as conn:
        before = conn.execute(
            text("""
                SELECT updated_at
                FROM etl_watermark
                WHERE pipeline_name=:p AND entity=:e
            """),
            {"p": pipeline_name, "e": "ib_receipts"},
        ).scalar_one()

    upsert_watermark(engine, pipeline_name, "ib_receipts", _utc(2026, 1, 3, 0, 0, 0), "run-2")

    with engine.connect() as conn:
        after = conn.execute(
            text("""
                SELECT updated_at
                FROM etl_watermark
                WHERE pipeline_name=:p AND entity=:e
            """),
            {"p": pipeline_name, "e": "ib_receipts"},
        ).scalar_one()

    assert after >= before
