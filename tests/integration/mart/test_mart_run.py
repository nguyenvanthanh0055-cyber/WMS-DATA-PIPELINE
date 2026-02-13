import json
from pathlib import Path

import pytest
from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[3]
SQL_IB_LOAD = (ROOT / "sql/mart/10_ib_load.sql").read_text(encoding="utf-8")
SQL_IB_LOAD_LINE = (ROOT / "sql/mart/11_ib_load_line.sql").read_text(encoding="utf-8")
SQL_OB_LOAD = (ROOT / "sql/mart/12_ob_load.sql").read_text(encoding="utf-8")
SQL_OB_LOAD_LINE = (ROOT / "sql/mart/13_ob_load_line.sql").read_text(encoding="utf-8")
SQL_INIT_MART = (ROOT / "sql/01_init_mart.sql").read_text(encoding="utf-8")
SQL_SEED_DATE = (ROOT / "sql/02_seed_dim_date.sql").read_text(encoding="utf-8")


@pytest.fixture(scope="module", autouse=True)
def _init_mart_schema(engine):
    with engine.begin() as conn:
        for st in [s.strip() for s in SQL_INIT_MART.split(";") if s.strip()]:
            conn.execute(text(st))
        conn.execute(text(SQL_SEED_DATE))


@pytest.fixture(autouse=True)
def _clean_mart_and_stg(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                TRUNCATE TABLE
                    stg.ib_receipts,
                    stg.ob_orders,
                    mart.fact_ib_receipt_line,
                    mart.fact_ib_receipt,
                    mart.fact_ob_order_line,
                    mart.fact_ob_order,
                    mart.dim_product,
                    mart.dim_user,
                    mart.dim_warehouse,
                    mart.dim_client,
                    mart.dim_customer,
                    mart.dim_shipping_address
                RESTART IDENTITY CASCADE
                """
            )
        )


def _insert_stg_ib(engine, receipt_id: str, updated_at: str, status: str = "NEW") -> None:
    payload = {
        "id": receipt_id,
        "po_code": "PO-001",
        "status": status,
        "client_id": "1",
        "warehouse_id": "101",
        "created_by": "system",
        "updated_by": "system",
        "po_date": "2026-02-01",
        "created_at": "2026-02-01T09:00:00Z",
        "updated_at": updated_at,
        "finished_at": None,
        "lines_json": json.dumps(
            [
                {
                    "line_id": "L1",
                    "product_id": "1001",
                    "sku": "SKU-1001",
                    "qty_unit_id": "1",
                    "expected_qty": 10,
                    "actual_qty": 0,
                },
                {
                    "line_id": "L2",
                    "product_id": "1002",
                    "sku": "SKU-1002",
                    "qty_unit_id": "1",
                    "expected_qty": 20,
                    "actual_qty": 0,
                },
            ]
        ),
    }
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO stg.ib_receipts(
                    id, updated_at, payload, payload_hash, _run_id, _extracted_at, _watermark_effective
                )
                VALUES(
                    :id, :updated_at, (:payload)::jsonb, 'h1', 'r1', :updated_at, :updated_at
                )
                """
            ),
            {"id": receipt_id, "updated_at": updated_at, "payload": json.dumps(payload)},
        )


def _insert_stg_ob(engine, order_id: str, updated_at: str, status: str = "NEW") -> None:
    payload = {
        "id": order_id,
        "so_code": "SO-001",
        "status": status,
        "client_id": "1",
        "warehouse_id": "101",
        "created_by": "system",
        "updated_by": "system",
        "customer_id": "2001",
        "shipping_address_id": "3001",
        "expected_delivery_date": "2026-02-03",
        "actual_delivery_date": None,
        "created_at": "2026-02-01T10:00:00Z",
        "updated_at": updated_at,
        "total_amount": 1000,
        "actual_amount": 0,
        "total_cod_amount": 0,
        "total_weight": 1.5,
        "total_volume": 0.2,
        "lines_json": json.dumps(
            [
                {"line_id": "OL1", "product_id": "1001", "sku": "SKU-1001", "qty": 2},
                {"line_id": "OL2", "product_id": "1003", "sku": "SKU-1003", "qty": 3},
            ]
        ),
    }
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO stg.ob_orders(
                    id, updated_at, payload, payload_hash, _run_id, _extracted_at, _watermark_effective
                )
                VALUES(
                    :id, :updated_at, (:payload)::jsonb, 'h1', 'r1', :updated_at, :updated_at
                )
                """
            ),
            {"id": order_id, "updated_at": updated_at, "payload": json.dumps(payload)},
        )


def test_ib_load_first_run_inserts(engine):
    _insert_stg_ib(engine, "550e8400-e29b-41d4-a716-446655440001", "2026-02-01T10:00:00Z")

    with engine.begin() as conn:
        row = conn.execute(text(SQL_IB_LOAD), {"wm_effective": "2026-01-01T00:00:00Z"}).mappings().one()
        conn.execute(text(SQL_IB_LOAD_LINE))
        fact_count = conn.execute(text("select count(*) from mart.fact_ib_receipt")).scalar_one()
        line_count = conn.execute(text("select count(*) from mart.fact_ib_receipt_line")).scalar_one()

    assert row["rows_inserted"] == 1
    assert row["rows_updated"] == 0
    assert str(row["new_wm"]).startswith("2026-02-01 10:00:00")
    assert fact_count == 1
    assert line_count == 2


def test_ib_load_rerun_no_change(engine):
    _insert_stg_ib(engine, "550e8400-e29b-41d4-a716-446655440002", "2026-02-01T10:00:00Z")
    with engine.begin() as conn:
        conn.execute(text(SQL_IB_LOAD), {"wm_effective": "2026-01-01T00:00:00Z"})
        conn.execute(text(SQL_IB_LOAD_LINE))
        row2 = conn.execute(text(SQL_IB_LOAD), {"wm_effective": "2026-01-01T00:00:00Z"}).mappings().one()

    assert row2["rows_inserted"] == 0
    assert row2["rows_updated"] == 0


def test_ob_load_first_run_inserts(engine):
    _insert_stg_ob(engine, "550e8400-e29b-41d4-a716-446655440003", "2026-02-01T11:00:00Z")

    with engine.begin() as conn:
        row = conn.execute(text(SQL_OB_LOAD), {"wm_effective": "2026-01-01T00:00:00Z"}).mappings().one()
        conn.execute(text(SQL_OB_LOAD_LINE))
        fact_count = conn.execute(text("select count(*) from mart.fact_ob_order")).scalar_one()
        line_count = conn.execute(text("select count(*) from mart.fact_ob_order_line")).scalar_one()

    assert row["rows_inserted"] == 1
    assert row["rows_updated"] == 0
    assert str(row["new_wm"]).startswith("2026-02-01 11:00:00")
    assert fact_count == 1
    assert line_count == 2


def test_ob_load_rerun_no_change(engine):
    _insert_stg_ob(engine, "550e8400-e29b-41d4-a716-446655440004", "2026-02-01T12:00:00Z")
    with engine.begin() as conn:
        conn.execute(text(SQL_OB_LOAD), {"wm_effective": "2026-01-01T00:00:00Z"})
        conn.execute(text(SQL_OB_LOAD_LINE))
        row2 = conn.execute(text(SQL_OB_LOAD), {"wm_effective": "2026-01-01T00:00:00Z"}).mappings().one()

    assert row2["rows_inserted"] == 0
    assert row2["rows_updated"] == 0
