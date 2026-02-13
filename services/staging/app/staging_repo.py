from typing import Any, Iterator
from sqlalchemy import text
from sqlalchemy.engine import Engine


TABLE: dict[str, tuple[str, str]] = {
    "ib_receipts": ("ib_receipts_history", "ib_receipts"),
    "ob_orders": ("ob_orders_history", "ob_orders")
}

def _get_table(entity: str) -> tuple[str, str]:
    if entity not in TABLE:
        raise ValueError(f"Unsupported entity: {entity}")
    return TABLE[entity]

def _batch(
    items: list[dict[str, Any]],
    batch_size: int
    ) -> Iterator[list[dict[str,Any]]]:
    for i in range(0, len(items), batch_size):
        yield items[i: i + batch_size]
        
def insert_history(
    engine: Engine,
    entity: str,
    records: list[dict[str, Any]],
    batch_size: int = 500
) -> int:
    history_table, _ = _get_table(entity)
    
    sql = text(f"""
    INSERT INTO stg.{history_table}(
        id, updated_at, payload,
        payload_hash, _run_id,
        _extracted_at, _watermark_effective
    )
    VALUES(
        :id, :updated_at, (:payload)::jsonb,
        :payload_hash, :_run_id,
        :_extracted_at, :_watermark_effective
    )
    ON CONFLICT (id, updated_at, payload_hash)
    DO NOTHING
    RETURNING 1
    """)
    inserted = 0
    
    

    
    with engine.begin() as conn:
        for b in _batch(records, batch_size):
            res = conn.execute(sql, b)
            inserted += int(res.rowcount or 0)
    
    return inserted



def upsert_stg_latest(
    engine: Engine,
    entity: str,
    records: list[dict[str, Any]],
    batch_size: int = 500
) -> int:
    _, latest_table = _get_table(entity)
    
    sql = text(f"""
    INSERT INTO stg.{latest_table}(
        id, updated_at, payload,
        payload_hash, _run_id,
        _extracted_at, _watermark_effective
    )
    VALUES(
        :id, :updated_at, (:payload)::jsonb,
        :payload_hash, :_run_id,
        :_extracted_at, :_watermark_effective   
    )
    ON CONFLICT (id)
    DO UPDATE SET
        updated_at = excluded.updated_at,
        payload = excluded.payload,
        payload_hash = excluded.payload_hash,
        _run_id = excluded._run_id,
        _extracted_at = excluded._extracted_at,
        _watermark_effective = excluded._watermark_effective
    WHERE excluded.updated_at > {latest_table}.updated_at
    RETURNING 1
    """)
    
    upserted = 0
    with engine.begin() as conn:
        for b in _batch(records, batch_size):
            res = conn.execute(sql, b)
            upserted += int(res.rowcount or 0)
    
    return upserted
