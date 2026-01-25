from typing import Any, Iterator
import pandas as pd
import json
from sqlalchemy import text
from sqlalchemy.engine import Engine


TABLE: dict[str, tuple[str, str]] = {
    "ib_receipts": ("stg_ib_receipts_history", "stg_ib_receipts"),
    "ob_orders": ("stg_ob_orders_history", "stg_ob_orders")
}

def _get_table(entity: str) -> tuple[str, str]:
    if entity not in TABLE:
        return ValueError(f"Unsupported entity: {entity}")
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
    INSERT INTO {history_table}(
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
    INSERT INTO {latest_table}(
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


def df_to_records_for_db(df: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    
    for row in df.to_dict(orient="records"):
        payload = row["payload"]
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        
        records.append({
            "id": row["id"],
            "updated_at": row["updated_at"],
            "payload": payload_json,
            "payload_hash": row["payload_hash"],
            "_run_id": row["_run_id"],
            "_extracted_at": row["_extracted_at"],
            "_watermark_effective": row["_watermark_effective"]
        })
    return records

