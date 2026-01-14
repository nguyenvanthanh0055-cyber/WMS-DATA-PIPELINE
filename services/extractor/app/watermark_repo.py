from datetime import datetime, timezone
from typing import Any
from sqlalchemy import text
from sqlalchemy.engine import Engine

def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _parse_default_iso(default_time_start: str) -> datetime:
    
    iso = default_time_start.replace("Z", "+00:00")
    
    return _to_utc(datetime.fromisoformat(iso))


def get_watermark(
    engine: Engine,
    pipeline_name: str,
    entity: str,
    default_time_start: str
) -> datetime:
    sql = text("""
               SELECT last_success_time
               FROM etl_watermark
               WHERE pipeline_name = :p AND entity = :e
               """)
    with engine.connect() as conn:
        row = conn.execute(sql, {
            "p": pipeline_name,
            "e": entity
        }).fetchone()
        
        if row and row[0]:
            return _to_utc(row[0])
        
        return _parse_default_iso(default_time_start)
        
def upsert_watermark(
    engine: Engine,
    pipeline_name: str,
    entity: str,
    new_wm: datetime,
    run_id: str
) -> None:
    
    sql = text("""
               INSERT INTO etl_watermark (pipeline_name, entity, last_success_time, last_success_run_id)
               VALUES(:p, :e, :t, :r)
               ON CONFLICT (pipeline_name, entity)
               DO UPDATE SET
                last_success_time = greatest(etl_watermark.last_success_time, excluded.last_success_time),
                last_success_run_id = excluded.last_success_run_id,
                updated_at = NOW()
               """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "p": pipeline_name,
            "e": entity,
            "t": _to_utc(new_wm),
            "r": run_id
            
        })