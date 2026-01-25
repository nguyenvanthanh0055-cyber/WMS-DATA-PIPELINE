from sqlalchemy import text
from sqlalchemy.engine import Engine

from typing import Any


def start_run_log(
    engine: Engine,
    run_id: str,
    pipeline_name: str,
    entity: str
    ) -> None:
    sql = """
    INSERT INTO pipeline_run_log(run_id, pipeline_name, entity, status, started_at)
    VALUES(:run_id, :pipeline_name, :entity, 'running', now())
    ON CONFLICT (run_id) DO NOTHING
    """
    with engine.begin() as conn:
        conn.execute(text(sql), 
                    {"run_id": run_id,
                     "pipeline_name": pipeline_name,
                     "entity": entity,
                     }
                    )
        
def finish_run_success(
    engine: Engine,
    run_id: str,
    rows_in: int,
    inserted_history: int,
    upserted_latest: int
) -> None:
    sql = """
    UPDATE pipeline_run_log
    SET 
        status = 'success',
        ended_at = now(),
        rows_in = :rows_in,
        rows_inserted_history = :inserted_history,
        rows_upserted_latest = :upserted_latest,
        error = NULL
    WHERE run_id = :run_id
    """
    
    with engine.begin() as conn:
        conn.execute(text(sql),{
            "run_id": run_id,
            "rows_in": rows_in,
            "inserted_history": inserted_history,
            "upserted_latest": upserted_latest   
        })

def finish_run_failed(
    engine: Engine,
    run_id: str,
    error_message: str
) -> None:
    sql = """
    UPDATE pipeline_run_log
    SET
        status = 'failed',
        ended_at = now(),
        error = :error
    WHERE run_id = :run_id
    """
    
    with engine.begin() as conn:
        conn.execute(text(sql),{
            "run_id": run_id,
            "error": error_message[:4000]
        })