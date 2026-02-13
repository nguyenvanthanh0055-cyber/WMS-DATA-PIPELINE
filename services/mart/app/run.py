from typing import Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datetime import datetime, timezone, timedelta
from .config import load_config_mart
from services.common.db import build_engine
from services.common.watermark_repo import get_watermark, upsert_watermark
from services.common.pipeline_run_logs_repo import start_run_log, finish_run_success, finish_run_failed
import logging
import argparse

logger = logging.getLogger(__name__)

def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

def parse_args(args: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--entity", required=True, choices=["ib_receipts", "ob_orders"])
    p.add_argument("--run-id", required=True, dest="run_id")
    return p.parse_args()

def main(args: Optional[list[str]] = None) -> int:
    _setup_logging()
    cfg = load_config_mart()
    engine = build_engine(cfg.common.pg_dsn)
    args = parse_args(args)
    entity = args.entity
    run_id = args.run_id
    pipeline_name = cfg.pipeline_name
    default_start_time = cfg.common.default_start_time
    
    if entity == "ib_receipts":
        sql_load = cfg.sql_ib_receipts.read_text(encoding="utf-8")
        sql_load_line = cfg.sql_ib_receipts_line.read_text(encoding="utf-8")
    else:
        sql_load = cfg.sql_ob_orders.read_text(encoding="utf-8")
        sql_load_line = cfg.sql_ob_orders_line.read_text(encoding="utf-8")
  
    
    with engine.begin() as conn:
        start_run_log(
            engine=engine,
            run_id=run_id,
            pipeline_name=pipeline_name,
            entity=entity
        )
        logger.info("pipiline_run_log started")
    
    try:
        
        wm_saved = get_watermark(
            engine=engine,
            pipeline_name=pipeline_name,
            entity=entity,
            default_time_start=default_start_time        
        )
        
        wm_effective = wm_saved - timedelta(seconds=cfg.common.lookback_seconds)
        
        with engine.begin() as conn:
            row = conn.execute(text(sql_load),{"wm_effective":wm_effective}).fetchone()
            new_wm = row["new_wm"]
            rows_inserted = row["rows_inserted"]
            rows_updated = row["rows_updated"]
            rows_affected = rows_inserted + rows_updated
            logger.info("[%s] wm_saved =%s wm_effective=%s" , entity, wm_saved, wm_effective)
            conn.execute(text(sql_load_line))
            rows_in = conn.execute(text("select count(*) from tmp_changed")).scalar()

            logger.info("[%s] rows_candidate=%s",entity, rows_in)

            if new_wm is not None:
                upsert_watermark(
                    engine=engine,
                    pipeline_name=pipeline_name,
                    entity=entity,
                    new_wm=new_wm,
                    run_id=run_id
                )
                logger.info("[%s] watermark updated new_wm=%s", entity, new_wm)
            finish_run_success(
                engine=engine,
                run_id=run_id,
                pipeline_name=pipeline_name,
                entity=entity,
                rows_in=rows_in,
                inserted_history= rows_inserted,
                affected_latest=rows_affected
            ) 
            logger.info("[%s] pipeline run log updated with run_id=%s rows_candiate=%s, inserted_history=%s, affected_latest=%s",
                        entity, run_id, rows_in, rows_inserted, rows_affected)
    except Exception as e:
        finish_run_failed(
            engine=engine,
            run_id=run_id,
            pipeline_name=pipeline_name,
            entity=entity,
            error_message=str(e)[:400]
        )
        raise

if __name__ == "__main__":
    raise SystemExit(main())
            
        
