
from services.common.db import build_engine
from services.staging.app.config import load_staging_config
from services.staging.app.payload import build_payload_and_hash
from services.common.pipeline_run_logs_repo import start_run_log, finish_run_success, finish_run_failed
from services.staging.app.reader_landing import reader_landing
from services.staging.app.staging_repo import insert_history, upsert_stg_latest
import logging
import argparse
from typing import Optional

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
    p.add_argument("--batch_size", type=int, default=500)
    return p.parse_args()


def main(args: Optional[list[str]] = None) -> int:
        
    _setup_logging()
    args = parse_args(args)
    entity = args.entity
    run_id = args.run_id
    cfg = load_staging_config()
    engine = build_engine(cfg.common.pg_dsn)
    pipeline_name = cfg.pipeline_name

    
    try:
        start_run_log(
            engine=engine,
            run_id=run_id,
            pipeline_name=pipeline_name,
            entity=entity
        )

        
        df = reader_landing(
            landing_root=cfg.common.landing_root,
            entity=entity,
            run_id=run_id
        )
      
        rows_in = len(df)
        if rows_in == 0:
            logger.info("landing loaded entity=%s run_id=%s rows_in=%s cols=%s",
                        entity, run_id, rows_in, df.columns)
            finish_run_success(
                engine=engine,
                run_id=run_id,
                pipeline_name=pipeline_name,
                entity=entity,
                rows_in=0,
                inserted_history=0,
                affected_latest=0
            )
        
        df2 = build_payload_and_hash(df)
        logger.info("payload_build entity=%s run_id=%s rows=%s",entity, run_id, len(df2)) 
        records = df2.to_dict(orient="records") 
        inserted_history = insert_history(
            engine=engine,
            entity=entity,
            records=records,
            batch_size=args.batch_size    
            )
        affected_latest = upsert_stg_latest(
            engine=engine,
            entity=entity,
            records=records,
            batch_size=args.batch_size
        )
        finish_run_success(
            engine=engine,
            run_id=run_id,
            pipeline_name=pipeline_name,
            entity=entity,
            rows_in=rows_in,
            inserted_history=inserted_history,
            affected_latest=affected_latest
        )
        return 0
    except Exception as e:
        logger.exception("Failure at the staging stage")
        try:
            finish_run_failed(
                engine=engine,
                run_id=run_id,
                pipeline_name=pipeline_name,
                entity=entity,
                error_message=str(e)
            )
        except Exception:
            logger.exception("Failed to update run log entity=%s run_id%s", entity, run_id)
        
    
if __name__ == "__main__":
    raise SystemExit(main())
