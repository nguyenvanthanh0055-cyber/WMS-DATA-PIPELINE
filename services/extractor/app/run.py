import logging
from datetime import datetime, timedelta, timezone
import uuid

from services.common.config import load_config
from services.common.db import build_engine
from services.extractor.app.http_client import build_session
from services.extractor.app.extract import fetch_all
from services.extractor.app.normalize import normalize_rows
from services.extractor.app.watermark_repo import get_watermark, upsert_watermark
from services.extractor.app.writer_landing import write_landing

logger = logging.getLogger(__name__)

def main():
    cfg = load_config()

    engine = build_engine(cfg.pg_dsn)
    
    run_id = uuid.uuid4().hex
    extracted_at = datetime.now(timezone.utc)
    session = build_session()
    entities = ["ib_receipts", "ob_orders"]
    
    for entity in entities:
        wm_saved = get_watermark(engine, cfg.pipeline_name, entity, cfg.default_start_time)
        wm_effective = wm_saved - timedelta(seconds=cfg.lookback_seconds)

        logger.info(
            "[%s] watermark_saved=%s watermark_effective=%s lookback_seconds=%s run_id=%s",
            entity, wm_saved, wm_effective, cfg.lookback_seconds, run_id
        )
    
        rows = fetch_all(
            session=session,
            base_url=cfg.wms_base_url,
            entity=entity,
            updated_after=wm_effective,
            limit=cfg.limit,
            request_timeout_seconds=cfg.request_timeout_seconds,
        )
        logger.info("[%s] fetched_rows=%s", entity, len(rows))
        
        df = normalize_rows(
            rows=rows,
            entity=entity,
            run_id=run_id,
            extracted_at=extracted_at,
            watermark_effective=wm_effective,
        )
        logger.info("Normalize data for %s (rows=%s)", entity, len(rows))
        
        if not df.empty:
            logger.info(
                "[%s] updated_at min=%s max=%s",
                entity,
                df["updated_at"].min(),
                df["updated_at"].max(),
            )
            logger.info("[%s] sample ids=%s", entity, df["id"].head(5).tolist())

        
        landing_file = write_landing(
            df=df,
            landing_root=cfg.landing_root,
            entity=entity,
            run_id=run_id,
            output_format=cfg.output_format,
        )
        logger.info("Data written to landing file: %s", landing_file)
        
        if df is not None and not df.empty:
            new_wm = df["updated_at"].max().to_pydatetime()
        else:
            new_wm = wm_saved
        
        upsert_watermark(
            engine=engine,
            pipeline_name=cfg.pipeline_name,
            entity=entity,
            new_wm=new_wm,
            run_id=run_id,
        )
        logger.info("Watermark updated new_wm=%s", new_wm)
        
        logger.info(
            "[%s] wm_saved=%s wm_effective=%s fetched=%s new_wm=%s",
            entity,
            wm_saved,
            wm_effective,
            len(rows),
            new_wm,
        )

    
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    main()
