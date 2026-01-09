import logging
from datetime import datetime, timezone

from services.extractor.app.config import load_config
from services.extractor.app.db import build_engine
from services.extractor.app.http_client import build_session
from services.extractor.app.extract import fetch_all
from services.extractor.app.normalize import normalize_rows
from services.extractor.app.watermark_repo import get_watermark, upsert_watermark
from services.extractor.app.writer_landing import write_landing

logger = logging.getLogger(__name__)

def main():
    
    
    cfg = load_config()

    engine = build_engine(cfg.pg_dsn)
    pipeline_name = cfg.pipeline_name
    
    updated_after = get_watermark(engine, pipeline_name, "ib_receipts", cfg.default_start_time)
    
    logger.info(f"watermark fetched: {updated_after}")
    
    session = build_session()
    
    base_url = cfg.wms_base_url
    entity = "ib_receipts"
    limit = cfg.limit
    request_timeout = cfg.request_timeout_seconds

    rows = fetch_all(session, base_url, entity, updated_after, limit, request_timeout)
    logger.info(f"Fetched {len(rows)} rows for entity {engine} after {updated_after}")
    
    df = normalize_rows(rows, engine, "run_1", datetime.now(timezone.utc), updated_after)
    logger.info(f"Normalize data for {entity} (rows={len(rows)})")
    
    landing_file = write_landing(df, cfg.landing_root, entity, "run_1", cfg.output_format)
    logger.info(f"Data written to landing file: {landing_file}")
    
    upsert_watermark(engine, pipeline_name, entity, updated_after, "run_1")
    logger.info(f"Watermark updated for entity {entity}")
    
if __name__ == "__main__":
    main()