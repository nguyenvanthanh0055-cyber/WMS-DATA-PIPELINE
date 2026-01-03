import os
import logging
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class Config:
    wms_base_url: str
    pg_dsn: str
    pipeline_name: str
    limit: int
    lookback_seconds: int
    output_format: str
    request_timeout_seconds: int
    
def load_config() -> Config:
    wms_base_url = os.getenv("WMS_BASE_URL","http://localhost:8000").rstrip("/")
    
    if not os.getenv("PG_DSN"):
        raise RuntimeError(f"Missing required env var PG_DSN")
    else: 
        pg_dsn = os.getenv("PG_DSN")
        
    pipeline_name = os.getenv("PIPELINE_NAME","wms_dw")
    limit = int(os.getenv("LIMIT", "500"))
    lookback_seconds = int(os.getenv("LOOKBACK_SECONDS", "120"))
    output_format = os.getenv("OUTPUT_FORMAT", "parquet").lower().strip()
    
    if output_format not in ("csv", "parquet"):
        output_format = "parquet"
        logging.warn(f"Output_format is error, auto using parquet")
    
    request_timeout_seconds = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
    
    return Config(
        wms_base_url = wms_base_url,
        pg_dsn = pg_dsn,
        pipeline_name = pipeline_name,
        limit = limit,
        lookback_seconds = lookback_seconds,
        output_format = output_format,
        request_timeout_seconds = request_timeout_seconds
    )