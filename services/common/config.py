import os
import logging
from dataclasses import dataclass
from pathlib import Path
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
    default_start_time: str
    landing_root: Path
    
    
def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except ValueError as e:
        raise RuntimeError(f" ENV var {name} must be int, got {raw}") from e
    
def load_config() -> Config:
    wms_base_url = os.getenv("WMS_BASE_URL","http://localhost:8000").rstrip("/")
    
    pg_dsn = os.getenv("PG_DSN")
    if not pg_dsn:
        raise RuntimeError("Missing required env var: PG_DSN")
        
    pipeline_name = os.getenv("PIPELINE_NAME","wms_dw")
    limit = _env_int("LIMIT", 500)
    lookback_seconds = _env_int("LOOKBACK_SECONDS", 120)
    output_format = os.getenv("OUTPUT_FORMAT", "parquet").lower().strip()
    
    if output_format not in ("csv", "parquet"):
        output_format = "parquet"
        logger.warning(f"Output_format is error, auto using parquet")
    
    request_timeout_seconds = _env_int("REQUEST_TIMEOUT_SECONDS", 20)
    
    default_start_time = os.getenv("DEFAULT_START_TIME", "1970-01-01T00:00:00Z")
    landing_root_env = os.getenv("LANDING_ROOT")
    if landing_root_env:
        landing_root = Path(landing_root_env)
    else:
        project_root = Path(__file__).resolve().parents[3]
        landing_root = project_root / "data" / "landing"
    return Config(
        wms_base_url = wms_base_url,
        pg_dsn = pg_dsn,
        pipeline_name = pipeline_name,
        limit = limit,
        lookback_seconds = lookback_seconds,
        output_format = output_format,
        request_timeout_seconds = request_timeout_seconds,
        default_start_time= default_start_time,
        landing_root= landing_root
    )
