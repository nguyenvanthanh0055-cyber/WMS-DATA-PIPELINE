from pathlib import Path
import pandas as pd
import uuid
import logging

logger = logging.getLogger(__name__)

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)
    
def _atomic_replace(src: Path, dst: Path) -> None:
    src.parent.mkdir(parents=True, exist_ok=True)
    src.replace(dst)
    

def write_landing(
    df: pd.DataFrame,
    landing_root: Path,
    entity: str,
    run_id: str,
    output_format: str = "parquet"
) -> Path:
    
    output_format = output_format.lower().strip()
    
    if output_format not in ("parquet", "csv"):
        raise ValueError(f"Unsupported output_format: {output_format}")
    
    run_dir = landing_root / entity / f"run_id={run_id}"
    
    _ensure_dir(run_dir)
    
    ext = "parquet" if output_format == "parquet" else "csv"
    
    final_path = run_dir / f"part-000.{ext}"
    
    if final_path.exists():
        raise RuntimeError(f"Landing output already exists: {final_path}")
    
    tmp_path = run_dir / f"part-000.{uuid.uuid4().hex}.tmp.{ext}"
    
    if df is None or df.empty:
        logger.info("[%s] empty dataframe, writing empty landing file", entity)
    
    if output_format == "parquet":
        df.to_parquet(tmp_path)
    else:
        df.to_csv(tmp_path)
    
    _atomic_replace(tmp_path, final_path)
    
    logger.info("[%s] wrote landing file: %s (rows=%s)",entity, final_path, len(df))
    return final_path
    
    
    
    