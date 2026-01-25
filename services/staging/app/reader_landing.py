from pathlib import Path
import pandas as pd
from services.common.config import load_config

def reader_landing(landing_root: Path, entity: str, run_id: str) -> pd.DataFrame:
    path = landing_root / entity / f"run_id={run_id}"
    p_parquet = path / "part-000.parquet"
    p_csv = path / "part-000.csv"
    
    if p_parquet.exists():
        df = pd.read_parquet(p_parquet)
    elif p_csv.exists():
        df = pd.read_csv(p_csv)
    else:
        raise FileNotFoundError(f"Landing not found: {p_parquet} or {p_csv}")
    
    required = {"id", "updated_at", "_run_id", "_extracted_at"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Landing missing columns {missing}")
    
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")
    df["_extracted_at"] = pd.to_datetime(df["_extracted_at"], utc=True, errors="coerce")
    df["_watermark_effective"] = pd.to_datetime(df["_watermark_effective"], utc=True, errors="coerce")
    
    return df


