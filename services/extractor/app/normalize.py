from typing import Any
from datetime import datetime, timezone
import pandas as pd
import json

def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _parse_cols_time(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if c.endswith("_at"):
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
        elif c.endswith("_date"):
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    
    return df

def _flatten_nested(df: pd.DataFrame) -> pd.DataFrame:
    if "lines" in df.columns:
        df["lines_json"] = df["lines"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
        )
        df = df.drop(columns=["lines"])
    
    return df

def normalize_rows(
    rows: Any,
    entity: str,
    run_id: str,
    extracted_at: datetime,
    watermark_effective: datetime
    ) -> pd.DataFrame:
    
    if not rows:
        return pd.DataFrame()
    
    if not isinstance(rows, list):
        raise RuntimeError(f"normalize_rows expects list[dict] but got {type(rows)}")
    
    df = pd.DataFrame(rows)
    df = _parse_cols_time(df)
    
    required_cols = ["id", "updated_at"]
    missing_data = [c for c in required_cols if c not in df.columns]
    
    if missing_data:
        raise RuntimeError(f"Missing required colums {missing_data} for entity {entity} ")
    else:
        null_mask = df[required_cols].isna().any(axis=1)
        if null_mask.any():
            bad_data = df.loc[null_mask, required_cols].head(5).to_dict(orient="records")
            raise RuntimeError(f"Null in required fields for entity {entity}, sample = {bad_data}")
    
    df = _flatten_nested(df)
    df["id"] = df["id"].astype("string")
    
    df["_run_id"] = run_id
    df["_extracted_at"] = _to_utc(extracted_at)
    df["_watermark_effective"] = _to_utc(watermark_effective)
    
    df = df.sort_values(["updated_at", "id"], ascending=[True, True], kind="mergesort")
    df = df.drop_duplicates(subset=["id", "updated_at"], keep="last")
    
    return df
    
    
    
    
    
        
    