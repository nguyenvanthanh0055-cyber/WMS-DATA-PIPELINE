import json, hashlib
from typing import Any
import datetime as dt
import pandas as pd

def _normalize_for_json(v: Any) -> Any:
    if v is None:
        return None
    if pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime().isoformat()
    if isinstance(v, dt.datetime):
        return v.isoformat()
    if isinstance(v, dt.date):
        return v.isoformat()
    return v
    
def build_payload_and_hash(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in df.columns if not c.startswith("_")]
    
    def row_payload(row: Any)-> dict[str, Any]:
        return {c: _normalize_for_json(row[c]) for c in cols}
    payload = df.apply(row_payload, axis=1)
    df = df.copy()
    df["payload"] = payload.apply(lambda p: json.dumps(p, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    def hash_payload(s: str) -> str:
        return hashlib.sha256(s.encode("utf-8")).hexdigest()
    
    df["payload_hash"] = df["payload"].apply(hash_payload)
    return df

   
