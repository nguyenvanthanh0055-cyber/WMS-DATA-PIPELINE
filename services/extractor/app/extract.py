import requests
import logging
from datetime import datetime,timezone
from typing import Any
from .http_client import get_json

logger = logging.getLogger(__name__)

ENTITY_CFG = {
    "ib_receipts": {"path": "/ib/receipts"},
    "ob_orders": {"path": "o/b/orders"}
}

def _to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def _stable_key(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row.get("updated_at")), row.get("id"))

def _assert_stable_order(rows: list[dict[str, Any]], entity: str):
    prev = None
    
    for row in rows:
        k = _stable_key(row)
        
        if prev is not None and k < prev:
            raise RuntimeError(f"Unstable ordering detected for {entity}: {k} < {prev}")
        prev = k

def fetch_all(
    session: requests.Session,
    base_url: str,
    entity: str,
    updated_after: datetime,
    limit: int,
    request_timeout_seconds: int
) -> list[dict[str, Any]]:
    
    if entity not in ENTITY_CFG:
        raise ValueError(f"Unknown entity {entity}")
    offset = 0
    all_rows: list[dict[str, Any]] = []
    
    url = base_url.rstrip("/") + ENTITY_CFG[entity]["path"]
    
    params = {
        "updated_after": _to_iso(updated_after),
        "limit": limit,
        "offset": offset
    }
    
    while True:
        
        page = get_json(
            session=session,
            url=url,
            params=params,
            timeout=(5, request_timeout_seconds),
            max_retries=3
        )
        
        if not page:
            break
        
        data = page["data"]
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected API response type for {entity}: {type(data)}")
        
        _assert_stable_order(data, entity)    
        all_rows.extend(data)
        
        logger.info("[%s] fetched data offset=%s count=%s total=%s",entity, offset, len(data), len(all_rows))
        
        if len(data) < limit:
            break
        
        offset += limit
        
        if offset >= 2_000_000:
            raise RuntimeError(f"Pagination runaway for {entity}: offset= {offset}")
    
    return all_rows