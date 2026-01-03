import random
import time
import requests
import logging
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = (5, 30)

def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "WMS_PIPELINE/1.0",
        "Accept": "application/json"
    })
    return s

def get_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
    max_retries: int = 3
    ) -> Any:
    
    for i in range(max_retries + 1):
        base = 0.5 * (2 ** i)
        sleep_s = base * (0.5 + 0.5 * random.random())
        try:
            resp = session.get(url=url, params=params, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                if i == max_retries:
                    resp.raise_for_status()
                logger.warning(
                    "Retryable HTTP %s for %s (attempt %d/%d). Sleep %.2fs ",
                    resp.status_code, url, i+1, max_retries + 1, sleep_s
                )
                time.sleep(sleep_s)
                continue
            if resp.status_code >= 400:
                resp.raise_for_status()
            try:
                return resp.json()
            except ValueError as e:
                    raise RuntimeError(
                    f"Response is not JSON: url={resp.url} status={resp.status_code} body={resp.text[:300]}"
                    ) from e
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if i == max_retries:
                raise
            logger.warning(
                "Network error  for %s (attempt %d/%d): %s. Sleep %.2fs",
                 url, i+1, max_retries+1, str(e), sleep_s
            )
            time.sleep(sleep_s)
            
            
            