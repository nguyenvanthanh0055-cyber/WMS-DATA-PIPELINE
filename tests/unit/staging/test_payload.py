import json
import hashlib
import pandas as pd

from services.staging.app.payload import build_payload_and_hash

def test_payload_excludes_metadata():
    df = pd.DataFrame([{"id":1, "status":"done", "_run_id":"r1"}])
    out = build_payload_and_hash(df)
    payload = json.loads(out.payload[0])
    assert "_run_id" not in payload
    assert "status" in payload

def test_payload_hash_is_sha256_of_payload_string():
    out = build_payload_and_hash(pd.DataFrame([{"id":1, "status":"done"}]))
    s = out.payload[0]
    assert out.payload_hash[0] == hashlib.sha256(s.encode("utf-8")).hexdigest()
