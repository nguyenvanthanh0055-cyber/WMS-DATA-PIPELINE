import pytest
import requests
from datetime import datetime, timezone

from services.extractor.app import extract


def _dt_utc(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc)


def test_fetch_all_unknown_entity_raises():
    s = requests.Session()
    with pytest.raises(ValueError):
        extract.fetch_all(
            session=s,
            base_url="http://test",
            entity="unknown",
            updated_after=_dt_utc(2026, 1, 1),
            limit=5,
            request_timeout_seconds=30,
        )


def test_fetch_all_paginates_until_last_page(monkeypatch):
    calls = []

    def fake_get_json(session, url, params, timeout, max_retries):
        calls.append(params.copy())
        if params["offset"] == 0:
            return {
                "data": [
                    {"id": "1", "updated_at": "2026-01-01T00:00:01Z"},
                    {"id": "2", "updated_at": "2026-01-01T00:00:02Z"},
                ],
                "meta": {"count": 2, "offset": 0},
            }
        if params["offset"] == 2:
            return {
                "data": [{"id": "3", "updated_at": "2026-01-01T00:00:03Z"}],
                "meta": {"count": 1, "offset": 2},
            }
        return {"data": [], "meta": {}}

    monkeypatch.setattr(extract, "get_json", fake_get_json)

    s = requests.Session()
    rows = extract.fetch_all(
        session=s,
        base_url="http://test",
        entity="ib_receipts",
        updated_after=_dt_utc(2026, 1, 1),
        limit=2,
        request_timeout_seconds=30,
    )

    assert [r["id"] for r in rows] == ["1", "2", "3"]
    assert [c["offset"] for c in calls] == [0, 2]


def test_fetch_all_breaks_on_empty_page(monkeypatch):
    def fake_get_json(session, url, params, timeout, max_retries):
        return {}  # falsy => break

    monkeypatch.setattr(extract, "get_json", fake_get_json)

    s = requests.Session()
    rows = extract.fetch_all(
        session=s,
        base_url="http://test",
        entity="ib_receipts",
        updated_after=_dt_utc(2026, 1, 1),
        limit=5,
        request_timeout_seconds=30,
    )
    assert rows == []


def test_fetch_all_raises_when_data_not_list(monkeypatch):
    def fake_get_json(session, url, params, timeout, max_retries):
        return {"data": {"oops": 1}, "meta": {}}

    monkeypatch.setattr(extract, "get_json", fake_get_json)

    s = requests.Session()
    with pytest.raises(RuntimeError, match="Unexpected API response type"):
        extract.fetch_all(
            session=s,
            base_url="http://test",
            entity="ib_receipts",
            updated_after=_dt_utc(2026, 1, 1),
            limit=5,
            request_timeout_seconds=30,
        )

