import pandas as pd
import pytest
from datetime import datetime, timezone

from services.extractor.app.normalize import normalize_rows


def _utc(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc)


def test_normalize_rows_empty_returns_empty_df():
    df = normalize_rows(
        rows=[],
        entity="ib_receipts",
        run_id="run-1",
        extracted_at=_utc(2026, 1, 1),
        watermark_effective=_utc(2026, 1, 1),
    )
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_normalize_rows_requires_list():
    with pytest.raises(RuntimeError):
        normalize_rows(
            rows={"id": "1"},  # type: ignore
            entity="ib_receipts",
            run_id="run-1",
            extracted_at=_utc(2026, 1, 1),
            watermark_effective=_utc(2026, 1, 1),
        )


def test_normalize_rows_missing_required_columns_raises():
    rows = [{"id": "1"}]  # missing updated_at
    with pytest.raises(RuntimeError, match="Missing required colums"):
        normalize_rows(
            rows=rows,
            entity="ib_receipts",
            run_id="run-1",
            extracted_at=_utc(2026, 1, 1),
            watermark_effective=_utc(2026, 1, 1),
        )


def test_normalize_rows_null_required_fields_raises_with_sample():
    rows = [{"id": None, "updated_at": "2026-01-01T00:00:00Z"}]
    with pytest.raises(RuntimeError, match="Null in required fields"):
        normalize_rows(
            rows=rows,
            entity="ib_receipts",
            run_id="run-1",
            extracted_at=_utc(2026, 1, 1),
            watermark_effective=_utc(2026, 1, 1),
        )


def test_normalize_parses_time_columns_and_flattens_lines_and_adds_metadata_and_dedupes():
    rows = [
        {
            "id": 1, 
            "updated_at": "2026-01-01T00:00:02Z",
            "created_at": "2026-01-01T00:00:01Z",
            "some_date": "2026-01-02",
            "lines": [{"sku": "A", "qty": 1}],
        },
        {
            "id": 1,
            "updated_at": "2026-01-01T00:00:02Z",
            "created_at": "2026-01-01T00:00:01Z",
            "some_date": "2026-01-02",
            "lines": [{"sku": "A", "qty": 2}],
        },
        {
            "id": "2",
            "updated_at": "2026-01-01T00:00:01Z",
            "created_at": "2026-01-01T00:00:00Z",
            "some_date": "2026-01-01",
            "lines": None,
        },
    ]

    extracted_at = datetime(2026, 1, 3, 0, 0, 0)
    wm_eff = _utc(2026, 1, 1, 0, 0, 0)

    df = normalize_rows(
        rows=rows,
        entity="ib_receipts",
        run_id="run-1",
        extracted_at=extracted_at,
        watermark_effective=wm_eff,
    )
    
    assert len(df) == 2

    assert "id" in df.columns
    assert "updated_at" in df.columns

    
    assert str(df["updated_at"].dtype).startswith("datetime64[ns, UTC")
    assert str(df["created_at"].dtype).startswith("datetime64[ns, UTC")

    assert "lines" not in df.columns
    assert "lines_json" in df.columns

    
    assert str(df["id"].dtype) == "string"

    
    assert (df["_run_id"] == "run-1").all()
    assert (df["_extracted_at"].dt.tz == timezone.utc)
    assert (df["_watermark_effective"].dt.tz == timezone.utc)


