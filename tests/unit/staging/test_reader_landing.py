# tests/unit/staging/test_landing_reader.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
import pytest

from services.staging.app.reader_landing import reader_landing


def _write_landing_csv(base: Path, rows: list[dict]) -> None:
    base.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(base / "part-000.csv", index=False)


def test_read_landing_csv_ok(tmp_path: Path) -> None:
    entity = "ib_receipts"
    run_id = "run123"
    landing_root = tmp_path / "data" / "landing"
    batch_dir = landing_root / entity / f"run_id={run_id}"

    _write_landing_csv(batch_dir, rows=[
        {
            "id": 1,
            "updated_at": "2026-01-23T10:00:00Z",
            "_run_id": run_id,
            "_extracted_at": "2026-01-23T10:01:00Z",
            "_watermark_effective": "2026-01-23T09:58:00Z",
            "status": "done",
        }
    ])

    df = reader_landing(landing_root, entity, run_id)

    assert len(df) == 1
    assert "id" in df.columns
    assert isinstance(df["updated_at"].dtype, pd.DatetimeTZDtype)


def test_read_landing_missing_file(tmp_path: Path) -> None:
    landing_root = tmp_path / "data" / "landing"
    with pytest.raises(FileNotFoundError):
        reader_landing(landing_root, "ib_receipts", "abc")


def test_read_landing_missing_required_columns(tmp_path: Path) -> None:
    entity = "ib_receipts"
    run_id = "run_missing_cols"
    landing_root = tmp_path / "data" / "landing"
    batch_dir = landing_root / entity / f"run_id={run_id}"

    _write_landing_csv(batch_dir, rows=[{"id": 1, "_run_id": run_id}])

    with pytest.raises(ValueError) as e:
        reader_landing(landing_root, entity, run_id)

    assert "missing columns" in str(e.value).lower()

