from pathlib import Path
import pandas as pd
import pytest

from services.extractor.app.writer_landing import write_landing


def test_write_landing_rejects_unknown_format(tmp_path: Path):
    df = pd.DataFrame([{"id": "1"}])
    with pytest.raises(ValueError, match="Unsupported output_format"):
        write_landing(df, landing_root=tmp_path, entity="ib_receipts", run_id="run-1", output_format="json")


def test_write_landing_creates_run_dir_and_writes_csv(tmp_path: Path):
    df = pd.DataFrame([{"id": "1", "updated_at": "2026-01-01T00:00:00Z"}])

    out = write_landing(
        df=df,
        landing_root=tmp_path,
        entity="ib_receipts",
        run_id="run-1",
        output_format="csv",
    )

    assert out == tmp_path / "ib_receipts" / "run_id=run-1" / "part-000.csv"
    assert out.exists()

    df2 = pd.read_csv(out)
    assert len(df2) == 1
    assert str(df2.loc[0, "id"]) == "1"



def test_write_landing_fails_if_output_already_exists(tmp_path: Path):
    df = pd.DataFrame([{"id": "1"}])

    out1 = write_landing(df, tmp_path, "ib_receipts", "run-1", output_format="csv")
    assert out1.exists()

    with pytest.raises(RuntimeError, match="Landing output already exists"):
        write_landing(df, tmp_path, "ib_receipts", "run-1", output_format="csv")
