from dataclasses import dataclass
from services.common.config import CommonConfig, load_common_config
from pathlib import Path
@dataclass(frozen=True)
class MartConfig:
    common: CommonConfig
    pipeline_name: str
    sql_ib_receipts: Path
    sql_ib_receipts_line: Path
    sql_ob_orders: Path
    sql_ob_orders_line: Path
    
def load_config_mart() -> MartConfig:
    cfg = load_common_config()
    root = Path(__file__).resolve().parents[3]
    return MartConfig(
        common=cfg,
        pipeline_name="mart",
        sql_ib_receipts=root / "sql/mart/10_ib_load.sql",
        sql_ib_receipts_line = root /"sql/mart/11_ib_load_line.sql",
        sql_ob_orders=root / "sql/mart/12_ob_load.sql",
        sql_ob_orders_line=root / "sql/mart/13_ob_load_line.sql"
    )