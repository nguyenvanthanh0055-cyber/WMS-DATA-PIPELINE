from dataclasses import dataclass
from services.common.config import load_common_config, CommonConfig


@dataclass(frozen=True)
class ExtractConfig:
    common: CommonConfig
    pipeline_name: str
    
def load_extract_config() -> ExtractConfig:
    cfg = load_common_config()
    return ExtractConfig(
        common=cfg,
        pipeline_name="extract"
    )