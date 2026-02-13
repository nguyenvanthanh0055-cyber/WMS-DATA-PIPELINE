from dataclasses import dataclass
from services.common.config import load_common_config, CommonConfig

@dataclass(frozen=True)
class StagingConfig:
    common: CommonConfig
    pipeline_name: str
    
def load_staging_config() -> StagingConfig:
    cfg = load_common_config()
    return StagingConfig(
        common=cfg,
        pipeline_name="staging"
    )