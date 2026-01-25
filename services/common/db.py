from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def build_engine(pg_dsn: str) -> Engine:
    return create_engine(pg_dsn, pool_pre_ping= True)
