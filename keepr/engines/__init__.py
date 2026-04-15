from keepr.engines.base import DatabaseEngine
from keepr.engines.mysql import MySQLEngine
from keepr.engines.postgres import PostgresEngine
from keepr.engines.sqlite import SQLiteEngine

ENGINES: dict[str, type[DatabaseEngine]] = {
    "postgres": PostgresEngine,
    "mysql": MySQLEngine,
    "sqlite": SQLiteEngine,
}


def get_engine(name: str) -> DatabaseEngine:
    cls = ENGINES.get(name)
    if cls is None:
        raise ValueError(f"Unknown engine: {name}. Available: {', '.join(ENGINES)}")
    return cls()
