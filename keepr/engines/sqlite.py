from __future__ import annotations

from keepr.config import DatabaseConfig
from keepr.engines.base import DatabaseEngine


class SQLiteEngine(DatabaseEngine):
    name = "sqlite"

    def needs_compression_for(self, config: DatabaseConfig) -> bool:
        return True

    def build_dump_command(self, config: DatabaseConfig) -> str:
        if not config.path:
            raise ValueError("SQLite engine requires 'path' in database config")
        binary = config.dump_path or "sqlite3"
        return f"{binary} {config.path} .dump"

    def build_restore_command(self, config: DatabaseConfig, backup_path: str) -> str:
        if not config.path:
            raise ValueError("SQLite engine requires 'path' in database config")
        return f"gunzip -c {backup_path} | sqlite3 {config.path}"

    def get_file_extension(self, config: DatabaseConfig) -> str:
        return ".sql.gz"
