from __future__ import annotations

from keepr.config import DatabaseConfig
from keepr.engines.base import DatabaseEngine


class SQLiteEngine(DatabaseEngine):
    name = "sqlite"

    @property
    def needs_compression(self) -> bool:
        return True

    def build_dump_command(self, config: DatabaseConfig) -> str:
        if not config.path:
            raise ValueError("SQLite engine requires 'path' in database config")
        # Use sqlite3 .dump for a consistent SQL dump, then compress
        return f"sqlite3 {config.path} .dump"

    def build_restore_command(self, config: DatabaseConfig, backup_path: str) -> str:
        if not config.path:
            raise ValueError("SQLite engine requires 'path' in database config")
        return f"gunzip -c {backup_path} | sqlite3 {config.path}"

    def get_file_extension(self) -> str:
        return ".sql.gz"
