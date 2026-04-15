from __future__ import annotations

from keepr.config import DatabaseConfig
from keepr.engines.base import DatabaseEngine


class MySQLEngine(DatabaseEngine):
    name = "mysql"

    @property
    def needs_compression(self) -> bool:
        return True

    def build_dump_command(self, config: DatabaseConfig) -> str:
        parts = [
            "mysqldump",
            f"-h {config.host}",
            f"-P {config.port}",
            f"-u {config.user}",
            "--single-transaction",
            "--routines",
            "--triggers",
        ]
        if config.password:
            parts.append(f"-p'{config.password}'")
        if config.extra_args:
            parts.append(config.extra_args)
        parts.append(config.name)
        return " ".join(parts)

    def build_restore_command(self, config: DatabaseConfig, backup_path: str) -> str:
        parts = [
            "gunzip -c",
            backup_path,
            "|",
            "mysql",
            f"-h {config.host}",
            f"-P {config.port}",
            f"-u {config.user}",
        ]
        if config.password:
            parts.append(f"-p'{config.password}'")
        parts.append(config.name)
        return " ".join(parts)

    def get_file_extension(self) -> str:
        return ".sql.gz"
