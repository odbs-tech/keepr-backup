from __future__ import annotations

from keepr.config import DatabaseConfig
from keepr.engines.base import DatabaseEngine


class MySQLEngine(DatabaseEngine):
    name = "mysql"

    def needs_compression_for(self, config: DatabaseConfig) -> bool:
        return True

    def build_dump_command(self, config: DatabaseConfig) -> str:
        binary = config.dump_path or "mysqldump"
        parts = [
            binary,
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
            f"gunzip -c {backup_path} |",
            "mysql",
            f"-h {config.host}",
            f"-P {config.port}",
            f"-u {config.user}",
        ]
        if config.password:
            parts.append(f"-p'{config.password}'")
        parts.append(config.name)
        return " ".join(parts)

    def get_file_extension(self, config: DatabaseConfig) -> str:
        return ".sql.gz"
