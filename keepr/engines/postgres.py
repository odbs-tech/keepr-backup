from __future__ import annotations

from keepr.config import DatabaseConfig
from keepr.engines.base import DatabaseEngine


class PostgresEngine(DatabaseEngine):
    name = "postgres"

    def build_dump_command(self, config: DatabaseConfig) -> str:
        parts = [
            "pg_dump",
            f"-h {config.host}",
            f"-p {config.port}",
            f"-U {config.user}",
            "-Fc",  # Custom format (compressed, supports parallel restore)
        ]
        if config.extra_args:
            parts.append(config.extra_args)
        parts.append(config.name)
        return " ".join(parts)

    def build_restore_command(self, config: DatabaseConfig, backup_path: str) -> str:
        parts = [
            "pg_restore",
            f"-h {config.host}",
            f"-p {config.port}",
            f"-U {config.user}",
            f"-d {config.name}",
            "--no-owner",
            "--clean",
            "--if-exists",
            backup_path,
        ]
        return " ".join(parts)

    def get_file_extension(self) -> str:
        return ".dump"

    def get_env(self, config: DatabaseConfig) -> dict[str, str]:
        env = {}
        if config.password:
            env["PGPASSWORD"] = config.password
        return env
