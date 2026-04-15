from __future__ import annotations

from keepr.config import DatabaseConfig
from keepr.engines.base import DatabaseEngine


class PostgresEngine(DatabaseEngine):
    name = "postgres"

    def build_dump_command(self, config: DatabaseConfig) -> str:
        binary = config.dump_path or "pg_dump"
        parts = [binary, f"-h {config.host}", f"-p {config.port}", f"-U {config.user}"]

        if config.format == "sql":
            parts.append("-Fp")  # Plain SQL format
        else:
            parts.append("-Fc")  # Custom format (compressed)

        if config.extra_args:
            parts.append(config.extra_args)
        parts.append(config.name)
        return " ".join(parts)

    def build_restore_command(self, config: DatabaseConfig, backup_path: str) -> str:
        if config.format == "sql":
            # SQL format needs gunzip + psql
            psql = "psql"
            parts = [
                f"gunzip -c {backup_path} |",
                psql,
                f"-h {config.host}",
                f"-p {config.port}",
                f"-U {config.user}",
                f"-d {config.name}",
            ]
        else:
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

    def get_file_extension(self, config: DatabaseConfig) -> str:
        if config.format == "sql":
            return ".sql.gz"
        return ".dump"

    @property
    def needs_compression(self, config: DatabaseConfig | None = None) -> bool:
        if config and config.format == "sql":
            return True
        return False

    def needs_compression_for(self, config: DatabaseConfig) -> bool:
        return config.format == "sql"

    def get_env(self, config: DatabaseConfig) -> dict[str, str]:
        env = {}
        if config.password:
            env["PGPASSWORD"] = config.password
        return env
