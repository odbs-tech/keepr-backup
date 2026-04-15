from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, model_validator


CONFIG_SEARCH_PATHS = [
    Path("keepr.yml"),
    Path("keepr.yaml"),
    Path.home() / ".config" / "keepr" / "keepr.yml",
    Path.home() / ".config" / "keepr" / "keepr.yaml",
]


class Destination(str, Enum):
    local = "local"
    server = "server"
    s3 = "s3"


class S3Config(BaseModel):
    bucket: str
    region: str = "eu-central-1"
    prefix: str = ""
    access_key_id: str | None = None
    secret_access_key: str | None = None
    endpoint_url: str | None = None


class StorageConfig(BaseModel):
    local_dir: str = "~/backups"
    server_dir: str = "/var/backups/keepr"
    s3: S3Config | None = None

    @property
    def resolved_local_dir(self) -> Path:
        return Path(os.path.expanduser(self.local_dir))


class RetentionConfig(BaseModel):
    keep_local: int = 7
    keep_s3: int = 30
    keep_server: int = 5


class DefaultsConfig(BaseModel):
    retention: RetentionConfig = RetentionConfig()
    destinations: list[Destination] = [Destination.local, Destination.s3]


class ServerConfig(BaseModel):
    host: str
    user: str = "root"
    port: int = 22
    ssh_key: str | None = None

    @property
    def is_local(self) -> bool:
        return self.host in ("localhost", "127.0.0.1")


class DatabaseConfig(BaseModel):
    name: str | None = None
    path: str | None = None  # SQLite only
    user: str = "postgres"
    password: str | None = None
    host: str = "localhost"
    port: int = 5432
    extra_args: str | None = None


class FilesConfig(BaseModel):
    paths: list[str]
    exclude: list[str] = []


class JobConfig(BaseModel):
    server: str
    engine: str | None = None  # postgres | mysql | sqlite
    database: DatabaseConfig | None = None
    files: FilesConfig | None = None
    connection: str | None = None  # "direct" | "ssh" — for display only
    destinations: list[Destination] | None = None
    retention: RetentionConfig | None = None

    @model_validator(mode="after")
    def validate_job(self):
        if not self.database and not self.files:
            raise ValueError("At least one of 'database' or 'files' is required")
        if self.database and not self.engine:
            raise ValueError("'engine' is required when 'database' is set")
        return self

    @property
    def has_database(self) -> bool:
        return self.database is not None

    @property
    def has_files(self) -> bool:
        return self.files is not None

    @property
    def type_label(self) -> str:
        if self.has_database and self.has_files:
            return "database + files"
        if self.has_database:
            return "database"
        return "files"


class KeeprConfig(BaseModel):
    storage: StorageConfig = StorageConfig()
    defaults: DefaultsConfig = DefaultsConfig()
    servers: dict[str, ServerConfig] = {}
    jobs: dict[str, JobConfig] = {}

    def get_server(self, name: str) -> ServerConfig:
        if name not in self.servers:
            raise ValueError(f"Server '{name}' not found. Available: {', '.join(self.servers)}")
        return self.servers[name]

    def get_destinations(self, job: JobConfig) -> list[Destination]:
        return job.destinations or self.defaults.destinations

    def get_retention(self, job: JobConfig) -> RetentionConfig:
        if job.retention:
            base = self.defaults.retention.model_dump()
            override = job.retention.model_dump(exclude_unset=True)
            base.update(override)
            return RetentionConfig(**base)
        return self.defaults.retention


def find_config_file(config_path: Path | None = None) -> Path:
    if config_path:
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        return config_path

    for path in CONFIG_SEARCH_PATHS:
        if path.exists():
            return path

    raise FileNotFoundError(
        "No config file found. Run 'keepr init' to create one, "
        "or specify with --config."
    )


def load_config(config_path: Path | None = None) -> KeeprConfig:
    path = find_config_file(config_path)
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return KeeprConfig(**data)


def load_config_raw(config_path: Path | None = None) -> tuple[dict, Path]:
    """Load config as raw dict + return the file path for saving back."""
    path = find_config_file(config_path)
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return data, path


def save_config_raw(data: dict, path: Path) -> None:
    """Save raw config dict back to YAML."""
    # Validate before saving
    KeeprConfig(**data)
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
