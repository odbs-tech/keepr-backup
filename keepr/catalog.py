from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel


CATALOG_PATH = Path.home() / ".config" / "keepr" / "catalog.json"


class BackupEntry(BaseModel):
    id: str
    job: str
    type: str  # "database" or "files"
    engine: str | None = None
    server: str
    timestamp: datetime
    filename: str
    size_bytes: int = 0
    checksum_sha256: str | None = None
    locations: dict[str, str] = {}  # destination -> path/key
    status: str = "completed"
    duration_seconds: float = 0.0


class Catalog(BaseModel):
    backups: list[BackupEntry] = []

    def add(self, entry: BackupEntry) -> None:
        self.backups.append(entry)

    def find(self, backup_id: str) -> BackupEntry | None:
        for b in self.backups:
            if b.id == backup_id:
                return b
        return None

    def remove(self, backup_id: str) -> None:
        self.backups = [b for b in self.backups if b.id != backup_id]

    def get_by_job(self, job_name: str) -> list[BackupEntry]:
        entries = [b for b in self.backups if b.job == job_name]
        entries.sort(key=lambda b: b.timestamp, reverse=True)
        return entries


def load_catalog() -> Catalog:
    if not CATALOG_PATH.exists():
        return Catalog()
    data = json.loads(CATALOG_PATH.read_text())
    return Catalog(**data)


def save_catalog(catalog: Catalog) -> None:
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CATALOG_PATH.with_suffix(".tmp")
    tmp.write_text(catalog.model_dump_json(indent=2))
    tmp.rename(CATALOG_PATH)


def generate_backup_id(job_name: str, timestamp: datetime | None = None) -> str:
    ts = timestamp or datetime.now(timezone.utc)
    base = f"{job_name}_{ts.strftime('%Y%m%d_%H%M%S')}"
    # Ensure uniqueness by checking existing catalog
    catalog = load_catalog()
    if not catalog.find(base):
        return base
    # Add counter suffix for same-second backups
    for i in range(1, 100):
        candidate = f"{base}_{i}"
        if not catalog.find(candidate):
            return candidate
    return base


def generate_filename(
    job_name: str,
    engine: str | None,
    backup_type: str,
    backup_id: str,
    extension: str = "",
) -> str:
    label = engine or backup_type
    return f"{backup_id}_{label}{extension}"
