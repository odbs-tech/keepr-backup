from __future__ import annotations

from abc import ABC, abstractmethod

from keepr.config import DatabaseConfig


class DatabaseEngine(ABC):
    @abstractmethod
    def build_dump_command(self, config: DatabaseConfig) -> str:
        """Build the shell command to dump the database to stdout."""

    @abstractmethod
    def build_restore_command(self, config: DatabaseConfig, backup_path: str) -> str:
        """Build the shell command to restore from a backup file."""

    def get_file_extension(self, config: DatabaseConfig) -> str:
        """Return the file extension for this engine's dumps."""
        return ".sql.gz"

    @property
    @abstractmethod
    def name(self) -> str:
        """Engine name for display."""

    def get_env(self, config: DatabaseConfig) -> dict[str, str]:
        """Return environment variables needed for the dump/restore command."""
        return {}

    def needs_compression_for(self, config: DatabaseConfig) -> bool:
        """Whether the dump output needs external gzip compression."""
        return False
