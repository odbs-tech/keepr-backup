from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path

from keepr import output
from keepr.catalog import (
    BackupEntry,
    generate_backup_id,
    generate_filename,
    load_catalog,
    save_catalog,
)
from keepr.config import Destination, JobConfig, KeeprConfig
from keepr.engines import get_engine
from keepr.executor import Executor
from keepr.storage import upload_to_s3


def run_backup(
    cfg: KeeprConfig,
    job_name: str,
    job: JobConfig,
    dry_run: bool = False,
) -> None:
    server = cfg.get_server(job.server)
    destinations = cfg.get_destinations(job)
    executor = Executor(server)

    output.info(f"Running job: [bold]{job_name}[/bold]")

    srv_display = "localhost" if server.is_local else f"{server.user}@{server.host}"
    output.info(f"Server: {job.server} ({srv_display})")

    if job.type == "database":
        _run_database_backup(cfg, job_name, job, executor, destinations, dry_run)
    elif job.type == "files":
        _run_files_backup(cfg, job_name, job, executor, destinations, dry_run)

    output.console.print()


def _run_database_backup(
    cfg: KeeprConfig,
    job_name: str,
    job: JobConfig,
    executor: Executor,
    destinations: list[Destination],
    dry_run: bool,
) -> None:
    engine = get_engine(job.engine)
    db_config = job.database

    output.info(f"Engine: {engine.name} — database: {db_config.name or db_config.path}")

    dump_cmd = engine.build_dump_command(db_config)
    env = engine.get_env(db_config)

    if engine.needs_compression:
        dump_cmd = f"{dump_cmd} | gzip"

    if dry_run:
        output.info(f"[dry-run] Would execute: {dump_cmd}")
        output.info(f"[dry-run] Destinations: {', '.join(d.value for d in destinations)}")
        return

    now = datetime.now(timezone.utc)
    backup_id = generate_backup_id(job_name, now)
    filename = generate_filename(job_name, engine.name, "database", backup_id, engine.get_file_extension())
    start = time.time()

    locations: dict[str, str] = {}
    local_path: Path | None = None

    # Determine if we need a local copy (for local destination or S3 upload)
    needs_local = Destination.local in destinations or Destination.s3 in destinations

    if needs_local:
        local_dir = cfg.storage.resolved_local_dir / job_name
        local_path = local_dir / filename

        output.info("Dumping...")
        executor.run_stream_to_file(dump_cmd, local_path, env=env)
        size = local_path.stat().st_size
        output.success(f"Saved: {local_path} ({output.format_size(size)})")

        if Destination.local in destinations:
            locations["local"] = str(local_path)

    # Server destination: keep dump on the remote server
    if Destination.server in destinations:
        server_path = f"{cfg.storage.server_dir}/{job_name}/{filename}"
        server_dir = f"{cfg.storage.server_dir}/{job_name}"

        output.info("Dumping to server...")
        executor.run_on_server(f"mkdir -p {server_dir}")
        executor.run_on_server(f"{dump_cmd} > {server_path}", env=env)
        output.success(f"Saved on server: {server_path}")
        locations["server"] = server_path

    # S3 upload
    if Destination.s3 in destinations and cfg.storage.s3:
        s3_key = f"{job_name}/{filename}"
        if local_path and local_path.exists():
            upload_to_s3(cfg.storage.s3, local_path, s3_key)
        elif Destination.server in destinations:
            # Download from server first, then upload to S3
            tmp_path = cfg.storage.resolved_local_dir / ".tmp" / filename
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            server_path = locations.get("server", "")
            executor.download(server_path, tmp_path)
            upload_to_s3(cfg.storage.s3, tmp_path, s3_key)
            tmp_path.unlink(missing_ok=True)
        locations["s3"] = s3_key

    duration = time.time() - start

    # Calculate size and checksum
    size_bytes = 0
    checksum = None
    if local_path and local_path.exists():
        size_bytes = local_path.stat().st_size
        checksum = _sha256(local_path)

    # Save to catalog
    entry = BackupEntry(
        id=backup_id,
        job=job_name,
        type="database",
        engine=engine.name,
        server=job.server,
        timestamp=now,
        filename=filename,
        size_bytes=size_bytes,
        checksum_sha256=checksum,
        locations=locations,
        duration_seconds=round(duration, 1),
    )
    catalog = load_catalog()
    catalog.add(entry)
    save_catalog(catalog)

    output.success(f"Done in {duration:.1f}s")

    # Auto cleanup
    from keepr.retention import apply_retention
    apply_retention(cfg, job_name, job, dry_run=False, quiet=True)


def _run_files_backup(
    cfg: KeeprConfig,
    job_name: str,
    job: JobConfig,
    executor: Executor,
    destinations: list[Destination],
    dry_run: bool,
) -> None:
    files_config = job.files

    paths_str = " ".join(files_config.paths)
    excludes = " ".join(f"--exclude='{e}'" for e in files_config.exclude)
    tar_cmd = f"tar -czf - {excludes} {paths_str}"

    output.info(f"Files: {', '.join(files_config.paths)}")

    if dry_run:
        output.info(f"[dry-run] Would execute: {tar_cmd}")
        output.info(f"[dry-run] Destinations: {', '.join(d.value for d in destinations)}")
        return

    now = datetime.now(timezone.utc)
    backup_id = generate_backup_id(job_name, now)
    filename = generate_filename(job_name, None, "files", backup_id, ".tar.gz")
    start = time.time()

    locations: dict[str, str] = {}
    local_path: Path | None = None

    needs_local = Destination.local in destinations or Destination.s3 in destinations

    if needs_local:
        local_dir = cfg.storage.resolved_local_dir / job_name
        local_path = local_dir / filename

        output.info("Archiving...")
        executor.run_stream_to_file(tar_cmd, local_path)
        size = local_path.stat().st_size
        output.success(f"Saved: {local_path} ({output.format_size(size)})")

        if Destination.local in destinations:
            locations["local"] = str(local_path)

    if Destination.server in destinations:
        server_path = f"{cfg.storage.server_dir}/{job_name}/{filename}"
        server_dir = f"{cfg.storage.server_dir}/{job_name}"

        output.info("Archiving to server...")
        executor.run_on_server(f"mkdir -p {server_dir}")
        executor.run_on_server(f"{tar_cmd} > {server_path}")
        output.success(f"Saved on server: {server_path}")
        locations["server"] = server_path

    if Destination.s3 in destinations and cfg.storage.s3:
        s3_key = f"{job_name}/{filename}"
        if local_path and local_path.exists():
            upload_to_s3(cfg.storage.s3, local_path, s3_key)
        elif Destination.server in destinations:
            tmp_path = cfg.storage.resolved_local_dir / ".tmp" / filename
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            server_path = locations.get("server", "")
            executor.download(server_path, tmp_path)
            upload_to_s3(cfg.storage.s3, tmp_path, s3_key)
            tmp_path.unlink(missing_ok=True)
        locations["s3"] = s3_key

    duration = time.time() - start

    size_bytes = 0
    checksum = None
    if local_path and local_path.exists():
        size_bytes = local_path.stat().st_size
        checksum = _sha256(local_path)

    entry = BackupEntry(
        id=backup_id,
        job=job_name,
        type="files",
        engine=None,
        server=job.server,
        timestamp=now,
        filename=filename,
        size_bytes=size_bytes,
        checksum_sha256=checksum,
        locations=locations,
        duration_seconds=round(duration, 1),
    )
    catalog = load_catalog()
    catalog.add(entry)
    save_catalog(catalog)

    output.success(f"Done in {duration:.1f}s")

    from keepr.retention import apply_retention
    apply_retention(cfg, job_name, job, dry_run=False, quiet=True)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
