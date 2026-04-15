from __future__ import annotations

from pathlib import Path

import typer

from keepr import output
from keepr.catalog import load_catalog
from keepr.config import KeeprConfig
from keepr.engines import get_engine
from keepr.executor import Executor
from keepr.storage import download_from_s3


def restore_backup(cfg: KeeprConfig, backup_id: str) -> None:
    catalog = load_catalog()
    entry = catalog.find(backup_id)

    if not entry:
        output.error(f"Backup not found: {backup_id}")
        raise typer.Exit(1)

    output.info(f"Restoring: [bold]{backup_id}[/bold]")
    output.info(f"Job: {entry.job} | Type: {entry.type} | Engine: {entry.engine or 'files'}")
    output.info(f"Date: {entry.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")

    # Find the backup file
    local_path = _resolve_backup_file(cfg, entry)

    if not local_path or not local_path.exists():
        output.error("Could not locate backup file.")
        raise typer.Exit(1)

    output.info(f"Backup file: {local_path}")

    # Confirm
    if not typer.confirm("  Proceed with restore?", default=False):
        raise typer.Exit(0)

    # Get the server config for the job
    job = cfg.jobs.get(entry.job)
    if not job:
        output.error(f"Job '{entry.job}' not found in config. Cannot determine restore target.")
        raise typer.Exit(1)

    server = cfg.get_server(job.server)
    executor = Executor(server)

    if entry.type == "database":
        _restore_database(cfg, entry, job, executor, local_path)
    elif entry.type == "files":
        _restore_files(cfg, entry, job, executor, local_path)

    output.success("Restore complete.")


def _resolve_backup_file(cfg, entry) -> Path | None:
    """Find the backup file, downloading from S3 if necessary."""
    # Try local first
    if "local" in entry.locations:
        local_path = Path(entry.locations["local"])
        if local_path.exists():
            return local_path

    # Try downloading from S3
    if "s3" in entry.locations and cfg.storage.s3:
        local_path = cfg.storage.resolved_local_dir / entry.job / entry.filename
        download_from_s3(cfg.storage.s3, entry.locations["s3"], local_path)
        return local_path

    # Try downloading from server
    if "server" in entry.locations:
        job = cfg.jobs.get(entry.job)
        if job:
            server = cfg.get_server(job.server)
            executor = Executor(server)
            local_path = cfg.storage.resolved_local_dir / entry.job / entry.filename
            executor.download(entry.locations["server"], local_path)
            return local_path

    return None


def _restore_database(cfg, entry, job, executor, local_path: Path) -> None:
    engine = get_engine(entry.engine)
    db_config = job.database

    output.info(f"Restoring database: {db_config.name or db_config.path}")

    restore_cmd = engine.build_restore_command(db_config, str(local_path))
    env = engine.get_env(db_config)

    if executor.server.is_local:
        output.info("Running restore locally...")
        executor.run(restore_cmd, env=env)
    else:
        # Upload backup to server, then restore
        remote_tmp = f"/tmp/{entry.filename}"
        output.info("Uploading backup to server...")
        # Use SCP in reverse
        _upload_to_server(executor, local_path, remote_tmp)

        restore_cmd = engine.build_restore_command(db_config, remote_tmp)
        output.info("Running restore on server...")
        executor.run_on_server(restore_cmd, env=env)

        # Clean up temp file
        executor.run_on_server(f"rm -f {remote_tmp}")


def _restore_files(cfg, entry, job, executor, local_path: Path) -> None:
    output.info("Extracting files...")

    if executor.server.is_local:
        executor.run(f"tar -xzf {local_path} -C /")
    else:
        remote_tmp = f"/tmp/{entry.filename}"
        output.info("Uploading archive to server...")
        _upload_to_server(executor, local_path, remote_tmp)

        output.info("Extracting on server...")
        executor.run_on_server(f"tar -xzf {remote_tmp} -C /")
        executor.run_on_server(f"rm -f {remote_tmp}")


def _upload_to_server(executor: Executor, local_path: Path, remote_path: str) -> None:
    """Upload a file to the remote server via SCP."""
    import subprocess

    server = executor.server
    scp_cmd = ["scp", "-P", str(server.port)]
    if server.ssh_key:
        scp_cmd += ["-i", server.ssh_key]
    scp_cmd += [str(local_path), f"{server.user}@{server.host}:{remote_path}"]
    subprocess.run(scp_cmd, check=True, capture_output=True)
