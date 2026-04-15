from __future__ import annotations

from pathlib import Path

from keepr import output
from keepr.catalog import load_catalog, save_catalog
from keepr.config import Destination, JobConfig, KeeprConfig
from keepr.executor import Executor
from keepr.storage import delete_from_s3


def apply_retention(
    cfg: KeeprConfig,
    job_name: str,
    job: JobConfig,
    dry_run: bool = False,
    quiet: bool = False,
) -> None:
    retention = cfg.get_retention(job)
    destinations = cfg.get_destinations(job)
    catalog = load_catalog()
    entries = catalog.get_by_job(job_name)  # Already sorted newest first

    if not entries:
        return

    changed = False

    # Apply retention for each destination
    if Destination.local in destinations:
        changed |= _apply_for_destination(
            cfg, entries, "local", retention.keep_local, dry_run, quiet
        )

    if Destination.s3 in destinations:
        changed |= _apply_for_destination(
            cfg, entries, "s3", retention.keep_s3, dry_run, quiet
        )

    if Destination.server in destinations:
        changed |= _apply_for_destination_server(
            cfg, job, entries, "server", retention.keep_server, dry_run, quiet
        )

    # Remove catalog entries that have no locations left
    to_remove = []
    for entry in entries:
        if not entry.locations:
            to_remove.append(entry.id)

    if to_remove:
        for bid in to_remove:
            catalog.remove(bid)
        changed = True

    if changed and not dry_run:
        save_catalog(catalog)


def _apply_for_destination(
    cfg: KeeprConfig,
    entries: list,
    dest_key: str,
    keep: int,
    dry_run: bool,
    quiet: bool,
) -> bool:
    # Filter entries that have this destination
    with_dest = [e for e in entries if dest_key in e.locations]

    if len(with_dest) <= keep:
        return False

    to_delete = with_dest[keep:]
    changed = False

    for entry in to_delete:
        path = entry.locations[dest_key]

        if dry_run:
            output.info(f"[dry-run] Would delete {dest_key}: {path} ({entry.id})")
            continue

        if dest_key == "local":
            local_path = Path(path)
            if local_path.exists():
                local_path.unlink()
        elif dest_key == "s3" and cfg.storage.s3:
            delete_from_s3(cfg.storage.s3, path)

        del entry.locations[dest_key]
        changed = True

        if not quiet:
            output.info(f"Deleted {dest_key}: {entry.id}")

    if changed and not quiet:
        output.success(
            f"Retention ({dest_key}): kept {keep}, deleted {len(to_delete)}"
        )

    return changed


def _apply_for_destination_server(
    cfg: KeeprConfig,
    job: JobConfig,
    entries: list,
    dest_key: str,
    keep: int,
    dry_run: bool,
    quiet: bool,
) -> bool:
    with_dest = [e for e in entries if dest_key in e.locations]

    if len(with_dest) <= keep:
        return False

    to_delete = with_dest[keep:]
    changed = False

    server = cfg.servers.get(job.server)
    executor = Executor(server) if server else None

    for entry in to_delete:
        path = entry.locations[dest_key]

        if dry_run:
            output.info(f"[dry-run] Would delete server: {path} ({entry.id})")
            continue

        if executor:
            try:
                executor.run_on_server(f"rm -f {path}")
            except Exception:
                output.warning(f"Could not delete server file: {path}")

        del entry.locations[dest_key]
        changed = True

        if not quiet:
            output.info(f"Deleted server: {entry.id}")

    if changed and not quiet:
        output.success(
            f"Retention (server): kept {keep}, deleted {len(to_delete)}"
        )

    return changed


def delete_backup_files(cfg: KeeprConfig, entry) -> None:
    """Delete all files for a backup entry from all locations."""
    if "local" in entry.locations:
        local_path = Path(entry.locations["local"])
        if local_path.exists():
            local_path.unlink()
            output.info(f"Deleted local: {local_path}")

    if "s3" in entry.locations and cfg.storage.s3:
        delete_from_s3(cfg.storage.s3, entry.locations["s3"])
        output.info(f"Deleted from S3: {entry.locations['s3']}")

    if "server" in entry.locations:
        # Try to find the job to get server config
        job = None
        for j_name, j in cfg.jobs.items():
            if j_name == entry.job:
                job = j
                break

        if job:
            server = cfg.servers.get(job.server)
            if server:
                executor = Executor(server)
                try:
                    executor.run_on_server(f"rm -f {entry.locations['server']}")
                    output.info(f"Deleted from server: {entry.locations['server']}")
                except Exception:
                    output.warning(
                        f"Could not delete server file: {entry.locations['server']}"
                    )
