from __future__ import annotations

import shutil
from pathlib import Path
from typing import Annotated, Optional

import typer

from keepr import output
from keepr.config import (
    CONFIG_SEARCH_PATHS,
    Destination,
    KeeprConfig,
    load_config,
)

app = typer.Typer(
    name="keepr",
    help="Database & file backup manager with SSH and S3 support.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

ConfigOption = Annotated[
    Optional[Path],
    typer.Option("--config", "-c", help="Path to config file"),
]


def _load(config: Path | None) -> KeeprConfig:
    try:
        return load_config(config)
    except FileNotFoundError as e:
        output.error(str(e))
        raise typer.Exit(1)
    except Exception as e:
        output.error(f"Config error: {e}")
        raise typer.Exit(1)


@app.command()
def init():
    """Create an example config file."""
    output.header()

    target = Path.home() / ".config" / "keepr" / "keepr.yml"
    if target.exists():
        output.warning(f"Config already exists: {target}")
        overwrite = typer.confirm("  Overwrite?", default=False)
        if not overwrite:
            raise typer.Exit(0)

    target.parent.mkdir(parents=True, exist_ok=True)

    example = Path(__file__).parent.parent / "keepr.example.yml"
    if example.exists():
        shutil.copy(example, target)
    else:
        target.write_text(_default_config())

    output.success(f"Config created: {target}")
    output.info("Edit it to add your servers and backup jobs.")


@app.command("config")
def show_config(config: ConfigOption = None):
    """Validate and display the current configuration."""
    output.header()

    cfg = _load(config)

    output.success("Config is valid.\n")

    # Storage
    output.info(f"Local dir:  {cfg.storage.resolved_local_dir}")
    output.info(f"Server dir: {cfg.storage.server_dir}")
    if cfg.storage.s3:
        output.info(f"S3 bucket:  {cfg.storage.s3.bucket} ({cfg.storage.s3.region})")
    else:
        output.info("S3:         not configured")

    # Defaults
    r = cfg.defaults.retention
    output.info(
        f"Retention:  local={r.keep_local}, s3={r.keep_s3}, server={r.keep_server}"
    )
    dests = ", ".join(d.value for d in cfg.defaults.destinations)
    output.info(f"Defaults:   destinations=\\[{dests}]")

    # Servers
    output.console.print()
    output.info(f"Servers: {len(cfg.servers)}")
    for name, srv in cfg.servers.items():
        loc = "local" if srv.is_local else f"{srv.user}@{srv.host}:{srv.port}"
        output.info(f"  {name}: {loc}")

    # Jobs
    output.console.print()
    output.info(f"Jobs: {len(cfg.jobs)}")
    for name, job in cfg.jobs.items():
        engine = job.engine or "tar"
        dests = ", ".join(d.value for d in cfg.get_destinations(job))
        output.info(f"  {name}: {job.type} ({engine}) -> \\[{dests}]")


@app.command()
def jobs(config: ConfigOption = None):
    """List configured backup jobs."""
    output.header()

    cfg = _load(config)

    if not cfg.jobs:
        output.warning("No jobs configured.")
        raise typer.Exit(0)

    table = output.make_table("Job", "Type", "Engine", "Server", "Destinations")
    for name, job in cfg.jobs.items():
        engine = job.engine or "tar"
        dests = ", ".join(d.value for d in cfg.get_destinations(job))
        server = cfg.servers.get(job.server)
        srv_display = "localhost" if server and server.is_local else job.server
        table.add_row(name, job.type, engine, srv_display, dests)

    output.console.print(table)


@app.command()
def run(
    job_names: Annotated[
        Optional[list[str]], typer.Argument(help="Job names to run (all if omitted)")
    ] = None,
    all_jobs: Annotated[bool, typer.Option("--all", help="Run all jobs")] = False,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Show what would be done")
    ] = False,
    config: ConfigOption = None,
):
    """Run backup jobs."""
    output.header()

    cfg = _load(config)

    from keepr.backup import run_backup

    targets = _resolve_jobs(cfg, job_names, all_jobs)

    for name in targets:
        job = cfg.jobs[name]
        run_backup(cfg, name, job, dry_run=dry_run)


@app.command("list")
def list_backups(
    job_name: Annotated[
        Optional[str], typer.Argument(help="Filter by job name")
    ] = None,
    config: ConfigOption = None,
):
    """List existing backups."""
    output.header()

    from keepr.catalog import load_catalog

    cfg = _load(config)
    catalog = load_catalog()

    backups = catalog.backups
    if job_name:
        backups = [b for b in backups if b.job == job_name]

    if not backups:
        output.warning("No backups found.")
        raise typer.Exit(0)

    backups.sort(key=lambda b: b.timestamp, reverse=True)

    table = output.make_table("ID", "Job", "Engine", "Date", "Size", "Locations")
    for b in backups:
        locs = ", ".join(b.locations.keys())
        table.add_row(
            b.id,
            b.job,
            b.engine or b.type,
            b.timestamp.strftime("%Y-%m-%d %H:%M"),
            output.format_size(b.size_bytes),
            locs,
        )

    output.console.print(table)


@app.command()
def restore(
    backup_id: Annotated[str, typer.Argument(help="Backup ID to restore")],
    config: ConfigOption = None,
):
    """Restore a backup."""
    output.header()

    cfg = _load(config)

    from keepr.restore import restore_backup

    restore_backup(cfg, backup_id)


@app.command()
def delete(
    backup_id: Annotated[str, typer.Argument(help="Backup ID to delete")],
    config: ConfigOption = None,
    force: Annotated[bool, typer.Option("--force", "-f", help="Skip confirmation")] = False,
):
    """Delete a backup from all locations."""
    output.header()

    cfg = _load(config)

    from keepr.catalog import load_catalog, save_catalog
    from keepr.retention import delete_backup_files

    catalog = load_catalog()
    entry = catalog.find(backup_id)
    if not entry:
        output.error(f"Backup not found: {backup_id}")
        raise typer.Exit(1)

    if not force:
        locs = ", ".join(entry.locations.keys())
        confirm = typer.confirm(
            f"  Delete {backup_id} from [{locs}]?", default=False
        )
        if not confirm:
            raise typer.Exit(0)

    delete_backup_files(cfg, entry)
    catalog.remove(backup_id)
    save_catalog(catalog)
    output.success(f"Deleted: {backup_id}")


@app.command()
def cleanup(
    job_name: Annotated[
        Optional[str], typer.Argument(help="Job name (all if omitted)")
    ] = None,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Show what would be deleted")
    ] = False,
    config: ConfigOption = None,
):
    """Apply retention policies and delete old backups."""
    output.header()

    cfg = _load(config)

    from keepr.retention import apply_retention

    job_names = [job_name] if job_name else list(cfg.jobs.keys())
    for name in job_names:
        if name not in cfg.jobs:
            output.error(f"Job not found: {name}")
            continue
        job = cfg.jobs[name]
        apply_retention(cfg, name, job, dry_run=dry_run)


@app.command()
def cron(config: ConfigOption = None):
    """Print crontab entries for scheduling backups."""
    output.header()

    cfg = _load(config)

    keepr_bin = shutil.which("keepr") or "keepr"
    config_flag = ""

    output.console.print("  # keepr - paste into crontab (crontab -e)")
    output.console.print("  # Run all jobs daily at 3:00 AM")
    output.console.print(
        f"  0 3 * * * {keepr_bin} run --all{config_flag} >> /var/log/keepr.log 2>&1"
    )
    output.console.print()
    output.console.print("  # Or run individual jobs:")
    for i, name in enumerate(cfg.jobs):
        minute = i * 15
        output.console.print(
            f"  # {minute} 3 * * * {keepr_bin} run {name}{config_flag} "
            f">> /var/log/keepr.log 2>&1"
        )
    output.console.print()
    output.console.print(
        f"  # Cleanup old backups daily at 5:00 AM"
    )
    output.console.print(
        f"  # 0 5 * * * {keepr_bin} cleanup{config_flag} >> /var/log/keepr.log 2>&1"
    )


def _resolve_jobs(
    cfg: KeeprConfig,
    job_names: list[str] | None,
    all_jobs: bool,
) -> list[str]:
    if all_jobs or not job_names:
        if not cfg.jobs:
            output.warning("No jobs configured.")
            raise typer.Exit(0)
        return list(cfg.jobs.keys())

    for name in job_names:
        if name not in cfg.jobs:
            output.error(f"Job not found: {name}")
            raise typer.Exit(1)
    return job_names


def _default_config() -> str:
    return """\
storage:
  local_dir: ~/backups
  server_dir: /var/backups/keepr
  # s3:
  #   bucket: my-backups
  #   region: eu-central-1
  #   prefix: keepr/

defaults:
  retention:
    keep_local: 7
    keep_s3: 30
    keep_server: 5
  destinations:
    - local
    - s3

servers:
  local:
    host: localhost

  # production:
  #   host: 159.69.42.100
  #   user: root
  #   port: 22

jobs: {}
  # example-db:
  #   server: production
  #   type: database
  #   engine: postgres
  #   database:
  #     name: mydb
  #     user: postgres
  #     host: localhost
  #     port: 5432
  #   destinations: [local, s3]
"""
