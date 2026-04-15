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
    load_config_raw,
    save_config_raw,
)

app = typer.Typer(
    name="keepr",
    help="Database & file backup manager with SSH and S3 support.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

# ── Sub-apps ──────────────────────────────────────────
server_app = typer.Typer(help="Manage servers.", no_args_is_help=True)
job_app = typer.Typer(help="Manage backup jobs.", no_args_is_help=True)
app.add_typer(server_app, name="server")
app.add_typer(job_app, name="job")

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


def _load_raw(config: Path | None) -> tuple[dict, Path]:
    try:
        return load_config_raw(config)
    except FileNotFoundError as e:
        output.error(str(e))
        raise typer.Exit(1)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Server commands
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@server_app.command("add")
def server_add(
    name: Annotated[str, typer.Argument(help="Server name")],
    host: Annotated[str, typer.Option("--host", "-h", help="Hostname or IP")] = "",
    user: Annotated[str, typer.Option("--user", "-u", help="SSH user")] = "root",
    port: Annotated[int, typer.Option("--port", "-p", help="SSH port")] = 22,
    ssh_key: Annotated[Optional[str], typer.Option("--ssh-key", help="SSH key path")] = None,
    config: ConfigOption = None,
):
    """Add a new server."""
    output.header()

    data, path = _load_raw(config)
    data.setdefault("servers", {})

    if name in data["servers"]:
        output.error(f"Server '{name}' already exists. Use 'keepr server remove {name}' first.")
        raise typer.Exit(1)

    # Interactive if --host not provided
    if not host:
        host = typer.prompt("  Host (hostname or IP)")

    if host in ("localhost", "127.0.0.1"):
        server_data = {"host": host}
    else:
        user = typer.prompt("  SSH user", default=user)
        port = int(typer.prompt("  SSH port", default=str(port)))
        ssh_key_input = typer.prompt("  SSH key path (optional)", default=ssh_key or "")
        server_data = {"host": host, "user": user, "port": port}
        if ssh_key_input:
            server_data["ssh_key"] = ssh_key_input

    data["servers"][name] = server_data
    save_config_raw(data, path)

    display = "localhost" if host in ("localhost", "127.0.0.1") else f"{user}@{host}:{port}"
    output.success(f"Server added: {name} ({display})")


@server_app.command("remove")
def server_remove(
    name: Annotated[str, typer.Argument(help="Server name to remove")],
    force: Annotated[bool, typer.Option("--force", "-f", help="Skip confirmation")] = False,
    config: ConfigOption = None,
):
    """Remove a server."""
    output.header()

    data, path = _load_raw(config)
    servers = data.get("servers", {})

    if name not in servers:
        output.error(f"Server '{name}' not found.")
        raise typer.Exit(1)

    # Check if any jobs reference this server
    jobs = data.get("jobs", {})
    using_jobs = [j for j, jcfg in jobs.items() if jcfg.get("server") == name]
    if using_jobs:
        output.warning(f"Jobs using this server: {', '.join(using_jobs)}")

    if not force:
        if not typer.confirm(f"  Remove server '{name}'?", default=False):
            raise typer.Exit(0)

    del data["servers"][name]
    save_config_raw(data, path)
    output.success(f"Server removed: {name}")


@server_app.command("list")
def server_list(config: ConfigOption = None):
    """List configured servers."""
    output.header()

    cfg = _load(config)

    if not cfg.servers:
        output.warning("No servers configured.")
        raise typer.Exit(0)

    table = output.make_table("Name", "Host", "User", "Port", "SSH Key")
    for name, srv in cfg.servers.items():
        table.add_row(
            name,
            srv.host,
            srv.user if not srv.is_local else "-",
            str(srv.port) if not srv.is_local else "-",
            srv.ssh_key or "-",
        )

    output.console.print(table)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Job commands
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ENGINES = ["postgres", "mysql", "sqlite"]
TYPES = ["database", "files"]
DEST_OPTIONS = ["local", "server", "s3"]


@job_app.command("add")
def job_add(
    name: Annotated[str, typer.Argument(help="Job name")],
    config: ConfigOption = None,
):
    """Add a new backup job (interactive)."""
    output.header()

    data, path = _load_raw(config)
    cfg = KeeprConfig(**data)

    data.setdefault("jobs", {})
    if name in data["jobs"]:
        output.error(f"Job '{name}' already exists. Use 'keepr job remove {name}' first.")
        raise typer.Exit(1)

    if not cfg.servers:
        output.error("No servers configured. Add one first: keepr server add <name>")
        raise typer.Exit(1)

    server_names = list(cfg.servers.keys())
    output.info(f"Available servers: {', '.join(server_names)}")

    # Server
    server = typer.prompt("  Server", default=server_names[0])
    if server not in cfg.servers:
        output.error(f"Server '{server}' not found.")
        raise typer.Exit(1)

    # Type
    job_type = typer.prompt("  Type (database/files)", default="database")
    if job_type not in TYPES:
        output.error(f"Invalid type. Choose: {', '.join(TYPES)}")
        raise typer.Exit(1)

    job_data: dict = {"server": server, "type": job_type}

    if job_type == "database":
        _prompt_database_job(job_data, cfg)
    else:
        _prompt_files_job(job_data)

    # Destinations
    default_dests = ", ".join(d.value for d in cfg.defaults.destinations)
    dests_input = typer.prompt(
        f"  Destinations ({', '.join(DEST_OPTIONS)})",
        default=default_dests,
    )
    dests = [d.strip() for d in dests_input.split(",")]
    for d in dests:
        if d not in DEST_OPTIONS:
            output.error(f"Invalid destination: {d}. Choose from: {', '.join(DEST_OPTIONS)}")
            raise typer.Exit(1)
    job_data["destinations"] = dests

    # Retention override
    if typer.confirm("  Custom retention?", default=False):
        retention = {}
        for dest in dests:
            key = f"keep_{dest}"
            default = getattr(cfg.defaults.retention, key, 7)
            val = typer.prompt(f"    {key}", default=str(default))
            retention[key] = int(val)
        job_data["retention"] = retention

    data["jobs"][name] = job_data
    save_config_raw(data, path)
    output.success(f"Job added: {name}")


def _prompt_database_job(job_data: dict, cfg: KeeprConfig) -> None:
    """Prompt for database job fields."""
    engine = typer.prompt(f"  Engine ({'/'.join(ENGINES)})", default="postgres")
    if engine not in ENGINES:
        output.error(f"Invalid engine. Choose: {', '.join(ENGINES)}")
        raise typer.Exit(1)

    job_data["engine"] = engine

    db: dict = {}

    if engine == "sqlite":
        db["path"] = typer.prompt("  Database file path")
    else:
        db["name"] = typer.prompt("  Database name")
        default_user = "postgres" if engine == "postgres" else "root"
        db["user"] = typer.prompt("  Database user", default=default_user)

        password = typer.prompt("  Database password (optional, leave empty to skip)", default="", show_default=False)
        if password:
            db["password"] = password

        db["host"] = typer.prompt("  Database host", default="localhost")
        default_port = "5432" if engine == "postgres" else "3306"
        db["port"] = int(typer.prompt("  Database port", default=default_port))

        extra = typer.prompt("  Extra dump args (optional)", default="")
        if extra:
            db["extra_args"] = extra

    job_data["database"] = db


def _prompt_files_job(job_data: dict) -> None:
    """Prompt for files job fields."""
    paths_input = typer.prompt("  Paths to backup (comma-separated)")
    paths = [p.strip() for p in paths_input.split(",")]

    exclude_input = typer.prompt("  Exclude patterns (comma-separated, optional)", default="")
    exclude = [e.strip() for e in exclude_input.split(",") if e.strip()]

    files_data: dict = {"paths": paths}
    if exclude:
        files_data["exclude"] = exclude

    job_data["files"] = files_data


@job_app.command("remove")
def job_remove(
    name: Annotated[str, typer.Argument(help="Job name to remove")],
    force: Annotated[bool, typer.Option("--force", "-f", help="Skip confirmation")] = False,
    config: ConfigOption = None,
):
    """Remove a backup job."""
    output.header()

    data, path = _load_raw(config)
    jobs = data.get("jobs", {})

    if name not in jobs:
        output.error(f"Job '{name}' not found.")
        raise typer.Exit(1)

    if not force:
        if not typer.confirm(f"  Remove job '{name}'?", default=False):
            raise typer.Exit(0)

    del data["jobs"][name]
    save_config_raw(data, path)
    output.success(f"Job removed: {name}")


@job_app.command("list")
def job_list(config: ConfigOption = None):
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Core commands
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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

    # Start with minimal clean config
    target.write_text(_default_config())

    output.success(f"Config created: {target}")
    output.info("Add servers and jobs:")
    output.info("  keepr server add production --host 1.2.3.4")
    output.info("  keepr job add my-db")


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

    output.console.print("  # keepr - paste into crontab (crontab -e)")
    output.console.print("  # Run all jobs daily at 3:00 AM")
    output.console.print(
        f"  0 3 * * * {keepr_bin} run --all >> /var/log/keepr.log 2>&1"
    )
    output.console.print()
    output.console.print("  # Or run individual jobs:")
    for i, name in enumerate(cfg.jobs):
        minute = i * 15
        output.console.print(
            f"  # {minute} 3 * * * {keepr_bin} run {name} "
            f">> /var/log/keepr.log 2>&1"
        )
    output.console.print()
    output.console.print("  # Cleanup old backups daily at 5:00 AM")
    output.console.print(
        f"  # 0 5 * * * {keepr_bin} cleanup >> /var/log/keepr.log 2>&1"
    )


# ── Helpers ───────────────────────────────────────────

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

jobs: {}
"""
