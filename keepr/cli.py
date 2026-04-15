from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Optional

import typer

from keepr import output
from keepr.config import (
    Destination,
    KeeprConfig,
    load_config,
    load_config_raw,
    save_config_raw,
)

app = typer.Typer(
    name="keepr",
    help="Database & file backup manager with SSH and S3 support.",
    rich_markup_mode="rich",
    invoke_without_command=True,
)


@app.callback(invoke_without_command=True)
def main_callback(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        _show_status()


def _show_status() -> None:
    from keepr import __version__
    output.console.print(f"\n  [bold]keepr[/bold] [muted]v{__version__}[/muted]\n")

    try:
        cfg = load_config()
    except FileNotFoundError:
        output.warning("No config found. Run [bold]keepr init[/bold] to get started.")
        output.console.print()
        return
    except Exception as e:
        output.error(f"Config error: {e}")
        output.console.print()
        return

    job_count = len(cfg.jobs)
    output.info(f"{job_count} job{'s' if job_count != 1 else ''} configured")

    try:
        from keepr.catalog import load_catalog
        catalog = load_catalog()
        if catalog.backups:
            last = max(catalog.backups, key=lambda b: b.timestamp)
            now = datetime.now(timezone.utc)
            delta = now - last.timestamp
            hours = int(delta.total_seconds() // 3600)
            if hours < 1:
                age = "just now"
            elif hours < 24:
                age = f"{hours}h ago"
            else:
                days = hours // 24
                age = f"{days}d ago"
            output.info(f"Last backup: [bold]{last.job}[/bold] — {age}")
        else:
            output.info("No backups yet — run [bold]keepr run[/bold]")
    except Exception:
        pass

    output.console.print(f"\n  [muted]keepr run · keepr list · keepr job add <name>[/muted]\n")

job_app = typer.Typer(help="Manage backup jobs.")
server_app = typer.Typer(help="Manage SSH servers.")
app.add_typer(job_app, name="job")
app.add_typer(server_app, name="server")


@job_app.callback(invoke_without_command=True)
def job_callback(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit(0)


@server_app.callback(invoke_without_command=True)
def server_callback(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit(0)

ConfigOption = Annotated[
    Optional[Path],
    typer.Option("--config", "-c", help="Path to config file"),
]

ENGINES = ["postgres", "mysql", "sqlite"]


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
# keepr init — interactive setup wizard
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.command()
def init():
    """Interactive setup wizard."""
    output.header()
    output.console.print("  Welcome to [bold]keepr[/bold]!\n")

    target = Path.home() / ".config" / "keepr" / "keepr.yml"
    if target.exists():
        output.warning(f"Config already exists: {target}")
        if not typer.confirm("  Overwrite?", default=False):
            raise typer.Exit(0)

    target.parent.mkdir(parents=True, exist_ok=True)

    data: dict = {"servers": {"local": {"host": "localhost"}}, "jobs": {}}

    # 1. Storage
    output.console.print("  [bold]1. Storage[/bold]")
    local_dir = typer.prompt("  Local backup path", default="~/backups")
    data["storage"] = {"local_dir": local_dir, "server_dir": "/var/backups/keepr"}

    # S3
    if typer.confirm("\n  Upload backups to S3?", default=False):
        data["storage"]["s3"] = _prompt_s3_config()
        default_dests = ["local", "s3"]
    else:
        default_dests = ["local"]

    data["defaults"] = {
        "retention": {"keep_local": 7, "keep_s3": 30, "keep_server": 5},
        "destinations": default_dests,
    }

    # 2. Servers
    output.console.print(f"\n  [bold]2. Servers[/bold]")
    while typer.confirm("  Add a remote server?", default=False):
        name = typer.prompt("    Name")
        host = typer.prompt("    Host")
        user = typer.prompt("    SSH user", default="root")
        port = int(typer.prompt("    SSH port", default="22"))
        ssh_key = typer.prompt("    SSH key (optional)", default="")
        srv: dict = {"host": host, "user": user, "port": port}
        if ssh_key:
            srv["ssh_key"] = ssh_key
        data["servers"][name] = srv
        output.success(f"Server added: {name} ({user}@{host})")

    # 3. Jobs
    output.console.print(f"\n  [bold]3. Backup jobs[/bold]")
    while typer.confirm("  Add a backup job?", default=True if not data["jobs"] else False):
        _prompt_job(data, None)

    # Save
    save_config_raw(data, target)

    output.console.print()
    job_count = len(data["jobs"])
    output.success(f"Setup complete! {job_count} job(s) configured.")
    output.info(f"Config saved: {target}")
    if job_count > 0:
        output.info("Run 'keepr run' to start backing up.")
    else:
        output.info("Run 'keepr job add <name>' to add a backup job.")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# keepr job add/remove/list
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@job_app.command("add")
def job_add(
    name: Annotated[str, typer.Argument(help="Job name")],
    config: ConfigOption = None,
):
    """Add a backup job."""
    output.header()

    data, path = _load_raw(config)
    data.setdefault("servers", {})
    data.setdefault("jobs", {})

    if name in data["jobs"]:
        output.error(f"'{name}' already exists. Use 'keepr job remove {name}' first.")
        raise typer.Exit(1)

    _prompt_job(data, name)
    save_config_raw(data, path)


@job_app.command("remove")
def job_remove(
    names: Annotated[list[str], typer.Argument(help="Job name(s) to remove")],
    force: Annotated[bool, typer.Option("--force", "-f")] = False,
    config: ConfigOption = None,
):
    """Remove one or more backup jobs."""
    output.header()

    data, path = _load_raw(config)
    jobs = data.get("jobs", {})

    missing = [n for n in names if n not in jobs]
    if missing:
        output.error(f"Not found: {', '.join(missing)}")
        raise typer.Exit(1)

    if not force:
        label = f"{len(names)} job(s): {', '.join(names)}" if len(names) > 1 else f"'{names[0]}'"
        if not typer.confirm(f"  Remove {label}?", default=False):
            raise typer.Exit(0)

    for name in names:
        del data["jobs"][name]
    save_config_raw(data, path)
    output.success(f"Removed: {', '.join(names)}")


@job_app.command("rename")
def job_rename(
    old_name: Annotated[str, typer.Argument(help="Current job name")],
    new_name: Annotated[str, typer.Argument(help="New job name")],
    config: ConfigOption = None,
):
    """Rename a backup job."""
    output.header()

    data, path = _load_raw(config)
    jobs = data.get("jobs", {})

    if old_name not in jobs:
        output.error(f"'{old_name}' not found.")
        raise typer.Exit(1)
    if new_name in jobs:
        output.error(f"'{new_name}' already exists.")
        raise typer.Exit(1)

    data["jobs"] = {(new_name if k == old_name else k): v for k, v in jobs.items()}
    save_config_raw(data, path)
    output.success(f"Renamed: {old_name} → {new_name}")


@job_app.command("list")
def job_list(config: ConfigOption = None):
    """List backup jobs."""
    output.header()

    cfg = _load(config)
    if not cfg.jobs:
        output.warning("No jobs configured.")
        raise typer.Exit(0)

    table = output.make_table("Name", "Type", "Engine", "Server", "Destinations")
    for name, job in cfg.jobs.items():
        engine = job.engine or "-"
        dests = ", ".join(d.value for d in cfg.get_destinations(job))
        server = cfg.servers.get(job.server)
        srv_display = "local" if server and server.is_local else job.server
        table.add_row(name, job.type_label, engine, srv_display, dests)

    output.console.print(table)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# keepr server add/remove/list
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@server_app.command("add")
def server_add(
    name: Annotated[str, typer.Argument(help="Server name")],
    host: Annotated[str, typer.Option("--host", "-H")] = "",
    user: Annotated[str, typer.Option("--user", "-u")] = "root",
    port: Annotated[int, typer.Option("--port", "-p")] = 22,
    ssh_key: Annotated[Optional[str], typer.Option("--ssh-key")] = None,
    config: ConfigOption = None,
):
    """Add an SSH server."""
    output.header()

    data, path = _load_raw(config)
    data.setdefault("servers", {})

    if name in data["servers"]:
        output.error(f"Server '{name}' already exists.")
        raise typer.Exit(1)

    if not host:
        host = typer.prompt("  Host")
    user = typer.prompt("  SSH user", default=user)
    port = int(typer.prompt("  SSH port", default=str(port)))
    ssh_key_input = typer.prompt("  SSH key (optional)", default=ssh_key or "")

    server_data: dict = {"host": host, "user": user, "port": port}
    if ssh_key_input:
        server_data["ssh_key"] = ssh_key_input

    data["servers"][name] = server_data
    save_config_raw(data, path)
    output.success(f"Server added: {name} ({user}@{host}:{port})")


@server_app.command("remove")
def server_remove(
    name: Annotated[str, typer.Argument(help="Server name")],
    force: Annotated[bool, typer.Option("--force", "-f")] = False,
    config: ConfigOption = None,
):
    """Remove an SSH server."""
    output.header()

    data, path = _load_raw(config)
    if name not in data.get("servers", {}):
        output.error(f"Server '{name}' not found.")
        raise typer.Exit(1)
    if name == "local":
        output.error("Cannot remove 'local' server.")
        raise typer.Exit(1)

    jobs = data.get("jobs", {})
    using = [j for j, jcfg in jobs.items() if jcfg.get("server") == name]
    if using:
        output.warning(f"Jobs using this server: {', '.join(using)}")

    if not force and not typer.confirm(f"  Remove '{name}'?", default=False):
        raise typer.Exit(0)

    del data["servers"][name]
    save_config_raw(data, path)
    output.success(f"Server removed: {name}")


@server_app.command("list")
def server_list(config: ConfigOption = None):
    """List SSH servers."""
    output.header()

    cfg = _load(config)
    remote = {n: s for n, s in cfg.servers.items() if not s.is_local}

    if not remote:
        output.warning("No SSH servers configured.")
        raise typer.Exit(0)

    table = output.make_table("Name", "Host", "User", "Port", "SSH Key")
    for name, srv in remote.items():
        table.add_row(name, srv.host, srv.user, str(srv.port), srv.ssh_key or "-")

    output.console.print(table)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Core commands
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.command()
def run(
    job_names: Annotated[Optional[list[str]], typer.Argument(help="Job names (e.g. 'keepr run cortex'). Runs all if omitted.")] = None,
    all_jobs: Annotated[bool, typer.Option("--all", help="Run all jobs")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Preview without executing")] = False,
    config: ConfigOption = None,
):
    """Run backup jobs. Specify job names or use --all."""
    output.header()
    cfg = _load(config)
    from keepr.backup import run_backup

    targets = _resolve_jobs(cfg, job_names, all_jobs)
    for name in targets:
        run_backup(cfg, name, cfg.jobs[name], dry_run=dry_run)


@app.command("list")
def list_backups(
    job_name: Annotated[Optional[str], typer.Argument(help="Filter by job")] = None,
    config: ConfigOption = None,
):
    """List taken backups."""
    output.header()
    from keepr.catalog import load_catalog

    _load(config)
    catalog = load_catalog()

    backups = catalog.backups
    if job_name:
        backups = [b for b in backups if b.job == job_name]

    if not backups:
        output.warning("No backups found.")
        raise typer.Exit(0)

    backups.sort(key=lambda b: b.timestamp, reverse=True)
    table = output.make_table("ID", "Job", "Type", "Date", "Size", "Locations")
    for b in backups:
        locs = ", ".join(b.locations.keys())
        table.add_row(
            b.id, b.job, b.engine or b.type,
            b.timestamp.strftime("%Y-%m-%d %H:%M"),
            output.format_size(b.size_bytes), locs,
        )
    output.console.print(table)


@app.command()
def restore(
    backup_id: Annotated[str, typer.Argument(help="Backup ID")],
    config: ConfigOption = None,
):
    """Restore a backup."""
    output.header()
    cfg = _load(config)
    from keepr.restore import restore_backup
    restore_backup(cfg, backup_id)


@app.command()
def delete(
    backup_id: Annotated[str, typer.Argument(help="Backup ID")],
    force: Annotated[bool, typer.Option("--force", "-f")] = False,
    config: ConfigOption = None,
):
    """Delete a backup."""
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
        if not typer.confirm(f"  Delete {backup_id} from [{locs}]?", default=False):
            raise typer.Exit(0)

    delete_backup_files(cfg, entry)
    catalog.remove(backup_id)
    save_catalog(catalog)
    output.success(f"Deleted: {backup_id}")


@app.command()
def cleanup(
    job_name: Annotated[Optional[str], typer.Argument(help="Job name")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    config: ConfigOption = None,
):
    """Apply retention policies."""
    output.header()
    cfg = _load(config)
    from keepr.retention import apply_retention

    names = [job_name] if job_name else list(cfg.jobs.keys())
    for name in names:
        if name not in cfg.jobs:
            output.error(f"Not found: {name}")
            continue
        apply_retention(cfg, name, cfg.jobs[name], dry_run=dry_run)


@app.command("config")
def show_config(config: ConfigOption = None):
    """Show current configuration."""
    output.header()
    cfg = _load(config)

    output.success("Config is valid.\n")
    output.info(f"Local dir:  {cfg.storage.resolved_local_dir}")
    output.info(f"Server dir: {cfg.storage.server_dir}")
    if cfg.storage.s3:
        output.info(f"S3 bucket:  {cfg.storage.s3.bucket} ({cfg.storage.s3.region})")
    else:
        output.info("S3:         not configured")

    r = cfg.defaults.retention
    output.info(f"Retention:  local={r.keep_local}, s3={r.keep_s3}, server={r.keep_server}")

    remote = {n: s for n, s in cfg.servers.items() if not s.is_local}
    if remote:
        output.console.print()
        output.info(f"SSH Servers: {len(remote)}")
        for name, srv in remote.items():
            output.info(f"  {name}: {srv.user}@{srv.host}:{srv.port}")

    if cfg.jobs:
        output.console.print()
        output.info(f"Jobs: {len(cfg.jobs)}")
        for name, job in cfg.jobs.items():
            dests = ", ".join(d.value for d in cfg.get_destinations(job))
            output.info(f"  {name}: {job.type_label} -> \\[{dests}]")


@app.command()
def cron(config: ConfigOption = None):
    """Print crontab entries."""
    output.header()
    cfg = _load(config)
    keepr_bin = shutil.which("keepr") or "keepr"

    output.console.print("  # keepr - paste into crontab (crontab -e)")
    output.console.print(f"  0 3 * * * {keepr_bin} run --all >> /var/log/keepr.log 2>&1")
    output.console.print()
    for i, name in enumerate(cfg.jobs):
        m = i * 15
        output.console.print(f"  # {m} 3 * * * {keepr_bin} run {name} >> /var/log/keepr.log 2>&1")
    output.console.print(f"  # 0 5 * * * {keepr_bin} cleanup >> /var/log/keepr.log 2>&1")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Shared prompt helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _prompt_job(data: dict, name: str | None) -> None:
    """Interactive job creation. Mutates data dict."""
    if not name:
        name = typer.prompt("    Name")

    if name in data.get("jobs", {}):
        output.error(f"'{name}' already exists.")
        raise typer.Exit(1)

    # Server selection
    server_name = _prompt_server_for_job(data)
    server = data["servers"][server_name]
    is_ssh = server.get("host") not in ("localhost", "127.0.0.1")

    job_data: dict = {"server": server_name}

    # What to backup
    output.console.print()
    output.info("What to backup?")
    output.info("  1) Database")
    output.info("  2) Files")
    output.info("  3) Database + Files")
    what = _prompt_choice("Select", ["1", "2", "3"], default="1")

    # Database
    if what in ("1", "3"):
        output.console.print()
        output.info("[bold]-- Database --[/bold]")
        engine = _prompt_choice("Engine", ENGINES, default="postgres")
        job_data["engine"] = engine

        db: dict = {}
        if engine == "sqlite":
            db["path"] = typer.prompt("    Path")
        else:
            if not is_ssh:
                output.info("Connection: direct (dump runs here)")
                db["host"] = typer.prompt("    DB host")
                job_data["connection"] = "direct"
            else:
                output.info("Connection: SSH (dump runs on server)")
                db["host"] = typer.prompt("    DB host (on server)", default="localhost")
                job_data["connection"] = "ssh"

            default_port = "5432" if engine == "postgres" else "3306"
            db["port"] = int(typer.prompt("    DB port", default=default_port))
            db["name"] = typer.prompt("    DB name")
            default_user = "postgres" if engine == "postgres" else "root"
            db["user"] = typer.prompt("    DB user", default=default_user)
            password = typer.prompt("    DB password (empty = skip)", default="", show_default=False)
            if password:
                db["password"] = password
            extra = typer.prompt("    Extra dump args (optional)", default="")
            if extra:
                db["extra_args"] = extra

            # Format (postgres only)
            if engine == "postgres":
                output.console.print()
                output.info("Dump format:")
                output.info("  1) custom (.dump) — compressed, supports parallel restore")
                output.info("  2) sql (.sql.gz) — plain SQL, human-readable")
                fmt = _prompt_choice("Select", ["1", "2"], default="1")
                db["format"] = "sql" if fmt == "2" else "custom"

        # Binary path detection (only for direct connection)
        if not is_ssh:
            binary_name = {"postgres": "pg_dump", "mysql": "mysqldump", "sqlite": "sqlite3"}[engine]
            detected = shutil.which(binary_name)
            if detected:
                output.success(f"{binary_name} found: {detected}")
                if typer.confirm("    Use a different path?", default=False):
                    db["dump_path"] = typer.prompt("    Binary path")
            else:
                output.warning(f"{binary_name} not found in PATH.")
                db["dump_path"] = typer.prompt(f"    Path to {binary_name}")

        job_data["database"] = db

    # Files
    if what in ("2", "3"):
        output.console.print()
        output.info("[bold]-- Files --[/bold]")
        paths_input = typer.prompt("    Directories (comma-separated)")
        paths = [p.strip() for p in paths_input.split(",")]
        exclude_input = typer.prompt("    Exclude patterns (optional)", default="")
        exclude = [e.strip() for e in exclude_input.split(",") if e.strip()]

        files_data: dict = {"paths": paths}
        if exclude:
            files_data["exclude"] = exclude
        job_data["files"] = files_data

    # Destinations
    dests = _prompt_destinations(data, is_ssh)
    job_data["destinations"] = dests

    # Retention
    if typer.confirm("    Custom retention?", default=False):
        retention = {}
        for d in dests:
            key = f"keep_{d}"
            val = typer.prompt(f"      {key}", default="7")
            retention[key] = int(val)
        job_data["retention"] = retention

    data.setdefault("jobs", {})
    data["jobs"][name] = job_data
    output.success(f"Job added: {name} ({job_data.get('engine', 'files')})")


def _prompt_server_for_job(data: dict) -> str:
    """Select server for a job — local, existing, or new."""
    servers = data.get("servers", {})
    remote = {n: s for n, s in servers.items() if s.get("host") not in ("localhost", "127.0.0.1")}

    output.console.print()
    output.info("Server:")
    idx = 1
    choices: list[str] = []

    for name, srv in remote.items():
        host = srv.get("host", "")
        user = srv.get("user", "root")
        output.info(f"  {idx}) {name} ({user}@{host})")
        choices.append(name)
        idx += 1

    output.info(f"  {idx}) local (this machine)")
    local_idx = idx
    idx += 1

    output.info(f"  {idx}) + Add new server")
    new_idx = idx

    default = "1" if remote else str(local_idx)
    choice = typer.prompt("    Select", default=default)

    try:
        c = int(choice)
        if c == local_idx:
            return "local"
        if c == new_idx:
            return _create_server_inline(data)
        if 1 <= c <= len(choices):
            return choices[c - 1]
    except ValueError:
        if choice in servers:
            return choice

    output.error("Invalid choice.")
    raise typer.Exit(1)


def _create_server_inline(data: dict) -> str:
    """Create a new server inline during job setup."""
    name = typer.prompt("    Server name")
    host = typer.prompt("    Host")
    user = typer.prompt("    SSH user", default="root")
    port = int(typer.prompt("    SSH port", default="22"))
    ssh_key = typer.prompt("    SSH key (optional)", default="")

    srv: dict = {"host": host, "user": user, "port": port}
    if ssh_key:
        srv["ssh_key"] = ssh_key

    data.setdefault("servers", {})
    data["servers"][name] = srv
    output.success(f"Server added: {name} ({user}@{host}:{port})")
    return name


def _prompt_destinations(data: dict, ssh: bool) -> list[str]:
    """Prompt for backup destinations."""
    output.console.print()
    output.info("Where to save?")

    if ssh:
        options = [
            ("1", "local", "this machine ~/backups/"),
            ("2", "s3", "upload to S3"),
            ("3", "server", "keep on server disk"),
            ("4", "local + s3", None),
            ("5", "server + s3", None),
            ("6", "local + server + s3", None),
        ]
    else:
        options = [
            ("1", "local", "this machine ~/backups/"),
            ("2", "s3", "upload to S3"),
            ("3", "local + s3", None),
        ]

    for num, label, desc in options:
        if desc:
            output.info(f"  {num}) {label} — {desc}")
        else:
            output.info(f"  {num}) {label}")

    choice = typer.prompt("    Select", default="1")
    dest_map = {num: label for num, label, _ in options}
    selected = dest_map.get(choice)
    if not selected:
        output.error("Invalid choice.")
        raise typer.Exit(1)

    dests = [d.strip() for d in selected.split("+")]

    if "s3" in dests:
        _ensure_s3(data)

    return dests


def _ensure_s3(data: dict) -> None:
    """Prompt for S3 config if not set."""
    if data.get("storage", {}).get("s3"):
        return

    output.console.print()
    output.warning("S3 is not configured yet. Let's set it up:")
    data.setdefault("storage", {})
    data["storage"]["s3"] = _prompt_s3_config()
    output.success("S3 configured.")


def _prompt_s3_config() -> dict:
    """Prompt for S3 config fields and return dict."""
    bucket = typer.prompt("    S3 bucket")
    region = typer.prompt("    Region", default="eu-central-1")
    prefix = typer.prompt("    Prefix", default="keepr/")
    access_key = typer.prompt("    Access Key ID (empty = from env)", default="", show_default=False)
    secret_key = typer.prompt("    Secret Access Key (empty = from env)", default="", show_default=False)
    endpoint = typer.prompt("    Endpoint URL (optional, for MinIO)", default="")

    s3: dict = {"bucket": bucket, "region": region, "prefix": prefix}
    if access_key:
        s3["access_key_id"] = access_key
    if secret_key:
        s3["secret_access_key"] = secret_key
    if endpoint:
        s3["endpoint_url"] = endpoint
    return s3


def _prompt_choice(label: str, options: list[str], default: str) -> str:
    result = typer.prompt(f"    {label} ({'/'.join(options)})", default=default)
    if result not in options:
        output.error(f"Invalid choice: {result}")
        raise typer.Exit(1)
    return result


def _resolve_jobs(cfg: KeeprConfig, names: list[str] | None, all_jobs: bool) -> list[str]:
    if all_jobs or not names:
        if not cfg.jobs:
            output.warning("No jobs configured.")
            raise typer.Exit(0)
        return list(cfg.jobs.keys())
    for n in names:
        if n not in cfg.jobs:
            output.error(f"Not found: {n}")
            raise typer.Exit(1)
    return names
