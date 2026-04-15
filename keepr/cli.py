from __future__ import annotations

import shutil
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
    no_args_is_help=True,
    rich_markup_mode="rich",
)

# ── Sub-apps ──────────────────────────────────────────
db_app = typer.Typer(help="Manage database backups.", no_args_is_help=True)
files_app = typer.Typer(help="Manage file backups.", no_args_is_help=True)
server_app = typer.Typer(help="Manage SSH servers.", no_args_is_help=True)
app.add_typer(db_app, name="db")
app.add_typer(files_app, name="files")
app.add_typer(server_app, name="server")

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
# keepr db add/remove/list
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@db_app.command("add")
def db_add(
    name: Annotated[str, typer.Argument(help="Backup name")],
    config: ConfigOption = None,
):
    """Add a database backup."""
    output.header()

    data, path = _load_raw(config)
    data.setdefault("servers", {})
    data.setdefault("jobs", {})

    if name in data["jobs"]:
        output.error(f"'{name}' already exists. Use 'keepr db remove {name}' first.")
        raise typer.Exit(1)

    # Engine
    engine = _prompt_choice("Engine", ENGINES, default="postgres")

    job_data: dict = {"type": "database", "engine": engine}
    db: dict = {}

    if engine == "sqlite":
        db["path"] = typer.prompt("  Path")
        job_data["server"] = "local"
        job_data["database"] = db
        # SQLite is always local, destinations = local only or local+s3
        dests = _prompt_destinations(data, path, ssh=False)
    else:
        # Connection type
        output.console.print()
        output.info("Baglanti yontemi:")
        output.info("  1) Direkt — pg_dump/mysqldump burada calisir, DB'ye uzaktan baglanir")
        output.info("  2) SSH — sunucuya baglanip orada dump calistirir")
        conn = _prompt_choice("Sec", ["1", "2"], default="1")

        if conn == "1":
            # Direct connection
            db["host"] = typer.prompt("  DB host")
            default_port = "5432" if engine == "postgres" else "3306"
            db["port"] = int(typer.prompt("  DB port", default=default_port))
            db["name"] = typer.prompt("  DB name")
            default_user = "postgres" if engine == "postgres" else "root"
            db["user"] = typer.prompt("  DB user", default=default_user)
            password = typer.prompt("  DB password (bos = atla)", default="", show_default=False)
            if password:
                db["password"] = password
            extra = typer.prompt("  Extra dump args (opsiyonel)", default="")
            if extra:
                db["extra_args"] = extra

            job_data["server"] = "local"
            job_data["database"] = db
            dests = _prompt_destinations(data, path, ssh=False)
        else:
            # SSH connection
            server_name = _prompt_server_selection(data, path)
            db["host"] = typer.prompt("  DB host (sunucu uzerinde)", default="localhost")
            default_port = "5432" if engine == "postgres" else "3306"
            db["port"] = int(typer.prompt("  DB port", default=default_port))
            db["name"] = typer.prompt("  DB name")
            default_user = "postgres" if engine == "postgres" else "root"
            db["user"] = typer.prompt("  DB user", default=default_user)
            password = typer.prompt("  DB password (bos = atla)", default="", show_default=False)
            if password:
                db["password"] = password
            extra = typer.prompt("  Extra dump args (opsiyonel)", default="")
            if extra:
                db["extra_args"] = extra

            job_data["server"] = server_name
            job_data["database"] = db
            dests = _prompt_destinations(data, path, ssh=True)

    job_data["destinations"] = dests

    # Retention
    if typer.confirm("  Custom retention?", default=False):
        retention = {}
        for d in dests:
            key = f"keep_{d}"
            val = typer.prompt(f"    {key}", default="7")
            retention[key] = int(val)
        job_data["retention"] = retention

    data["jobs"][name] = job_data
    save_config_raw(data, path)

    srv = job_data["server"]
    host = db.get("host") or db.get("path", "")
    output.success(f"Added: {name} ({engine} @ {host}) — server: {srv}")


@db_app.command("remove")
def db_remove(
    name: Annotated[str, typer.Argument(help="Backup name")],
    force: Annotated[bool, typer.Option("--force", "-f")] = False,
    config: ConfigOption = None,
):
    """Remove a database backup."""
    output.header()

    data, path = _load_raw(config)
    jobs = data.get("jobs", {})

    if name not in jobs:
        output.error(f"'{name}' not found.")
        raise typer.Exit(1)

    if jobs[name].get("type") != "database":
        output.error(f"'{name}' is not a database backup. Use 'keepr files remove'.")
        raise typer.Exit(1)

    if not force and not typer.confirm(f"  Remove '{name}'?", default=False):
        raise typer.Exit(0)

    del data["jobs"][name]
    save_config_raw(data, path)
    output.success(f"Removed: {name}")


@db_app.command("list")
def db_list(config: ConfigOption = None):
    """List database backups."""
    output.header()

    cfg = _load(config)
    db_jobs = {n: j for n, j in cfg.jobs.items() if j.type == "database"}

    if not db_jobs:
        output.warning("No database backups configured.")
        raise typer.Exit(0)

    table = output.make_table("Name", "Engine", "Database", "Connection", "Destinations")
    for name, job in db_jobs.items():
        server = cfg.servers.get(job.server)
        db_name = job.database.name or job.database.path or ""
        if server and server.is_local:
            conn = f"direkt ({job.database.host})" if job.database.host != "localhost" else "lokal"
        else:
            conn = f"SSH ({job.server})"
        dests = ", ".join(d.value for d in cfg.get_destinations(job))
        table.add_row(name, job.engine, db_name, conn, dests)

    output.console.print(table)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# keepr files add/remove/list
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@files_app.command("add")
def files_add(
    name: Annotated[str, typer.Argument(help="Backup name")],
    config: ConfigOption = None,
):
    """Add a file/directory backup."""
    output.header()

    data, path = _load_raw(config)
    data.setdefault("servers", {})
    data.setdefault("jobs", {})

    if name in data["jobs"]:
        output.error(f"'{name}' already exists.")
        raise typer.Exit(1)

    # Server selection (files always need a server context)
    servers = data.get("servers", {})
    output.console.print()
    output.info("Dosyalar nerede?")
    output.info("  1) Bu makine (lokal)")
    output.info("  2) Uzak sunucu (SSH)")
    where = _prompt_choice("Sec", ["1", "2"], default="1")

    if where == "1":
        server_name = "local"
        ssh = False
    else:
        server_name = _prompt_server_selection(data, path)
        ssh = True

    # Paths
    paths_input = typer.prompt("  Backup alinacak dizinler (virgul ile)")
    paths = [p.strip() for p in paths_input.split(",")]

    exclude_input = typer.prompt("  Haric tutulacak pattern'ler (opsiyonel)", default="")
    exclude = [e.strip() for e in exclude_input.split(",") if e.strip()]

    files_data: dict = {"paths": paths}
    if exclude:
        files_data["exclude"] = exclude

    dests = _prompt_destinations(data, path, ssh=ssh)

    job_data: dict = {
        "server": server_name,
        "type": "files",
        "files": files_data,
        "destinations": dests,
    }

    # Retention
    if typer.confirm("  Custom retention?", default=False):
        retention = {}
        for d in dests:
            key = f"keep_{d}"
            val = typer.prompt(f"    {key}", default="7")
            retention[key] = int(val)
        job_data["retention"] = retention

    data["jobs"][name] = job_data
    save_config_raw(data, path)

    output.success(f"Added: {name} (files @ {server_name})")


@files_app.command("remove")
def files_remove(
    name: Annotated[str, typer.Argument(help="Backup name")],
    force: Annotated[bool, typer.Option("--force", "-f")] = False,
    config: ConfigOption = None,
):
    """Remove a file backup."""
    output.header()

    data, path = _load_raw(config)
    jobs = data.get("jobs", {})

    if name not in jobs:
        output.error(f"'{name}' not found.")
        raise typer.Exit(1)

    if not force and not typer.confirm(f"  Remove '{name}'?", default=False):
        raise typer.Exit(0)

    del data["jobs"][name]
    save_config_raw(data, path)
    output.success(f"Removed: {name}")


@files_app.command("list")
def files_list(config: ConfigOption = None):
    """List file backups."""
    output.header()

    cfg = _load(config)
    file_jobs = {n: j for n, j in cfg.jobs.items() if j.type == "files"}

    if not file_jobs:
        output.warning("No file backups configured.")
        raise typer.Exit(0)

    table = output.make_table("Name", "Paths", "Server", "Destinations")
    for name, job in file_jobs.items():
        paths = ", ".join(job.files.paths)
        server = cfg.servers.get(job.server)
        srv = "lokal" if server and server.is_local else job.server
        dests = ", ".join(d.value for d in cfg.get_destinations(job))
        table.add_row(name, paths, srv, dests)

    output.console.print(table)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# keepr server add/remove/list
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@server_app.command("add")
def server_add(
    name: Annotated[str, typer.Argument(help="Server name")],
    host: Annotated[str, typer.Option("--host", "-H", help="Hostname or IP")] = "",
    user: Annotated[str, typer.Option("--user", "-u", help="SSH user")] = "root",
    port: Annotated[int, typer.Option("--port", "-p", help="SSH port")] = 22,
    ssh_key: Annotated[Optional[str], typer.Option("--ssh-key", help="SSH key path")] = None,
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
    ssh_key_input = typer.prompt("  SSH key (opsiyonel)", default=ssh_key or "")

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
    servers = data.get("servers", {})

    if name not in servers:
        output.error(f"Server '{name}' not found.")
        raise typer.Exit(1)

    if name == "local":
        output.error("Cannot remove 'local' server.")
        raise typer.Exit(1)

    # Check usage
    jobs = data.get("jobs", {})
    using = [j for j, jcfg in jobs.items() if jcfg.get("server") == name]
    if using:
        output.warning(f"Bu server'i kullanan job'lar: {', '.join(using)}")

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
# Shared prompts
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _prompt_choice(label: str, options: list[str], default: str) -> str:
    result = typer.prompt(f"  {label} ({'/'.join(options)})", default=default)
    if result not in options:
        output.error(f"Gecersiz secim: {result}")
        raise typer.Exit(1)
    return result


def _prompt_server_selection(data: dict, path: Path) -> str:
    """Show server list with option to add new. Returns server name."""
    servers = data.get("servers", {})
    remote_servers = {n: s for n, s in servers.items() if s.get("host") not in ("localhost", "127.0.0.1")}

    output.console.print()
    choices = []
    for i, (name, srv) in enumerate(remote_servers.items(), 1):
        host = srv.get("host", "")
        user = srv.get("user", "root")
        output.info(f"  {i}) {name} ({user}@{host})")
        choices.append(name)

    new_idx = len(choices) + 1
    output.info(f"  {new_idx}) + Yeni server ekle")

    choice = typer.prompt("  Server sec", default="1")

    try:
        idx = int(choice)
        if idx == new_idx:
            # Create new server inline
            return _create_server_inline(data, path)
        elif 1 <= idx <= len(choices):
            return choices[idx - 1]
    except ValueError:
        # Maybe they typed the server name directly
        if choice in servers:
            return choice

    output.error("Gecersiz secim.")
    raise typer.Exit(1)


def _create_server_inline(data: dict, path: Path) -> str:
    """Create a new server during db/files add."""
    name = typer.prompt("  Server ismi")
    host = typer.prompt("  Host")
    user = typer.prompt("  SSH user", default="root")
    port = int(typer.prompt("  SSH port", default="22"))
    ssh_key = typer.prompt("  SSH key (opsiyonel)", default="")

    server_data: dict = {"host": host, "user": user, "port": port}
    if ssh_key:
        server_data["ssh_key"] = ssh_key

    data.setdefault("servers", {})
    data["servers"][name] = server_data
    save_config_raw(data, path)
    output.success(f"Server eklendi: {name} ({user}@{host}:{port})")
    return name


def _prompt_destinations(data: dict, path: Path, ssh: bool) -> list[str]:
    """Prompt for backup destinations. Shows server option only if SSH."""
    output.console.print()
    output.info("Backup nereye kaydedilsin?")

    if ssh:
        options = [
            ("1", "local", "bu makinede ~/backups/"),
            ("2", "s3", "S3 bucket'a yukle"),
            ("3", "server", "sunucunun diskinde birak"),
            ("4", "local + s3", None),
            ("5", "server + s3", None),
            ("6", "local + server + s3", None),
        ]
    else:
        options = [
            ("1", "local", "bu makinede ~/backups/"),
            ("2", "s3", "S3 bucket'a yukle"),
            ("3", "local + s3", None),
        ]

    for num, label, desc in options:
        if desc:
            output.info(f"  {num}) {label} — {desc}")
        else:
            output.info(f"  {num}) {label}")

    choice = typer.prompt("  Sec", default="1")

    dest_map = {num: label for num, label, _ in options}
    selected = dest_map.get(choice)
    if not selected:
        output.error("Gecersiz secim.")
        raise typer.Exit(1)

    dests = [d.strip() for d in selected.split("+")]

    # S3 setup if needed
    if "s3" in dests:
        _ensure_s3_configured(data, path)

    return dests


def _ensure_s3_configured(data: dict, path: Path) -> None:
    """If S3 is not configured, prompt for setup."""
    storage = data.get("storage", {})
    if storage.get("s3"):
        return  # Already configured

    output.console.print()
    output.warning("S3 henuz ayarlanmamis. Simdi ayarlayalim:")

    bucket = typer.prompt("  S3 bucket")
    region = typer.prompt("  Region", default="eu-central-1")
    prefix = typer.prompt("  Prefix", default="keepr/")
    access_key = typer.prompt("  Access Key ID (bos = env'den al)", default="", show_default=False)
    secret_key = typer.prompt("  Secret Access Key (bos = env'den al)", default="", show_default=False)
    endpoint = typer.prompt("  Endpoint URL (MinIO icin, opsiyonel)", default="")

    s3_data: dict = {"bucket": bucket, "region": region, "prefix": prefix}
    if access_key:
        s3_data["access_key_id"] = access_key
    if secret_key:
        s3_data["secret_access_key"] = secret_key
    if endpoint:
        s3_data["endpoint_url"] = endpoint

    data.setdefault("storage", {})
    data["storage"]["s3"] = s3_data
    save_config_raw(data, path)
    output.success("S3 ayarlandi.")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Core commands
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.command()
def init():
    """Create config file."""
    output.header()

    target = Path.home() / ".config" / "keepr" / "keepr.yml"
    if target.exists():
        output.warning(f"Config already exists: {target}")
        overwrite = typer.confirm("  Overwrite?", default=False)
        if not overwrite:
            raise typer.Exit(0)

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(_default_config())

    output.success(f"Config created: {target}")
    output.info("Baslangic:")
    output.info("  keepr db add cortex-db")
    output.info("  keepr files add uploads")


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
        output.info("S3:         ayarlanmamis")

    r = cfg.defaults.retention
    output.info(f"Retention:  local={r.keep_local}, s3={r.keep_s3}, server={r.keep_server}")

    # Servers
    remote = {n: s for n, s in cfg.servers.items() if not s.is_local}
    if remote:
        output.console.print()
        output.info(f"SSH Servers: {len(remote)}")
        for name, srv in remote.items():
            output.info(f"  {name}: {srv.user}@{srv.host}:{srv.port}")

    # Jobs
    if cfg.jobs:
        output.console.print()
        output.info(f"Backups: {len(cfg.jobs)}")
        for name, job in cfg.jobs.items():
            engine = job.engine or "files"
            dests = ", ".join(d.value for d in cfg.get_destinations(job))
            output.info(f"  {name}: {job.type} ({engine}) -> \\[{dests}]")


@app.command()
def run(
    job_names: Annotated[
        Optional[list[str]], typer.Argument(help="Backup names to run")
    ] = None,
    all_jobs: Annotated[bool, typer.Option("--all", help="Run all")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Preview only")] = False,
    config: ConfigOption = None,
):
    """Run backups."""
    output.header()

    cfg = _load(config)
    from keepr.backup import run_backup

    targets = _resolve_jobs(cfg, job_names, all_jobs)
    for name in targets:
        run_backup(cfg, name, cfg.jobs[name], dry_run=dry_run)


@app.command("list")
def list_backups(
    job_name: Annotated[Optional[str], typer.Argument(help="Filter by name")] = None,
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

    table = output.make_table("ID", "Name", "Engine", "Date", "Size", "Locations")
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
    job_name: Annotated[Optional[str], typer.Argument(help="Backup name")] = None,
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


# ── Helpers ───────────────────────────────────────────

def _resolve_jobs(cfg: KeeprConfig, names: list[str] | None, all_jobs: bool) -> list[str]:
    if all_jobs or not names:
        if not cfg.jobs:
            output.warning("No backups configured.")
            raise typer.Exit(0)
        return list(cfg.jobs.keys())
    for n in names:
        if n not in cfg.jobs:
            output.error(f"Not found: {n}")
            raise typer.Exit(1)
    return names


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

servers:
  local:
    host: localhost

jobs: {}
"""
