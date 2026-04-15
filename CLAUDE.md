# keepr

Database & file backup manager CLI tool.

## Stack

- Python 3.11+, Typer, Pydantic 2, PyYAML, boto3, Rich
- Build: hatchling
- Config: `~/.config/keepr/keepr.yml` (YAML)
- Catalog: `~/.config/keepr/catalog.json` (backup metadata)

## Project structure

```
keepr/
├── cli.py          # Typer CLI — db/files/server sub-apps + core commands
├── config.py       # Pydantic models, YAML load/save
├── executor.py     # Local + SSH command execution (subprocess)
├── backup.py       # Backup orchestration (DB dump + tar)
├── restore.py      # Restore orchestration
├── storage.py      # S3 upload/download/delete (boto3)
├── catalog.py      # JSON backup metadata tracking
├── retention.py    # Retention policy enforcement
├── output.py       # Rich terminal output helpers
└── engines/
    ├── base.py     # Abstract DatabaseEngine
    ├── postgres.py # pg_dump / pg_restore
    ├── mysql.py    # mysqldump / mysql
    └── sqlite.py   # sqlite3 .dump
```

## Key concepts

- **Connection types**: Direct (pg_dump runs locally, connects to remote DB) vs SSH (pg_dump runs on server via SSH)
- **Destinations**: `local` (keepr's machine), `server` (remote disk, SSH only), `s3`
- **Servers**: SSH connection profiles, managed via `keepr server add` or created inline during `keepr db add`
- **Jobs**: Backup definitions stored in config under `jobs:` key. Created via `keepr db add` or `keepr files add`

## Commands

```
keepr db add/list/remove      # Database backup management
keepr files add/list/remove   # File backup management
keepr server add/list/remove  # SSH server management
keepr run/list/restore/delete/cleanup/cron/config/init
```

## Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
keepr --help
```

## Testing

```bash
# Quick local test with SQLite
keepr init
keepr db add test-db   # choose sqlite, local
keepr run test-db
keepr list
keepr restore <ID>
```
