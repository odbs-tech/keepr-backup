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
├── cli.py          # Typer CLI — init wizard, job/server sub-apps, core commands
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

- **Jobs** can have `database`, `files`, or both — no separate `type` field
- **Connection**: direct (dump runs locally) vs SSH (dump runs on server) — determined by server choice
- **Destinations**: `local` (keepr's machine), `server` (remote disk, SSH only), `s3`
- **Servers**: SSH connection profiles, created via `keepr server add` or inline during job setup
- **Init wizard**: `keepr init` walks through storage, servers, and jobs interactively

## Commands

```
keepr init                        # Interactive setup wizard
keepr job add/list/remove         # Job management
keepr server add/list/remove      # SSH server management
keepr run/list/restore/delete/cleanup/cron/config
```

## Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
keepr --help
```
