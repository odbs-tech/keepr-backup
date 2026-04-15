# keepr

Database & file backup manager with SSH and S3 support.

Backup PostgreSQL, MySQL, and SQLite databases alongside file directories. Run locally or connect to remote servers via SSH. Store backups on local disk, remote server, or S3.

## Features

- **Multi-engine database support** — PostgreSQL (`pg_dump`), MySQL (`mysqldump`), SQLite (file copy)
- **File & directory backups** — `tar.gz` with exclude patterns
- **SSH remote execution** — Backup remote servers from your local machine
- **Flexible storage** — Local disk, remote server disk, and/or S3
- **Retention policies** — Automatic cleanup per destination (local, server, S3)
- **Backup catalog** — Track all backups with metadata, sizes, and checksums
- **Restore support** — Restore any backup to its original or a different target
- **Cron ready** — Generate crontab entries for scheduled backups

## Quick Start

```bash
# Install
pip install -e .

# Create config
keepr init

# Edit config
vim ~/.config/keepr/keepr.yml

# Run all backup jobs
keepr run

# List backups
keepr list

# Restore a backup
keepr restore cortex-db_20260415_030000
```

## Configuration

Config file is searched in this order:
1. `./keepr.yml` (current directory)
2. `~/.config/keepr/keepr.yml`

Or specify with `--config`:
```bash
keepr --config /path/to/keepr.yml run
```

### Example Config

```yaml
storage:
  local_dir: ~/backups                    # Where backups are saved locally
  server_dir: /var/backups/keepr          # Where backups are saved on remote servers
  s3:
    bucket: my-backups
    region: eu-central-1
    prefix: keepr/

defaults:
  retention:
    keep_local: 7                         # Keep last 7 local backups per job
    keep_s3: 30                           # Keep last 30 S3 backups per job
    keep_server: 5                        # Keep last 5 server backups per job
  destinations:
    - local
    - s3

servers:
  local:
    host: localhost

  production:
    host: 159.69.42.100
    user: root
    port: 22

jobs:
  # PostgreSQL backup
  app-db:
    server: production
    type: database
    engine: postgres
    database:
      name: myapp
      user: postgres
      host: localhost
      port: 5432
    destinations: [local, s3]
    retention:
      keep_local: 3
      keep_s3: 14

  # MySQL backup
  blog-db:
    server: production
    type: database
    engine: mysql
    database:
      name: blog
      user: blog_user
      host: localhost
      port: 3306
    destinations: [local, s3]

  # SQLite backup (local)
  dev-db:
    server: local
    type: database
    engine: sqlite
    database:
      path: /path/to/app.db
    destinations: [local]

  # File backup
  uploads:
    server: production
    type: files
    files:
      paths:
        - /var/www/app/uploads
      exclude:
        - "*.tmp"
        - ".cache"
    destinations: [local, s3]

  # Keep backup on server + S3 (don't download locally)
  large-db:
    server: production
    type: database
    engine: postgres
    database:
      name: warehouse
      user: postgres
    destinations: [server, s3]
    retention:
      keep_server: 3
      keep_s3: 30
```

### Destinations

Each job can store backups in one or more destinations:

| Destination | Description |
|-------------|-------------|
| `local` | The machine where keepr is running |
| `server` | The remote server's own disk |
| `s3` | S3-compatible object storage |

### Database Passwords

Passwords can be set in config or via environment variables:

```yaml
database:
  password: mypassword            # In config (not recommended)
```

```bash
# Via environment variable
export KEEPR_CORTEX_DB_PASSWORD=mypassword
```

For PostgreSQL, `~/.pgpass` is also supported.

## CLI Reference

```
keepr run [JOB...]            Run backup jobs (all if none specified)
keepr run --all               Run all configured jobs
keepr run --dry-run           Show what would be done
keepr list [JOB]              List existing backups
keepr restore BACKUP_ID       Restore a backup
keepr delete BACKUP_ID        Delete a backup from all locations
keepr cleanup [JOB]           Apply retention policies
keepr jobs                    List configured jobs
keepr cron                    Print crontab entries
keepr config                  Validate and show config
keepr init                    Create example config file
```

## How It Works

### Remote Database Backup (SSH)

```
keepr run app-db

  keepr v0.1.0

  [>] Running job: app-db
  [>] Server: production (159.69.42.100)
  [>] Engine: postgres — database: myapp
  [>] Dumping via SSH...
  [OK] Saved: ~/backups/app-db/app-db_20260415_030000_postgres.dump (14.5 MB)
  [>] Uploading to S3...
  [OK] S3 upload complete.
  [OK] Done in 12.4s
```

The backup streams through SSH directly to your local machine:
```
ssh root@server "pg_dump -Fc myapp" > local_file.dump
```

### Local Backup

When `server: local`, keepr runs the dump command directly — no SSH involved.

### Retention

After each backup run, keepr automatically applies retention policies. Old backups beyond the `keep_*` limit are deleted from the respective destination.

## Scheduling

Generate crontab entries:

```bash
keepr cron
```

This outputs ready-to-use cron lines. Paste them into `crontab -e`:

```cron
0 3 * * * /usr/local/bin/keepr run --all >> /var/log/keepr.log 2>&1
0 5 * * * /usr/local/bin/keepr cleanup >> /var/log/keepr.log 2>&1
```

## File Naming

Backup files follow the pattern:
```
{job}_{YYYYMMDD}_{HHMMSS}_{engine}.{ext}
```

Examples:
- `app-db_20260415_030000_postgres.dump`
- `blog-db_20260415_030000_mysql.sql.gz`
- `dev-db_20260415_030000_sqlite.db.gz`
- `uploads_20260415_030000_files.tar.gz`
