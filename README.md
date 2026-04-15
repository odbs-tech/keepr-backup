# keepr

Database & file backup manager with SSH and S3 support.

Backup PostgreSQL, MySQL, and SQLite databases alongside file directories. Connect directly to databases or via SSH to remote servers. Store backups on local disk, remote server, or S3.

## Features

- **Multi-engine database support** — PostgreSQL (`pg_dump`), MySQL (`mysqldump`), SQLite
- **File & directory backups** — `tar.gz` with exclude patterns
- **Direct or SSH connection** — Connect to DB directly or SSH into the server
- **Flexible storage** — Local disk, remote server disk, and/or S3
- **Interactive CLI** — Add databases and servers with guided prompts
- **Retention policies** — Automatic cleanup per destination
- **Backup catalog** — Track all backups with metadata, sizes, and checksums
- **Restore support** — Restore any backup from any destination
- **Cron ready** — Generate crontab entries for scheduled backups

## Quick Start

```bash
# Install
pip install -e .

# Create config
keepr init

# Add a database backup
keepr db add myapp-db

# Add a file backup
keepr files add uploads

# Run all backups
keepr run

# List taken backups
keepr list
```

## Adding Backups

### Database backup

```bash
keepr db add cortex-db
```

Interactive prompts guide you through:

```
Engine (postgres/mysql/sqlite): postgres

Baglanti yontemi:
  1) Direkt — pg_dump burada calisir, DB'ye uzaktan baglanir
  2) SSH — sunucuya baglanip orada dump calistirir
Sec: 1

DB host: 159.69.42.100
DB port [5432]:
DB name: cortex
DB user [postgres]:

Backup nereye kaydedilsin?
  1) local — bu makinede ~/backups/
  2) s3 — S3 bucket'a yukle
  3) local + s3

[OK] Added: cortex-db (postgres @ 159.69.42.100)
```

**SSH connection** — If you choose SSH, you can select an existing server or create one inline:

```
Sec: 2

  1) production (root@159.69.42.100)
  2) + Yeni server ekle
Server sec: 1

DB host (sunucu uzerinde) [localhost]:
DB name: cortex

Backup nereye kaydedilsin?
  1) local
  2) s3
  3) server — sunucunun diskinde birak
  4) local + s3
  5) server + s3
  6) local + server + s3
```

### File backup

```bash
keepr files add uploads
```

```
Dosyalar nerede?
  1) Bu makine (lokal)
  2) Uzak sunucu (SSH)
Sec: 2

Server sec: 1) production

Backup alinacak dizinler: /var/www/app/uploads
Haric tutulacak pattern'ler: *.tmp, .cache

[OK] Added: uploads (files @ production)
```

### S3 setup

First time you select S3 as a destination, keepr prompts for setup:

```
S3 henuz ayarlanmamis. Simdi ayarlayalim:
  Bucket: my-backups
  Region [eu-central-1]:
  Prefix [keepr/]:
  Access Key ID:
  Secret Access Key:
[OK] S3 ayarlandi
```

## Connection Types

| Type | How it works | When to use |
|------|-------------|-------------|
| **Direct** | `pg_dump -h remote_host` runs on your machine | DB port is accessible, pg_dump installed locally |
| **SSH** | `ssh server "pg_dump"` runs on the remote server | DB port not exposed, more secure |

## Destinations

| Destination | Description |
|-------------|-------------|
| `local` | keepr's machine (`~/backups/`) |
| `server` | Remote server's disk (SSH only) |
| `s3` | S3-compatible object storage |

## CLI Reference

### Backup management
```
keepr db add <name>           Add a database backup (interactive)
keepr db list                 List configured database backups
keepr db remove <name>        Remove a database backup

keepr files add <name>        Add a file/directory backup (interactive)
keepr files list              List configured file backups
keepr files remove <name>     Remove a file backup
```

### Server management
```
keepr server add <name>       Add an SSH server
keepr server list             List SSH servers
keepr server remove <name>    Remove an SSH server
```

### Operations
```
keepr run [NAME...]           Run backups (all if none specified)
keepr run --dry-run           Preview without executing
keepr list [NAME]             List taken backups
keepr restore <BACKUP_ID>     Restore a backup
keepr delete <BACKUP_ID>      Delete a backup
keepr cleanup [NAME]          Apply retention policies
keepr cron                    Print crontab entries
keepr config                  Show current configuration
keepr init                    Create config file
```

## How It Works

### Direct connection
```
[Your machine] ---pg_dump -h remote_host---> [DB]
       |
       v
  ~/backups/ and/or S3
```

### SSH connection
```
[Your machine] --SSH--> [Server] --pg_dump--> stream --> [Your machine]
                                                    \--> [S3]
                                          or  \--> [Server disk]
```

### Retention

After each backup, keepr automatically applies retention policies. Per destination:
- `keep_local: 7` — keep last 7 on local disk
- `keep_s3: 30` — keep last 30 on S3
- `keep_server: 5` — keep last 5 on server

Override per backup in the config or during `keepr db add`.

## Scheduling

```bash
keepr cron
```

Outputs ready-to-use cron lines:
```cron
0 3 * * * keepr run --all >> /var/log/keepr.log 2>&1
0 5 * * * keepr cleanup >> /var/log/keepr.log 2>&1
```

## File Naming

```
{name}_{YYYYMMDD}_{HHMMSS}_{engine}.{ext}
```

Examples:
- `cortex-db_20260415_030000_postgres.dump`
- `blog-db_20260415_030000_mysql.sql.gz`
- `dev-db_20260415_030000_sqlite.sql.gz`
- `uploads_20260415_030000_files.tar.gz`

## Configuration

Config file: `~/.config/keepr/keepr.yml`

You rarely need to edit this manually — use `keepr db add`, `keepr files add`, and `keepr server add` instead. But here's the full format for reference:

```yaml
storage:
  local_dir: ~/backups
  server_dir: /var/backups/keepr
  s3:
    bucket: my-backups
    region: eu-central-1
    prefix: keepr/

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
  production:
    host: 159.69.42.100
    user: root
    port: 22

jobs:
  cortex-db:
    server: production
    type: database
    engine: postgres
    database:
      name: cortex
      user: postgres
      host: localhost
      port: 5432
    destinations: [local, s3]
```
