# keepr

Database & file backup manager with SSH and S3 support.

Backup PostgreSQL, MySQL, and SQLite databases alongside file directories — or both at once. Connect directly to databases or via SSH to remote servers. Store backups on local disk, remote server, or S3.

## Features

- **Multi-engine database support** — PostgreSQL (`pg_dump`), MySQL (`mysqldump`), SQLite
- **File & directory backups** — `tar.gz` with exclude patterns
- **Combined jobs** — Backup database + files together in a single job
- **Direct or SSH connection** — Connect to DB directly or SSH into the server
- **Flexible storage** — Local disk, remote server disk, and/or S3
- **Interactive setup** — Guided wizard for initial configuration
- **Retention policies** — Automatic cleanup per destination
- **Restore support** — Restore any backup from any destination
- **Cron ready** — Generate crontab entries for scheduled backups

## Installation

```bash
# With pipx (recommended)
pipx install git+https://github.com/odbs-tech/keepr-backup.git

# Or from source
git clone https://github.com/odbs-tech/keepr-backup.git
cd keepr-backup
pip install -e .
```

Update:
```bash
pipx upgrade keepr
```

## Quick Start

```bash
# Interactive setup — walks you through everything
keepr init
```

The setup wizard guides you through:

```
Welcome to keepr!

1. Storage
   Local backup path [~/backups]:
   Upload backups to S3? [y/N]:

2. Servers
   Add a remote server? [y/N]: y
   Name: production
   Host: 159.69.42.100
   SSH user [root]:

3. Backup jobs
   Add a backup job? [Y/n]: y
   Name: cortex
   Server: 1) production
   What to backup?
     1) Database
     2) Files
     3) Database + Files
   Select: 3

   -- Database --
   Engine: postgres
   DB name: cortex
   DB user [postgres]:

   -- Files --
   Directories: /var/www/cortex/uploads
   Exclude patterns: *.tmp

   Where to save?
     1) local  2) s3  3) server  4) local+s3  5) server+s3  6) all
   Select: 4

   Add another job? [y/N]: n

Setup complete! 1 job(s) configured.
Run 'keepr run' to start backing up.
```

## Adding More Jobs

```bash
keepr job add my-other-db
```

Same interactive flow — pick server, choose database/files/both, set destinations.

## CLI Reference

### Setup & config
```
keepr init                    Interactive setup wizard
keepr config                  Show current configuration
```

### Job management
```
keepr job add <name>          Add a backup job (interactive)
keepr job list                List all jobs
keepr job remove <name>       Remove a job
```

### Server management
```
keepr server add <name>       Add an SSH server
keepr server list             List SSH servers
keepr server remove <name>    Remove a server
```

### Backup operations
```
keepr run [NAME...]           Run backups (all if none specified)
keepr run --dry-run           Preview without executing
keepr list [NAME]             List taken backups
keepr restore <BACKUP_ID>     Restore a backup
keepr delete <BACKUP_ID>      Delete a backup
keepr cleanup [NAME]          Apply retention policies
keepr cron                    Print crontab entries
```

## Job Types

A job can backup a database, files, or both:

```yaml
jobs:
  # Database + Files together
  cortex:
    server: production
    engine: postgres
    database:
      name: cortex
      user: postgres
    files:
      paths: [/var/www/cortex/uploads]
      exclude: ["*.tmp"]
    destinations: [local, s3]

  # Database only
  quickbill-db:
    server: production
    engine: mysql
    database:
      name: quickbill
      user: root
    destinations: [local, s3]

  # Files only
  logs:
    server: production
    files:
      paths: [/var/log/app]
    destinations: [local]
```

## Connection Types

| Type | How it works | When to use |
|------|-------------|-------------|
| **Direct** | `pg_dump -h remote_host` runs on your machine | DB port is accessible |
| **SSH** | `ssh server "pg_dump"` runs on the remote server | DB port not exposed, more secure |

If the selected server is remote (SSH), dump runs on the server.
If the server is `local`, dump runs on your machine (direct connection).

## Destinations

| Destination | Description |
|-------------|-------------|
| `local` | keepr's machine (`~/backups/`) |
| `server` | Remote server's disk (SSH only) |
| `s3` | S3-compatible object storage |

## Retention

After each backup, keepr automatically cleans up old backups per destination:
- `keep_local: 7` — keep last 7 on local disk
- `keep_s3: 30` — keep last 30 on S3
- `keep_server: 5` — keep last 5 on server

Override per job during setup or in the config file.

## Scheduling

```bash
keepr cron
```

Outputs cron lines for `crontab -e`:
```cron
0 3 * * * keepr run --all >> /var/log/keepr.log 2>&1
0 5 * * * keepr cleanup >> /var/log/keepr.log 2>&1
```

## File Naming

```
{job}_{YYYYMMDD}_{HHMMSS}_{engine}.{ext}
```

- `cortex_20260415_030000_postgres.dump`
- `cortex_20260415_030000_files.tar.gz`
- `quickbill-db_20260415_030000_mysql.sql.gz`
