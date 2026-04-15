from __future__ import annotations

from rich.console import Console
from rich.table import Table
from rich.theme import Theme


theme = Theme({
    "info": "cyan",
    "success": "green",
    "warning": "yellow",
    "error": "red bold",
    "muted": "dim",
})

console = Console(theme=theme)


def info(msg: str) -> None:
    console.print(f"  [info][>][/info] {msg}")


def success(msg: str) -> None:
    console.print(f"  [success][OK][/success] {msg}")


def warning(msg: str) -> None:
    console.print(f"  [warning][!][/warning] {msg}")


def error(msg: str) -> None:
    console.print(f"  [error][X][/error] {msg}")


def header() -> None:
    from keepr import __version__
    console.print(f"\n  [bold]keepr[/bold] [muted]v{__version__}[/muted]\n")


def make_table(*columns: str) -> Table:
    table = Table(show_header=True, header_style="bold", padding=(0, 2))
    for col in columns:
        table.add_column(col)
    return table


def format_size(size_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"
