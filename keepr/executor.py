from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from keepr.config import ServerConfig
from keepr import output


class Executor:
    """Runs commands locally or over SSH."""

    def __init__(self, server: ServerConfig):
        self.server = server

    def run(
        self,
        cmd: str,
        env: dict[str, str] | None = None,
        capture_output: bool = True,
    ) -> subprocess.CompletedProcess:
        if self.server.is_local:
            return self._run_local(cmd, env=env, capture_output=capture_output)
        return self._run_ssh(cmd, env=env, capture_output=capture_output)

    def run_stream_to_file(
        self,
        cmd: str,
        output_path: Path,
        env: dict[str, str] | None = None,
    ) -> None:
        """Run a command and stream stdout to a local file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if self.server.is_local:
            full_cmd = self._with_env(cmd, env)
            with open(output_path, "wb") as f:
                proc = subprocess.run(
                    full_cmd, shell=True, stdout=f, stderr=subprocess.PIPE, text=False
                )
            if proc.returncode != 0:
                stderr = proc.stderr.decode() if proc.stderr else ""
                raise RuntimeError(f"Command failed (exit {proc.returncode}): {stderr}")
        else:
            ssh_cmd = self._build_ssh_command(self._with_env(cmd, env))
            with open(output_path, "wb") as f:
                proc = subprocess.run(
                    ssh_cmd, stdout=f, stderr=subprocess.PIPE, text=False
                )
            if proc.returncode != 0:
                stderr = proc.stderr.decode() if proc.stderr else ""
                raise RuntimeError(f"SSH command failed (exit {proc.returncode}): {stderr}")

    def run_on_server(
        self,
        cmd: str,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess:
        """Run a command on the server (local or SSH), output stays on server."""
        full_cmd = self._with_env(cmd, env)
        if self.server.is_local:
            return subprocess.run(
                full_cmd, shell=True,
                capture_output=True, text=True, check=True,
            )
        ssh_cmd = self._build_ssh_command(full_cmd)
        return subprocess.run(ssh_cmd, capture_output=True, text=True, check=True)

    def download(self, remote_path: str, local_path: Path) -> None:
        """Download a file from the remote server via SCP."""
        if self.server.is_local:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(remote_path, local_path)
            return

        local_path.parent.mkdir(parents=True, exist_ok=True)
        scp_cmd = ["scp", "-P", str(self.server.port)]
        scp_cmd += self._ssh_key_args()
        scp_cmd += [
            f"{self.server.user}@{self.server.host}:{remote_path}",
            str(local_path),
        ]
        subprocess.run(scp_cmd, check=True, capture_output=True)

    def get_file_size(self, remote_path: str) -> int:
        """Get file size on the server."""
        result = self.run(f"stat -c%s {remote_path} 2>/dev/null || stat -f%z {remote_path}")
        return int(result.stdout.strip())

    def _run_local(
        self, cmd: str, env: dict[str, str] | None = None, capture_output: bool = True,
    ) -> subprocess.CompletedProcess:
        import os
        full_env = None
        if env:
            full_env = {**os.environ, **env}
        return subprocess.run(
            cmd, shell=True, capture_output=capture_output,
            text=True, check=True, env=full_env,
        )

    def _run_ssh(
        self, cmd: str, env: dict[str, str] | None = None, capture_output: bool = True,
    ) -> subprocess.CompletedProcess:
        full_cmd = self._with_env(cmd, env)
        ssh_cmd = self._build_ssh_command(full_cmd)
        return subprocess.run(
            ssh_cmd, capture_output=capture_output, text=True, check=True,
        )

    def _build_ssh_command(self, remote_cmd: str) -> list[str]:
        ssh = ["ssh", "-p", str(self.server.port)]
        ssh += ["-o", "StrictHostKeyChecking=accept-new"]
        ssh += ["-o", "ConnectTimeout=10"]
        ssh += self._ssh_key_args()
        ssh += [f"{self.server.user}@{self.server.host}", remote_cmd]
        return ssh

    def _ssh_key_args(self) -> list[str]:
        if self.server.ssh_key:
            return ["-i", self.server.ssh_key]
        return []

    @staticmethod
    def _with_env(cmd: str, env: dict[str, str] | None) -> str:
        if not env:
            return cmd
        prefix = " ".join(f"{k}={v}" for k, v in env.items())
        return f"{prefix} {cmd}"
