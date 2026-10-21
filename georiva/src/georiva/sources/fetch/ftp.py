"""
GeoRiva HTTP Fetch Strategy

For direct HTTP/HTTPS downloads
"""

import errno
import ftplib
import socket
import time
from pathlib import Path

from .base import BaseFetchStrategy, FailureScope, FetchMode, FetchResult, FileRequest


class _FileTimeout(Exception):
    """Raised from the transfer callback when a file outlasts max_file_seconds."""


class FTPFetchStrategy(BaseFetchStrategy):
    """
    Fetch strategy for FTP/SFTP/FTPS.

    Wraps your existing FTP loader logic into the new architecture.
    """

    type = "ftp"
    label = "FTP/SFTP/FTPS"

    def __init__(self, config: dict):
        """
        Initialize FTP fetch strategy.

        Config options:
            protocol: 'ftp', 'ftps', or 'sftp'
            host: Server hostname
            port: Server port (default: 21 for FTP, 22 for SFTP)
            username: Login username
            password: Login password
            private_key: SSH private key (for SFTP)
            passive_mode: Use passive mode (default: True)
            timeout: Connection timeout (default: 30)
            max_file_seconds: Wall-clock limit for one file (default: 900)
        """
        super().__init__(config)

        self.protocol = config.get("protocol", "ftp")
        self.host = config["host"]
        self.port = config.get("port", 22 if self.protocol == "sftp" else 21)
        self.username = config.get("username")
        self.password = config.get("password")
        self.private_key = config.get("private_key")
        self.passive_mode = config.get("passive_mode", True)
        self.timeout = config.get("timeout", 30)

        self._connection = None
        self._sftp = None

    @property
    def mode(self) -> FetchMode:
        return FetchMode.SYNC

    def connect(self) -> None:
        """Establish FTP/SFTP connection."""
        if self.protocol == "sftp":
            self._connect_sftp()
        else:
            self._connect_ftp()

    def _connect_ftp(self) -> None:
        """Connect via FTP/FTPS."""
        from ftplib import FTP, FTP_TLS

        if self.protocol == "ftps":
            self._connection = FTP_TLS()
        else:
            self._connection = FTP()

        self._connection.connect(
            host=self.host,
            port=self.port,
            timeout=self.timeout,
        )

        if self.username:
            self._connection.login(self.username, self.password or "")
        else:
            self._connection.login()

        if self.protocol == "ftps":
            self._connection.prot_p()

        if self.passive_mode:
            self._connection.set_pasv(True)

        self.logger.info(f"Connected to {self.host}")

    def _connect_sftp(self) -> None:
        """Connect via SFTP."""
        import paramiko

        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs = {
            "hostname": self.host,
            "port": self.port,
            "username": self.username,
            "timeout": self.timeout,
        }

        if self.private_key:
            from io import StringIO

            key_file = StringIO(self.private_key)
            for key_class in [paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey]:
                try:
                    key_file.seek(0)
                    connect_kwargs["pkey"] = key_class.from_private_key(key_file)
                    break
                except paramiko.SSHException:
                    continue
        else:
            connect_kwargs["password"] = self.password

        self._ssh.connect(**connect_kwargs)
        self._sftp = self._ssh.open_sftp()

        self.logger.info(f"Connected to {self.host} via SFTP")

    def disconnect(self) -> None:
        """Close connection."""
        if self._sftp:
            self._sftp.close()
            self._sftp = None

        if hasattr(self, "_ssh") and self._ssh:
            self._ssh.close()
            self._ssh = None

        if self._connection:
            try:
                self._connection.quit()
            except Exception:
                self._connection.close()
            self._connection = None

    def _reset_connection(self) -> None:
        """Reconnect after an aborted transfer; a failure to do so surfaces on the next fetch."""
        try:
            self.disconnect()
            self.connect()
        except Exception as e:
            self.logger.warning(f"Reconnect after aborted transfer failed: {e}")

    def fetch(self, request: FileRequest, local_path: Path) -> FetchResult:
        """Download file via FTP/SFTP. A failed result carries its failure scope (#477)."""
        result = FetchResult(request=request, local_path=local_path)

        remote_path = request.params.get("remote_path")
        if not remote_path:
            return result.fail("Missing remote_path in params", FailureScope.FILE)

        # Open the local file on its own first, so a failure here is plainly ours.
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_file = open(local_path, "wb")
        except OSError as e:
            self.logger.error(f"Cannot write {local_path}: {e}")
            return result.fail(f"IO error writing file: {e}", FailureScope.SOURCE)

        start_time = time.time()
        deadline = self.clock() + self.max_file_seconds

        def write(data: bytes) -> None:
            if self.clock() > deadline:
                raise _FileTimeout()
            local_file.write(data)

        try:
            with local_file:
                if self.protocol == "sftp":
                    self._sftp.getfo(remote_path, local_file, callback=lambda done, total: write(b""))
                else:
                    self._connection.retrbinary(f"RETR {remote_path}", write)

            result.success = True
            result.status = "complete"
            result.bytes_transferred = local_path.stat().st_size
            result.duration_seconds = time.time() - start_time

        except _FileTimeout:
            local_path.unlink(missing_ok=True)
            self.logger.error(f"FTP download of {remote_path} exceeded {self.max_file_seconds}s")
            # The control connection is mid-transfer and unusable for the next file.
            self._reset_connection()
            result.fail(self.file_timeout_error(), FailureScope.TRANSIENT)

        except Exception as e:
            self.logger.error(f"FTP download failed: {e}")
            result.fail(str(e), _classify_error(e))

        return result


# Login refused, or an account needed: the server will not serve us at all.
_FTP_REFUSED_REPLIES = ("530", "332")
# Our side cannot take the bytes: every file after this one would fail too.
_LOCAL_STORAGE_ERRNOS = {errno.ENOSPC, errno.EDQUOT, errno.EROFS}


def _classify_error(exc: BaseException) -> FailureScope:
    """The failure scope of an exception raised while transferring one file."""
    if isinstance(exc, ftplib.error_perm):
        return FailureScope.SOURCE if str(exc)[:3] in _FTP_REFUSED_REPLIES else FailureScope.FILE
    if isinstance(exc, (socket.gaierror, ConnectionRefusedError)):
        return FailureScope.SOURCE
    if isinstance(exc, OSError) and exc.errno in _LOCAL_STORAGE_ERRNOS:
        return FailureScope.SOURCE
    # Timeouts, dropped connections, 4xx replies (error_temp) and anything else
    # unrecognised: ambiguous.
    return FailureScope.TRANSIENT
