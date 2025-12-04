"""
GeoRiva FTP/SFTP Loader

Loader implementation for FTP, SFTP, and FTPS protocols.
Uses ftplib for FTP/FTPS and paramiko for SFTP.
"""

import fnmatch
import re
from datetime import datetime
from ftplib import FTP, FTP_TLS
from pathlib import Path
from typing import Iterator

from .base import BaseLoader, FetchResult, RemoteFile


class FTPLoader(BaseLoader):
    """
    Loader for FTP, SFTP, and FTPS sources.
    
    Supports:
    - Plain FTP
    - FTPS (FTP over TLS)
    - SFTP (SSH File Transfer Protocol)
    - Glob and regex filename patterns
    - Recursive directory scanning
    - Passive/active mode for FTP
    """
    
    def __init__(self, config, collection) -> None:
        super().__init__(config, collection)
        self._connection = None
        self._sftp = None
        self._ssh = None
    
    @property
    def protocol(self) -> str:
        return self.config.protocol
    
    @property
    def is_sftp(self) -> bool:
        return self.protocol == 'sftp'
    
    # =========================================================================
    # Connection Management
    # =========================================================================
    
    def connect(self) -> None:
        """Establish connection based on protocol."""
        self.logger.info(
            f"Connecting to {self.config.host}:{self.config.port} "
            f"via {self.protocol.upper()}"
        )
        
        if self.is_sftp:
            self._connect_sftp()
        else:
            self._connect_ftp()
    
    def _connect_ftp(self) -> None:
        """Connect via FTP or FTPS."""
        try:
            # Create connection
            if self.protocol == 'ftps':
                self._connection = FTP_TLS()
            else:
                self._connection = FTP()
            
            # Set timeout
            self._connection.connect(
                host=self.config.host,
                port=self.config.port,
                timeout=self.config.timeout,
            )
            
            # Login
            if self.config.username:
                self._connection.login(
                    user=self.config.username,
                    passwd=self.config.password or '',
                )
            else:
                self._connection.login()
            
            # FTPS: Switch to secure data connection
            if self.protocol == 'ftps':
                self._connection.prot_p()
            
            # Set passive mode
            if self.config.passive_mode:
                self._connection.set_pasv(True)
            
            self.logger.info(f"Connected: {self._connection.getwelcome()}")
        
        except Exception as e:
            self.logger.error(f"FTP connection failed: {e}")
            raise ConnectionError(f"Failed to connect to {self.config.host}: {e}")
    
    def _connect_sftp(self) -> None:
        """Connect via SFTP using paramiko."""
        try:
            import paramiko
        except ImportError:
            raise ImportError("paramiko is required for SFTP. Install with: pip install paramiko")
        
        try:
            # Create SSH client
            self._ssh = paramiko.SSHClient()
            self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Prepare connection kwargs
            connect_kwargs = {
                'hostname': self.config.host,
                'port': self.config.port,
                'username': self.config.username,
                'timeout': self.config.timeout,
            }
            
            # Authentication method
            if self.config.private_key:
                # Use private key
                from io import StringIO
                key_file = StringIO(self.config.private_key)
                
                # Try different key types
                for key_class in [paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey]:
                    try:
                        key_file.seek(0)
                        pkey = key_class.from_private_key(key_file)
                        connect_kwargs['pkey'] = pkey
                        break
                    except paramiko.SSHException:
                        continue
                else:
                    raise ValueError("Could not parse private key")
            else:
                # Use password
                connect_kwargs['password'] = self.config.password
            
            # Connect
            self._ssh.connect(**connect_kwargs)
            
            # Open SFTP session
            self._sftp = self._ssh.open_sftp()
            
            self.logger.info(f"SFTP connected to {self.config.host}")
        
        except Exception as e:
            self.logger.error(f"SFTP connection failed: {e}")
            if self._ssh:
                self._ssh.close()
            raise ConnectionError(f"Failed to connect via SFTP to {self.config.host}: {e}")
    
    def disconnect(self) -> None:
        """Close connection."""
        if self._sftp:
            try:
                self._sftp.close()
            except Exception:
                pass
            self._sftp = None
        
        if self._ssh:
            try:
                self._ssh.close()
            except Exception:
                pass
            self._ssh = None
        
        if self._connection:
            try:
                self._connection.quit()
            except Exception:
                try:
                    self._connection.close()
                except Exception:
                    pass
            self._connection = None
        
        self.logger.debug("Disconnected")
    
    # =========================================================================
    # File Listing
    # =========================================================================
    
    def list_files(self) -> Iterator[RemoteFile]:
        """List files matching the configured pattern."""
        remote_path = self.config.remote_path.rstrip('/')
        
        if self.is_sftp:
            yield from self._list_files_sftp(remote_path)
        else:
            yield from self._list_files_ftp(remote_path)
    
    def _list_files_ftp(self, path: str) -> Iterator[RemoteFile]:
        """List files via FTP."""
        try:
            # Try MLSD first (modern, provides metadata)
            yield from self._list_mlsd(path)
        except Exception:
            # Fall back to LIST
            yield from self._list_nlst(path)
    
    def _list_mlsd(self, path: str) -> Iterator[RemoteFile]:
        """List using MLSD command (RFC 3659)."""
        try:
            entries = list(self._connection.mlsd(path))
        except Exception as e:
            self.logger.debug(f"MLSD not supported: {e}")
            raise
        
        for name, facts in entries:
            if name in ('.', '..'):
                continue
            
            full_path = f"{path}/{name}"
            file_type = facts.get('type', 'file')
            
            if file_type == 'dir':
                if self.config.recursive:
                    yield from self._list_mlsd(full_path)
                continue
            
            if not self._matches_pattern(name):
                continue
            
            # Parse modification time
            modify_str = facts.get('modify', '')
            modified = None
            if modify_str:
                try:
                    modified = datetime.strptime(modify_str[:14], '%Y%m%d%H%M%S')
                except ValueError:
                    pass
            
            yield RemoteFile(
                path=full_path,
                filename=name,
                size=int(facts.get('size', 0)) if facts.get('size') else None,
                modified=modified,
            )
    
    def _list_nlst(self, path: str) -> Iterator[RemoteFile]:
        """List using NLST command (legacy fallback)."""
        try:
            self._connection.cwd(path)
            names = self._connection.nlst()
        except Exception as e:
            self.logger.error(f"Failed to list {path}: {e}")
            return
        
        for name in names:
            if name in ('.', '..'):
                continue
            
            full_path = f"{path}/{name}"
            
            # Check if directory
            is_dir = False
            try:
                self._connection.cwd(full_path)
                self._connection.cwd(path)  # Go back
                is_dir = True
            except Exception:
                pass
            
            if is_dir:
                if self.config.recursive:
                    yield from self._list_nlst(full_path)
                continue
            
            if not self._matches_pattern(name):
                continue
            
            # Try to get size
            size = None
            try:
                size = self._connection.size(full_path)
            except Exception:
                pass
            
            yield RemoteFile(
                path=full_path,
                filename=name,
                size=size,
            )
    
    def _list_files_sftp(self, path: str) -> Iterator[RemoteFile]:
        """List files via SFTP."""
        try:
            entries = self._sftp.listdir_attr(path)
        except IOError as e:
            self.logger.error(f"Failed to list {path}: {e}")
            return
        
        import stat
        
        for entry in entries:
            if entry.filename in ('.', '..'):
                continue
            
            full_path = f"{path}/{entry.filename}"
            
            # Check if directory
            if stat.S_ISDIR(entry.st_mode):
                if self.config.recursive:
                    yield from self._list_files_sftp(full_path)
                continue
            
            if not self._matches_pattern(entry.filename):
                continue
            
            # Parse modification time
            modified = None
            if entry.st_mtime:
                modified = datetime.fromtimestamp(entry.st_mtime)
            
            yield RemoteFile(
                path=full_path,
                filename=entry.filename,
                size=entry.st_size,
                modified=modified,
            )
    
    def _matches_pattern(self, filename: str) -> bool:
        """Check if filename matches configured pattern."""
        pattern = self.config.filename_pattern
        
        if not pattern or pattern == '*':
            return True
        
        if self.config.use_regex:
            try:
                return bool(re.match(pattern, filename))
            except re.error:
                self.logger.warning(f"Invalid regex pattern: {pattern}")
                return True
        else:
            return fnmatch.fnmatch(filename, pattern)
    
    # =========================================================================
    # File Fetching
    # =========================================================================
    
    def fetch_file(self, remote_file: RemoteFile, local_path: Path) -> FetchResult:
        """Download a file."""
        result = FetchResult(remote_file=remote_file, local_path=local_path)
        
        try:
            # Ensure parent directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            if self.is_sftp:
                self._fetch_sftp(remote_file.path, local_path)
            else:
                self._fetch_ftp(remote_file.path, local_path)
            
            result.success = True
            result.bytes_transferred = local_path.stat().st_size
        
        except Exception as e:
            self.logger.error(f"Failed to fetch {remote_file.path}: {e}")
            result.success = False
            result.error = str(e)
        
        return result
    
    def _fetch_ftp(self, remote_path: str, local_path: Path) -> None:
        """Download via FTP."""
        with open(local_path, 'wb') as f:
            self._connection.retrbinary(f'RETR {remote_path}', f.write)
    
    def _fetch_sftp(self, remote_path: str, local_path: Path) -> None:
        """Download via SFTP."""
        self._sftp.get(remote_path, str(local_path))
