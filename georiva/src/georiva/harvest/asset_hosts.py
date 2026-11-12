"""
The Asset host allowlist: Upstream decides what we index, never where our servers connect.

An asset href is upstream content, and it is later opened from inside our
network — by Titiler for a tile, by zonal statistics for a read. So before a
harvest stores one it must be on `https` (plain `http` only where the operator
allowed it), name a host the feed allows, and resolve only to public addresses
(ADR 0042 §9). This check at harvest time is the guarantee; nothing downstream
repeats it.
"""

from __future__ import annotations

import ipaddress
from collections.abc import Callable, Iterable
from urllib.parse import urlsplit

#: `Asset.href`'s column width: a longer href cannot be stored whole, and a
#: truncated one names something else.
MAX_HREF_LENGTH = 500


def _is_public(address: str) -> bool:
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return False
    # An IPv4 address written as IPv6 is judged as the IPv4 address it reaches.
    if ip.version == 6 and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    # Not global covers private, loopback, link-local, shared (CGNAT), reserved and unspecified.
    return ip.is_global and not ip.is_multicast


class AssetHostAllowlist:
    """Says whether one feed may store an asset href, and if not, why."""

    def __init__(self, hosts: Iterable[str], *, allow_http: bool, resolve: Callable[[str], list[str]]):
        self.hosts = {host.lower().rstrip(".") for host in hosts if host}
        self.allow_http = allow_http
        self._resolve = resolve
        self._resolved: dict[str, str | None] = {}

    def refusal(self, href: str) -> str | None:
        """Why `href` may not be stored, or `None` if it may."""
        if len(href) > MAX_HREF_LENGTH:
            return f"href is longer than {MAX_HREF_LENGTH} characters"
        try:
            parts = urlsplit(href)
            host = (parts.hostname or "").lower().rstrip(".")
        except ValueError:
            return "href is not a URL"
        if parts.scheme != "https" and not (parts.scheme == "http" and self.allow_http):
            return f"scheme {parts.scheme or 'missing'!r} is not allowed"
        if not host:
            return "href names no host"
        if host not in self.hosts:
            return f"host {host} is not in the asset host allowlist"
        return self._address_refusal(host)

    def _address_refusal(self, host: str) -> str | None:
        """A host is asked about once per harvest, however many assets it serves."""
        if host not in self._resolved:
            self._resolved[host] = self._judge(host)
        return self._resolved[host]

    def _judge(self, host: str) -> str | None:
        try:
            addresses = [str(ipaddress.ip_address(host))]
        except ValueError:
            addresses = self._resolve(host)
        if not addresses:
            return f"host {host} does not resolve"
        if not all(_is_public(address) for address in addresses):
            return f"host {host} resolves to a private, loopback or link-local address"
        return None
