"""
The Asset host allowlist: Upstream decides what we index, never where our servers connect.

An asset href is upstream content, and it is later opened from inside our
network — by Titiler for a tile, by zonal statistics for a read. So before a
harvest stores one it must be on `https` (plain `http` only where the operator
allowed it), name a host the feed allows, and resolve only to public addresses
(ADR 0042 §9). This check at harvest time is the guarantee; nothing downstream
repeats it.

An Upstream that names a public bucket by `s3://bucket/key` is not naming an
address we may open: inside our deployment `s3://` is our own object store. Such
an href is first translated to the public `https` host a person pinned for that
bucket, and the translation is what is judged and stored.
"""

from __future__ import annotations

import ipaddress
from collections.abc import Callable, Iterable, Mapping
from urllib.parse import quote, urlsplit

from georiva.core.storage.asset_location import AssetLocation

#: `Asset.href`'s column width: a longer href cannot be stored whole, and a
#: truncated one names something else.
MAX_HREF_LENGTH = 500


#: NAT64's well-known prefix: an IPv6 address here reaches the IPv4 address in its last 32 bits.
_NAT64 = ipaddress.ip_network("64:ff9b::/96")


def _reached(ip):
    """The addresses a connection to `ip` can end at: itself, and any IPv4
    address an IPv6 one is only a wrapping for."""
    if ip.version == 4:
        return [ip]
    wrapped = [ip.ipv4_mapped, ip.sixtofour]
    if ip in _NAT64:
        wrapped.append(ipaddress.ip_address(int(ip) & 0xFFFFFFFF))
    return [ip, *(inner for inner in wrapped if inner is not None)]


def _is_public(address: str) -> bool:
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return False
    # Not global covers private, loopback, link-local, shared (CGNAT), reserved and unspecified.
    return all(reached.is_global and not reached.is_multicast for reached in _reached(ip))


_S3 = "s3://"


def _is_s3(href: str) -> bool:
    return href[: len(_S3)].lower() == _S3


class PinnedBuckets:
    """A feed's Pinned locations of the bucket kind: `s3://` bucket → the public
    `https` host its files are read at."""

    def __init__(self, hosts_by_bucket: Mapping[str, str]):
        self._hosts = {bucket.lower(): host for bucket, host in hosts_by_bucket.items() if bucket and host}

    def locate(self, href: str) -> str:
        """`href` as it may be stored: an `s3://` href in a pinned bucket as its
        `https` address, any other href untouched. An `s3://` href that comes
        back as it went in names nowhere pinned, and the allowlist refuses it."""
        if not _is_s3(href):
            return href
        bucket, _, key = href[len(_S3) :].partition("/")
        host = self._hosts.get(bucket.lower())
        if host is None or not key:
            return href
        # An s3 href spells the key raw; a URL spells it escaped.
        return f"https://{host}/{quote(key, safe='/=')}"


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
        # What is judged must be what is stored and later opened: a parser that
        # forgives stray whitespace or a backslash would pass a string that the
        # next reader of it takes for something else.
        if any(char.isspace() or not char.isprintable() or char == "\\" for char in href):
            return "href contains whitespace, a control character or a backslash"
        if _is_s3(href):
            return "href names an s3 bucket, or nothing in one, that is not a pinned location of this feed"
        if not AssetLocation(href).is_remote:
            return "href is not a URL"
        try:
            parts = urlsplit(href)
            host = (parts.hostname or "").lower().rstrip(".")
        except ValueError:
            return "href is not a URL"
        if "@" in parts.netloc:
            return "href carries credentials"
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
