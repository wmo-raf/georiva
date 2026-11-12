"""Where an asset's bytes are (ADR 0043 §1).

An ``Asset.href`` names one of two things. Usually it is a key in our assets
bucket, written by ingestion or a derivation. For a **Remote asset** (ADR 0042)
it is an Upstream URL: the bytes were never copied, and the href is the only
way to them.

This module is the one place that tells the two apart. Everything that reads an
``Asset.href`` — the public URL, the STAC serializer, zonal statistics, virtual
Zarr, timeseries, the handoff of a Remote asset's href to Titiler
(``machine_plane.remote_assets``), the delete that follows a row out and the
orphan sweep — asks it, rather than looking at the string itself.
``StagingAsset`` hrefs are always keys on the staging bucket and are not its
concern. "Still points upstream" in
ADR 0042's jurisdiction rule is :attr:`AssetLocation.is_remote`, not a second
definition.

The test is the URL scheme. Every key we write starts with the owning
organisation's slug, and a slug cannot contain ``://``; a URL always does,
straight after its scheme. So the scheme is decisive without a column, and the
database can answer the same question the Python does (:func:`remote_q`).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from django.db.models import Q

from .manager import storage

#: An RFC 3986 scheme followed by ``://`` — what a URL opens with and no key we
#: write can. Written once for Python and for Postgres's ``~`` alike.
_REMOTE_PATTERN = r"^[A-Za-z][A-Za-z0-9+.-]*://"
_REMOTE_RE = re.compile(_REMOTE_PATTERN)


def remote_q(prefix: str = "") -> Q:
    """Rows whose asset is Remote. ``prefix`` is the path to the asset, e.g. ``"assets__"``."""
    return Q(**{f"{prefix}href__regex": _REMOTE_PATTERN})


def local_q(prefix: str = "") -> Q:
    """Rows whose asset's bytes are an object in our assets bucket."""
    return ~remote_q(prefix)


@dataclass(frozen=True)
class AssetLocation:
    """Where one asset's bytes are, and how to reach them."""

    href: str

    @property
    def is_remote(self) -> bool:
        return bool(_REMOTE_RE.match(self.href))

    @property
    def key(self) -> str:
        """The asset's key in the assets bucket. A Remote asset has none."""
        if self.is_remote:
            raise ValueError(f"A remote asset has no key in our storage: {self.href}")
        return self.href

    def public_url(self) -> str:
        """The URL a consumer reads the bytes at: the upstream's own, untouched, for a Remote asset."""
        if self.is_remote:
            return self.href
        return storage.assets.url(self.key)

    def open_raster(self):
        """Open the asset with rasterio for range reads — GDAL fetches only the blocks it needs.

        A Remote asset is opened at its href (GDAL's ``/vsicurl/``; upstreams are
        anonymous, so there is nothing to configure). A local one through the
        assets bucket. Neither is read into memory first (ADR 0043 §7).
        """
        import rasterio

        if self.is_remote:
            return rasterio.open(self.href)
        return storage.assets.open_raster(self.key)
