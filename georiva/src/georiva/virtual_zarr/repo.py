"""
Icechunk repository wiring for GeoRiva's MinIO buckets.

One repo per Variable, on the zarr bucket at the org-first prefix
``{org}/{catalog}/{collection}/{variable}/``.  The repo holds only virtual
references to COG bytes on the assets bucket — committed refs store plain
``s3://{assets-bucket}/{key}`` URIs, and the actual endpoint/credentials are
supplied at ``Repository.open()`` time via the virtual chunk container
(late binding: readers choose the internal or public endpoint themselves).

Spike-validated against MinIO: plain ``icechunk.s3_storage(...)`` with
``allow_http=True`` + ``force_path_style=True`` needs no non-default
StorageSettings; MinIO's conditional writes are sufficient for icechunk's
ref updates.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import icechunk
from django.conf import settings

from georiva.virtual_zarr.classify import CommittedState

logger = logging.getLogger(__name__)

# Committed virtual refs use s3:// URIs on the assets bucket (see
# VirtualZarrBuilder — rename_paths rewrites chunk URLs to this form).
def container_prefix() -> str:
    return f"s3://{settings.GEORIVA_ASSETS_BUCKET}/"


def _public_endpoint_url() -> str:
    """Public MinIO endpoint as a full URL (settings may omit the scheme)."""
    endpoint = settings.MINIO_PUBLIC_ENDPOINT
    if endpoint.startswith(("http://", "https://")):
        return endpoint
    scheme = "https" if settings.MINIO_PUBLIC_ENDPOINT_USE_SSL else "http"
    return f"{scheme}://{endpoint}"


def _endpoint_url(internal: bool) -> str:
    return settings.AWS_S3_ENDPOINT_URL if internal else _public_endpoint_url()


def _storage(repo_path: str) -> icechunk.Storage:
    # Repo metadata always travels over the internal endpoint — open_repo()
    # runs inside the container, whatever endpoint the *chunk* reads use.
    # Only the virtual chunk container (below) switches on `internal`,
    # mirroring the old kerchunk remote_options late binding.
    return icechunk.s3_storage(
        bucket=settings.GEORIVA_ZARR_BUCKET,
        prefix=repo_path,
        region=settings.AWS_S3_REGION_NAME,
        endpoint_url=_endpoint_url(internal=True),
        allow_http=True,
        access_key_id=settings.AWS_ACCESS_KEY_ID,
        secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        force_path_style=True,
    )


def _config(*, internal: bool) -> icechunk.RepositoryConfig:
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(icechunk.VirtualChunkContainer(
        container_prefix(),
        icechunk.s3_store(
            region=settings.AWS_S3_REGION_NAME,
            endpoint_url=_endpoint_url(internal),
            allow_http=True,
            s3_compatible=True,
            force_path_style=True,
        ),
    ))
    return config


def _credentials() -> dict:
    return icechunk.containers_credentials({
        container_prefix(): icechunk.s3_credentials(
            access_key_id=settings.AWS_ACCESS_KEY_ID,
            secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        ),
    })


def open_repo(
        repo_path: str,
        *,
        internal: bool = True,
        create: bool = False,
) -> icechunk.Repository:
    """
    Open (or, with ``create=True``, open-or-create) the Icechunk repo at an
    org-first prefix on the zarr bucket.
    """
    opener = icechunk.Repository.open_or_create if create else icechunk.Repository.open
    return opener(
        _storage(repo_path),
        config=_config(internal=internal),
        authorize_virtual_chunk_access=_credentials(),
    )


def repo_exists(repo_path: str) -> bool:
    return icechunk.Repository.exists(_storage(repo_path))


# ---------------------------------------------------------------------------
# Commit metadata — the watermark's source of truth (design decision 5)
# ---------------------------------------------------------------------------

def commit_metadata(
        watermark: datetime,
        item_count: int,
        time_start: datetime,
        time_end: datetime,
) -> dict:
    """Properties recorded on every commit; values must be JSON-friendly."""
    return {
        "watermark": watermark.isoformat(),
        "item_count": item_count,
        "time_start": time_start.isoformat(),
        "time_end": time_end.isoformat(),
    }


def _tip_snapshot(repo: icechunk.Repository):
    """Latest snapshot on ``main``, or None on an empty repo."""
    try:
        return next(iter(repo.ancestry(branch="main")))
    except StopIteration:
        return None


def latest_snapshot_id(repo: icechunk.Repository) -> Optional[str]:
    """Snapshot id at the tip of ``main``, or None on an empty repo."""
    snapshot = _tip_snapshot(repo)
    return snapshot.id if snapshot else None


def latest_committed_state(repo: icechunk.Repository) -> Optional[CommittedState]:
    """
    Read the committed state from the latest commit on ``main``.

    Returns None when the repo has no build commit yet (fresh repo, or a
    repo whose tip predates this metadata scheme) — the caller then falls
    back to a full rebuild.
    """
    snapshot = _tip_snapshot(repo)
    if snapshot is None:
        return None

    metadata = snapshot.metadata or {}
    try:
        return CommittedState(
            watermark=datetime.fromisoformat(metadata["watermark"]),
            time_end=datetime.fromisoformat(metadata["time_end"]),
            item_count=int(metadata["item_count"]),
        )
    except (KeyError, TypeError, ValueError):
        logger.info(
            "Icechunk tip %s carries no build metadata — treating as unbuilt",
            snapshot.id,
        )
        return None
