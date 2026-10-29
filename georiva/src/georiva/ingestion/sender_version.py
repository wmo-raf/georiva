"""Which ``georiva_push.py`` a machine sender runs, from the user agent its writes carry.

The script sends ``User-Agent: georiva-push/<version>``; through boto3 the same token is
appended to botocore's own. MinIO's Bucket notification passes it on as
``source.userAgent``. Senders never update themselves, so the version recorded on each
Delivery is how an operator learns which sender needs a newer copy of the script.
"""

import re

#: At most four parts of six digits, so whatever matches fits ``Delivery.sender_version``.
_TOKEN = re.compile(r"(?:^|\s)georiva-push/(\d{1,6}(?:\.\d{1,6}){0,3})(?=\s|$)")
_RELEASE = re.compile(r"\d+(?:\.\d+)*")


def from_user_agent(user_agent: str | None) -> str:
    """The georiva-push version a user agent names, or ``""`` for any other client."""
    match = _TOKEN.search(user_agent or "")
    return match.group(1) if match else ""


def of_event(ev: dict) -> str:
    """The georiva-push version behind a Bucket notification, or ``""``."""
    return from_user_agent((ev.get("source") or {}).get("userAgent"))


def served() -> str:
    """The version of the script this instance serves on the push feed page."""
    from georiva.ingestion.sender.georiva_push import __version__

    return __version__


def is_outdated(version: str, current: str | None = None) -> bool:
    """True when ``version`` is older than ``current`` (by default, the script this instance serves).

    Missing trailing parts count as zero, so ``1.1`` is the same as ``1.1.0``, and only the
    release counts, so a served ``1.2.0rc1`` compares as ``1.2.0``.
    """
    if not version:
        return False
    sent, newest = _release(version), _release(current or served())
    width = max(len(sent), len(newest))
    return sent + (0,) * (width - len(sent)) < newest + (0,) * (width - len(newest))


def _release(version: str) -> tuple[int, ...]:
    """The numeric parts a version starts with; empty when it starts with none."""
    match = _RELEASE.match(version)
    return tuple(int(p) for p in match.group(0).split(".")) if match else ()
