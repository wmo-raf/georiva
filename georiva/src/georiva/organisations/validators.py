"""Validation for the Organisation slug.

The slug is the one string an Organisation can never change: it is a DNS label
(``<slug>.<GEORIVA_BASE_DOMAIN>``) *and* the first segment of every object-storage
key. Both uses are unforgiving — hence lowercase-only, no leading/trailing or
doubled hyphens, and a blocklist of names that would collide with an existing
host or URL prefix on the instance.
"""

import re

from django.core.exceptions import ValidationError

ORG_SLUG_MAX_LENGTH = 50

# Lowercase alphanumerics in hyphen-separated groups: no leading, trailing or
# repeated hyphens. Matches the LDH rules for a DNS label, minus uppercase.
ORG_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")

# Hostnames and URL prefixes the instance already owns. An organisation taking
# one of these would shadow a real service on the base domain.
RESERVED_ORG_SLUGS = frozenset(
    {
        "admin",
        "api",
        "assets",
        "auth",
        "docs",
        "edr",
        "ftp",
        "internal",
        "localhost",
        "mail",
        "martin",
        "media",
        "minio",
        "root",
        "stac",
        "static",
        "titiler",
        "www",
    }
)


def validate_org_slug(value):
    """Reject anything that is not a safe subdomain label and path segment."""
    if not value:
        raise ValidationError("Enter an organisation slug.", code="invalid_org_slug")

    if len(value) > ORG_SLUG_MAX_LENGTH:
        raise ValidationError(
            "Organisation slug must be at most %(max)d characters.",
            code="invalid_org_slug",
            params={"max": ORG_SLUG_MAX_LENGTH},
        )

    if not ORG_SLUG_RE.match(value):
        raise ValidationError(
            "Organisation slug must be lowercase letters and digits, separated by single hyphens (e.g. “kenya-met”).",
            code="invalid_org_slug",
        )

    if value in RESERVED_ORG_SLUGS:
        raise ValidationError(
            "“%(slug)s” is reserved and cannot be used as an organisation slug.",
            code="reserved_org_slug",
            params={"slug": value},
        )
