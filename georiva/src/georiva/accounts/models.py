import hashlib
import secrets
from datetime import timedelta

from django.conf import settings
from django.db import models
from django.utils import timezone

from georiva.organisations.lookups import NOT_ORM_SCOPABLE

KEY_PREFIX = "grv_"

SECRET_BYTES = 32

#: How much of the secret is kept in the clear, after the prefix, so a user can
#: tell their keys apart in the management panel. Short enough to be useless on
#: its own.
DISPLAY_CHARS = 6

#: How stale ``last_used_at`` is allowed to get. A tile client makes hundreds of
#: requests a minute and every one of them presents the key; writing the column
#: each time would turn a read plane into a write plane for no added meaning.
#: The field answers "is anything still using this key", to a minute.
LAST_USED_RESOLUTION = timedelta(minutes=1)


def hash_secret(secret):
    """The digest stored for ``secret``. The only place the algorithm is named."""
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def live_q(now=None):
    """The condition a key must meet to be accepted, as a ``Q``.

    The three ways a key stops working, written once: it was revoked, it
    expired, or the person it belongs to was deactivated. The last is the easy
    one to forget and the one that matters most — disabling a departing member's
    account must not leave their scripts running.

    A ``Q`` rather than a method body so the queryset filter and the per-row
    :attr:`ApiKey.is_live` are the same rule rather than two spellings of it: a
    row the listing shows as live and the authenticator rejects (or worse, the
    reverse) is exactly the drift this avoids.
    """
    now = now or timezone.now()
    return (
        models.Q(revoked_at__isnull=True)
        & models.Q(user__is_active=True)
        & (models.Q(expires_at__isnull=True) | models.Q(expires_at__gt=now))
    )


class ApiKeyQuerySet(models.QuerySet):
    def live(self, now=None):
        """Keys that would be accepted right now."""
        return self.filter(live_q(now))


class ApiKeyManager(models.Manager.from_queryset(ApiKeyQuerySet)):
    def mint(self, *, user, name, expires_at=None):
        """Create a key, returning ``(row, secret)``.

        The only moment the secret exists. Callers show it to its owner once and
        do not store it; there is deliberately no way to ask for it again.
        """
        secret = f"{KEY_PREFIX}{secrets.token_urlsafe(SECRET_BYTES)}"
        key = self.create(
            user=user,
            name=name,
            expires_at=expires_at,
            prefix=secret[: len(KEY_PREFIX) + DISPLAY_CHARS],
            hashed_key=hash_secret(secret),
        )
        return key, secret

    def resolve(self, secret):
        """The live key ``secret`` presents, or ``None``.

        Also the point where ``last_used_at`` is kept roughly current — see
        :data:`LAST_USED_RESOLUTION` for why "roughly".
        """
        if not secret or not secret.startswith(KEY_PREFIX):
            return None
        key = self.live().select_related("user").filter(hashed_key=hash_secret(secret)).first()
        if key is not None:
            key.touch()
        return key


class ApiKey(models.Model):
    ORGANISATION_LOOKUP = NOT_ORM_SCOPABLE

    objects = ApiKeyManager()

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="api_keys",
    )
    name = models.CharField(
        max_length=80,
        help_text="What holds this key — 'QGIS on my laptop', 'ingest cron'.",
    )
    prefix = models.CharField(
        max_length=len(KEY_PREFIX) + DISPLAY_CHARS,
        editable=False,
        help_text="The visible start of the key, so its owner can tell it apart.",
    )
    hashed_key = models.CharField(max_length=64, unique=True, editable=False)
    created = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Optional. After this moment the key stops working.",
    )
    revoked_at = models.DateTimeField(null=True, blank=True, editable=False)
    last_used_at = models.DateTimeField(null=True, blank=True, editable=False)

    class Meta:
        ordering = ["-created"]
        verbose_name = "API key"

    def __str__(self):
        return f"{self.name} ({self.prefix}…)"

    @property
    def is_live(self):
        """Whether this key would be accepted right now.

        Answered by re-running :func:`live_q` against this row rather than by
        restating it in Python, so the listing and the authenticator can never
        disagree about what "live" means. It costs a query per row, which is
        affordable because it is only ever a display-time question over one
        person's handful of keys — the serving path filters with :meth:`live`
        and never asks a row about itself.
        """
        return type(self).objects.filter(live_q(), pk=self.pk).exists()

    @property
    def is_expired(self):
        return self.expires_at is not None and self.expires_at <= timezone.now()

    def revoke(self):
        """Retire the key, keeping the row.

        Revoking twice keeps the first timestamp — when it stopped working is a
        fact, not a function of how often somebody clicked.
        """
        if self.revoked_at is None:
            self.revoked_at = timezone.now()
            self.save(update_fields=["revoked_at"])

    def touch(self, now=None):
        """Record use, at :data:`LAST_USED_RESOLUTION` granularity."""
        now = now or timezone.now()
        if self.last_used_at is not None and now - self.last_used_at < LAST_USED_RESOLUTION:
            return
        self.last_used_at = now
        self.save(update_fields=["last_used_at"])
