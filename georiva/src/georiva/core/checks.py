"""System checks for installed plugins.

Run by ``manage.py check``, ``migrate`` and ``runserver`` — and so once per web
boot, not in the celery workers, beat or the consumers, which never run checks.
"""

import os

from django.conf import settings
from django.core.checks import Error, Warning, register

from .plugin_manifest import load_manifest, missing_required_env, parse_requires_env


@register()
def check_plugin_requires_env(app_configs, **kwargs):
    """Report required ``requires_env`` variables a plugin declares but the
    environment lacks (#113). A warning, or an error under
    ``GEORIVA_STRICT_PLUGIN_ENV``."""
    strict = getattr(settings, "GEORIVA_STRICT_PLUGIN_ENV", False)
    missing_level = Error if strict else Warning
    folders = getattr(settings, "GEORIVA_PLUGIN_FOLDER_BY_APP", {})
    messages = []

    for app_name, folder in sorted(folders.items()):
        try:
            manifest = load_manifest(folder)
        except ValueError as exc:
            messages.append(Warning(str(exc), obj=app_name, id="georiva.W002"))
            continue
        if manifest is None:
            continue

        entries, problems = parse_requires_env(manifest)
        for problem in problems:
            messages.append(
                Warning(
                    f"Plugin '{app_name}': {problem}.",
                    hint="Fix 'requires_env' in georiva_plugin_info.json.",
                    obj=app_name,
                    id="georiva.W002",
                )
            )
        for entry in missing_required_env(entries, os.environ):
            hint = entry["description"] or "Set it in the stack .env."
            messages.append(
                missing_level(
                    f"Plugin '{app_name}' requires {entry['name']}, but it is not set.",
                    hint=hint,
                    obj=app_name,
                    id="georiva.E001" if strict else "georiva.W001",
                )
            )
    return messages


@register()
def check_public_endpoint_name(app_configs, **kwargs):
    """Warn when ``MINIO_PUBLIC_ENDPOINT`` is not named the way the bundled proxy routes
    the S3 API (``s3.<domain>``).

    A browser upload is a ``PUT`` to that name. Anything else reaches the admin site,
    which has no route for it: the progress bar still hits 100% and the file is never
    stored. Only a warning: an operator may pin another sender host in the proxy.
    Whether the name is the site's own host needs a request, so that is the upload
    page's job, not this check's.
    """
    if getattr(settings, "GEORIVA_STORAGE_BACKEND", "local") != "s3":
        return []

    from georiva.core.storage.public_endpoint import NO_S3_PREFIX, describe

    endpoint = describe(settings.MINIO_PUBLIC_ENDPOINT, settings.MINIO_PUBLIC_ENDPOINT_USE_SSL)
    if endpoint.advisory != NO_S3_PREFIX:
        return []
    return [
        Warning(
            f"MINIO_PUBLIC_ENDPOINT is '{endpoint.host}', which does not start with 's3.'.",
            hint=(
                "Browser uploads go to this name, and the bundled proxy routes only s3.<your domain> "
                "to storage; anything else reaches the admin site and the upload fails with a 404. "
                "Set it in the stack .env and re-run ./deploy.sh. Ignore this if you pinned a "
                "different sender host in the proxy."
            ),
            id="georiva.W003",
        )
    ]
