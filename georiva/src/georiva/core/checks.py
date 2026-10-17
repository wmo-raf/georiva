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
