"""Reading a plugin's ``georiva_plugin_info.json``.

Pure Python — no Django, no settings — so the parsing can be tested without an
app registry. The system check in :mod:`georiva.core.checks` is the consumer.

Only ``requires_env`` is interpreted here: a list of
``{"name": str, "required": bool, "description": str}`` entries naming the
environment variables a plugin needs (names only, never values).
"""

import json
import os
from pathlib import Path

MANIFEST_FILENAME = "georiva_plugin_info.json"


def load_manifest(folder):
    """The parsed manifest in a plugin checkout, or ``None`` when absent.

    Raises ``ValueError`` for a file that exists but is not a JSON object, so the
    caller can report it instead of silently skipping the plugin.
    """
    path = Path(folder) / MANIFEST_FILENAME
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"{path} could not be read as JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return data


def parse_requires_env(manifest):
    """Split ``requires_env`` into ``(entries, problems)``.

    ``entries`` holds the well-formed ones as dicts with ``name``, ``required``
    and ``description`` keys; ``problems`` is a list of human-readable strings
    for the malformed ones. An absent key is simply no requirements.
    """
    raw = manifest.get("requires_env", [])
    if not isinstance(raw, list):
        return [], ["'requires_env' must be a list"]

    entries, problems = [], []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            problems.append(f"requires_env[{index}] must be an object")
            continue
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            problems.append(f"requires_env[{index}] needs a non-empty string 'name'")
            continue
        required = item.get("required", False)
        if not isinstance(required, bool):
            problems.append(f"requires_env[{index}] ('{name}'): 'required' must be true or false")
            continue
        description = item.get("description", "")
        entries.append(
            {
                "name": name.strip(),
                "required": required,
                "description": description if isinstance(description, str) else "",
            }
        )
    return entries, problems


def missing_required_env(entries, environ=None):
    """The required entries whose variable is unset or empty."""
    environ = os.environ if environ is None else environ
    return [e for e in entries if e["required"] and not environ.get(e["name"], "").strip()]
