"""Reading ``plugins.toml`` for the site installer."""

import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PluginSpec:
    name: str
    git: str = ""
    tag: str = ""
    url: str = ""
    hash: str = ""


def load_specs(path: Path) -> list[PluginSpec]:
    """The enabled plugins in the manifest; an absent manifest is no plugins.

    Only ``git`` and ``url`` sources are accepted: a ``folder`` names a path inside
    a container, which a server installing for every service cannot rely on.

    A manifest with keys but no ``plugins`` table is refused rather than read as no
    plugins: it is almost always a misspelt table (``[[plugin]]``), and no plugins
    here means uninstall every one already on the server.
    """
    if not path.is_file():
        return []
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"{path} is not valid TOML: {exc}") from exc
    if data and "plugins" not in data:
        found = ", ".join(sorted(data))
        raise ValueError(
            f"{path} declares no [[plugins]] entries (found: {found}), "
            "refused rather than read as no plugins; an empty file means no plugins"
        )
    entries = data.get("plugins", [])
    if not (isinstance(entries, list) and all(isinstance(e, dict) for e in entries)):
        raise ValueError(f"{path}: 'plugins' must be [[plugins]] entries, not {type(entries).__name__}")

    specs = []
    for index, entry in enumerate(entries, 1):
        if not entry.get("enabled", True):
            continue
        name = entry.get("name", f"plugin #{index}")
        if not (entry.get("git") or entry.get("url")):
            raise ValueError(f"'{name}' needs a 'git' or 'url' source; 'folder' is not supported on a server")
        specs.append(
            PluginSpec(
                name=name,
                git=entry.get("git", ""),
                tag=entry.get("tag", ""),
                url=entry.get("url", ""),
                hash=entry.get("hash", ""),
            )
        )
    return specs
