"""The site install: fetch plugin sources, install their dependencies, stamp it.

Everything that touches the network (``fetch``, ``pip``) is passed in, so the
logic here — stamping, staging, hook and ``requires_env`` checks — is testable
alone. ``__main__`` supplies the real ones.

Beside the stamp sits ``installed.json``: which plugins this server runs, at what
version, from where. Each run compares the new install with it and reports one
line per plugin, so an operator sees what an update changed before anything
restarts (ADR 0040).
"""

import hashlib
import json
import re
import shutil
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from georiva.core.plugin_manifest import load_manifest, missing_required_env, parse_requires_env

from .manifest import PluginSpec, load_specs

HOOKS = ("build.sh", "runtime_setup.sh")
SET_LINE = re.compile(r"\s*(set(\s+-\S+)+(\s+\w+)?|exit\s+0)\s*")
STAMP = "stamp"
INSTALLED = "installed.json"

#: The installer's last line. ``deploy/deploy.sh plugins`` restarts the backend
#: only when it reads the first, so these are a contract: change both sides at once.
INSTALLED_LINE = "install-plugins: installed"
UP_TO_DATE_LINE = "install-plugins: up to date"


class InstallError(Exception):
    pass


@dataclass(frozen=True)
class Change:
    """One plugin's line in the report: ``+`` added, ``~`` changed, ``-`` removed, ``=`` unchanged."""

    kind: str
    package: str
    old: dict | None = None
    new: dict | None = None


@dataclass
class Result:
    installed: bool
    warnings: list[str] = field(default_factory=list)
    changes: list[Change] = field(default_factory=list)


def report(changes: list[Change]) -> list[str]:
    """One line per plugin, the package names aligned."""
    width = max((len(c.package) for c in changes), default=0)
    return [f"{c.kind} {c.package:<{width}}  {_describe(c)}" for c in changes]


def _describe(change: Change) -> str:
    """``1.0.0``; ``0.2.0 → 0.2.1  (git 0.2.1 @ 5703bd8)``; ``1.4.0  (commit a1b2c3d → e4f5a6b)``."""
    entry = change.new or change.old
    version = entry.get("version") or "?"
    if change.kind in "=-":
        return version
    if change.kind == "~" and change.old.get("version") == entry.get("version"):
        if change.old.get("commit") != entry.get("commit"):
            return f"{version}  (commit {_short(change.old.get('commit'))} → {_short(entry.get('commit'))})"
        return f"{version}  ({_origin(entry)}, was {_origin(change.old)})"
    if change.kind == "~":
        version = f"{change.old.get('version') or '?'} → {version}"
    return f"{version}  ({_origin(entry)})"


def _short(commit: str | None) -> str:
    return (commit or "?")[:7]


def _origin(entry: dict) -> str:
    """Where it came from: ``git 0.2.1 @ 5703bd8`` for a git source (the commit is
    recorded only for git), ``url`` for a tarball."""
    if not entry.get("commit"):
        return "url" if not entry.get("tag") else f"git {entry['tag']}"
    return f"git {entry.get('tag') or 'branch tip'} @ {_short(entry['commit'])}"


def _read_installed(site: Path) -> dict:
    try:
        return json.loads((site / INSTALLED).read_text())
    except (OSError, ValueError):
        return {}


def _diff(old: dict, new: dict) -> list[Change]:
    changes = []
    for package, entry in new.items():
        before = old.get(package)
        kind = "+" if before is None else ("=" if before == entry else "~")
        changes.append(Change(kind, package, before, entry))
    changes += [Change("-", package, entry, None) for package, entry in old.items() if package not in new]
    return changes


def _version(folder: Path, info: dict | None, spec: PluginSpec) -> str:
    """The plugin's own version: its plugin info, else its pyproject, else the tag it was fetched at."""
    if info and info.get("version"):
        return str(info["version"])
    pyproject = folder / "pyproject.toml"
    if pyproject.is_file():
        version = tomllib.loads(pyproject.read_text(encoding="utf-8")).get("project", {}).get("version")
        if version:
            return str(version)
    return spec.tag


def stamp_for(manifest: Path, tag: str) -> str:
    """Identifies an install: the manifest's bytes plus the release it was built against."""
    body = manifest.read_bytes() if manifest.is_file() else b""
    return hashlib.sha256(body + b"\0" + tag.encode()).hexdigest()


def _package_name(folder: Path, spec: PluginSpec) -> str:
    packages = [
        p.name
        for p in (folder / "src").glob("*/")
        if (p / "__init__.py").is_file() and not p.name.startswith((".", "__"))
    ]
    if len(packages) != 1:
        raise InstallError(
            f"{spec.name} is not a GeoRiva plugin: src/ must hold exactly one package, found {len(packages)}"
        )
    return packages[0]


def _check_hooks(folder: Path, package: str) -> None:
    """Hooks are not run on a server; one that does real work is a plugin we cannot install faithfully."""
    for hook in HOOKS:
        path = folder / hook
        if not path.is_file():
            continue
        commands = [
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#") and not SET_LINE.fullmatch(line)
        ]
        if commands:
            raise InstallError(f"{package} has a {hook} with commands in it; site install does not run hooks")


def _requirements(folder: Path) -> list[str]:
    pyproject = folder / "pyproject.toml"
    if not pyproject.is_file():
        return []
    return list(tomllib.loads(pyproject.read_text(encoding="utf-8")).get("project", {}).get("dependencies", []))


def install(
    manifest: Path,
    site: Path,
    *,
    tag: str,
    constraints: Path,
    fetch: Callable[[PluginSpec, Path], str | None],
    pip: Callable[[list[str], Path, Path], None],
    environ: Mapping[str, str],
    force: bool = False,
) -> Result:
    if manifest.is_dir():
        # Docker makes a directory of a bind-mounted file that was missing. Reading that as "no plugins"
        # would uninstall everything, so stop and say what happened.
        raise InstallError(f"{manifest} is a directory: create the file (cp plugins.toml.sample plugins.toml)")
    stamp = stamp_for(manifest, tag)
    stamp_file = site / STAMP
    previous = _read_installed(site)
    if not force and stamp_file.is_file() and stamp_file.read_text().strip() == stamp:
        return Result(installed=False, changes=_diff(previous, previous))

    # Built beside the live install and swapped in, so a failed run leaves the old one standing.
    staging = site / ".staging"
    shutil.rmtree(staging, ignore_errors=True)
    (staging / "src").mkdir(parents=True)

    warnings, requirements, installed = [], [], {}
    try:
        for spec in load_specs(manifest):
            checkout = staging / ".checkout"
            checkout.mkdir()
            commit = fetch(spec, checkout)
            package = _package_name(checkout, spec)
            _check_hooks(checkout, package)
            requirements += [r for r in _requirements(checkout) if r not in requirements]
            info = load_manifest(checkout)
            installed[package] = {
                "name": spec.name,
                "source": spec.git or spec.url,
                "tag": spec.tag,
                "version": _version(checkout, info, spec),
                "commit": commit or "",
            }
            entries, _ = parse_requires_env(info or {})
            unset = missing_required_env(entries, environ)
            warnings += [f"{package} needs {e['name']}, which is not set" for e in unset]
            checkout.rename(staging / "src" / package)

        if requirements:
            pip(requirements, staging / "lib", constraints)
        (staging / "lib").mkdir(exist_ok=True)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    # Old aside, new in, old deleted: at no point is a directory missing for a service that is reading it.
    for name in ("src", "lib"):
        aside = site / f".old-{name}"
        shutil.rmtree(aside, ignore_errors=True)
        if (site / name).exists():
            (site / name).rename(aside)
        (staging / name).rename(site / name)
        shutil.rmtree(aside, ignore_errors=True)
    shutil.rmtree(staging, ignore_errors=True)
    (site / INSTALLED).write_text(json.dumps(installed, indent=2, sort_keys=True) + "\n")
    stamp_file.write_text(stamp + "\n")
    return Result(installed=True, warnings=warnings, changes=_diff(previous, installed))
