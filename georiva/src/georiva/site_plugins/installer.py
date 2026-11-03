"""The site install: fetch plugin sources, install their dependencies, stamp it.

Everything that touches the network (``fetch``, ``pip``) is passed in, so the
logic here — stamping, staging, hook and ``requires_env`` checks — is testable
alone. ``__main__`` supplies the real ones.
"""

import hashlib
import re
import shutil
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from georiva.core.plugin_manifest import load_manifest, missing_required_env, parse_requires_env

from .manifest import PluginSpec, load_specs

HOOKS = ("build.sh", "runtime_setup.sh")
SET_LINE = re.compile(r"\s*set\s+-[\w]+(\s+\w+)?\s*")
STAMP = "stamp"


class InstallError(Exception):
    pass


@dataclass
class Result:
    installed: bool
    warnings: list[str] = field(default_factory=list)


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
    fetch: Callable[[PluginSpec, Path], None],
    pip: Callable[[list[str], Path, Path], None],
    environ: Mapping[str, str],
    force: bool = False,
) -> Result:
    stamp = stamp_for(manifest, tag)
    stamp_file = site / STAMP
    if not force and stamp_file.is_file() and stamp_file.read_text().strip() == stamp:
        return Result(installed=False)

    # Built beside the live install and swapped in, so a failed run leaves the old one standing.
    staging = site / ".staging"
    shutil.rmtree(staging, ignore_errors=True)
    (staging / "src").mkdir(parents=True)

    warnings, requirements = [], []
    try:
        for spec in load_specs(manifest):
            checkout = staging / ".checkout"
            checkout.mkdir()
            fetch(spec, checkout)
            package = _package_name(checkout, spec)
            _check_hooks(checkout, package)
            requirements += [r for r in _requirements(checkout) if r not in requirements]
            info = load_manifest(checkout)
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

    for name in ("src", "lib"):
        shutil.rmtree(site / name, ignore_errors=True)
        (staging / name).rename(site / name)
    shutil.rmtree(staging, ignore_errors=True)
    stamp_file.write_text(stamp + "\n")
    return Result(installed=True, warnings=warnings)
