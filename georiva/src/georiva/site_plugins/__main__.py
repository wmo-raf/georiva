"""``python -m georiva.site_plugins`` — the ``install-plugins`` command."""

import argparse
import hashlib
import importlib.metadata
import io
import json
import os
import shutil
import subprocess
import sys
import sysconfig
import tarfile
import urllib.request
from pathlib import Path

from packaging.utils import canonicalize_name

from .installer import InstallError, install
from .manifest import PluginSpec


def fetch(spec: PluginSpec, dest: Path) -> None:
    if spec.git:
        cmd = ["git", "clone", "--quiet", "--depth", "1"]
        if spec.tag:
            cmd += ["--branch", spec.tag]
        _run(cmd + [spec.git, str(dest)], f"cloning {spec.git}")
        shutil.rmtree(dest / ".git", ignore_errors=True)
        return

    with urllib.request.urlopen(spec.url, timeout=120) as response:  # noqa: S310 - the operator's own manifest
        body = response.read()
    if spec.hash and hashlib.sha256(body).hexdigest() != spec.hash:
        raise InstallError(f"{spec.name}: the download at {spec.url} does not match its sha256 hash")
    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as tar:
        tar.extractall(dest, filter="data", members=_strip_top_level(tar))


def _strip_top_level(tar: tarfile.TarFile):
    """GitHub-style tarballs wrap everything in one directory; the plugin root is inside it."""
    members = tar.getmembers()
    tops = {m.name.split("/", 1)[0] for m in members}
    if len(tops) != 1:
        return members
    for member in members:
        member.name = member.name.partition("/")[2]
    return [m for m in members if m.name]


def _core_distributions() -> dict[str, str]:
    """What the venv itself provides. Read from its site-packages alone: the .pth that exposes the
    live plugin ``lib`` would otherwise make a reinstall think those dependencies are still satisfied."""
    purelib = sysconfig.get_paths()["purelib"]
    return {canonicalize_name(d.metadata["Name"]): d.version for d in importlib.metadata.distributions(path=[purelib])}


def pip(requirements: list[str], target: Path, constraints: Path) -> None:
    """Install what core does not already provide, never moving a version core pins.

    ``--ignore-installed`` makes the dry run name everything the plugins need, so the
    packages core already has can be left out and ``lib`` holds no second copy of numpy.
    """
    base = [sys.executable, "-m", "pip", "install", "--only-binary=:all:", "-c", str(constraints)]
    report = Path(target).parent / ".pip-report.json"
    _run(
        base + ["--dry-run", "--ignore-installed", "--quiet", "--report", str(report), *requirements],
        "resolving plugin dependencies",
    )
    core = _core_distributions()
    needed = [item["metadata"] for item in json.loads(report.read_text())["install"]]
    report.unlink()
    missing = [f"{m['name']}=={m['version']}" for m in needed if canonicalize_name(m["name"]) not in core]
    if missing:
        _run(base + ["--no-deps", "--target", str(target), *missing], "installing plugin dependencies")


def _run(cmd: list[str], doing: str) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode:
        raise InstallError(f"{doing} failed:\n{result.stdout}{result.stderr}".rstrip())


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="install-plugins", description=__doc__)
    parser.add_argument("--manifest", default=os.environ.get("GEORIVA_PLUGINS_MANIFEST", "/georiva/plugins.toml"))
    parser.add_argument("--site", default="/georiva/site-plugins")
    parser.add_argument("--constraints", default="/georiva/constraints.txt")
    parser.add_argument("--force", action="store_true", help="reinstall even when the stamp matches")
    args = parser.parse_args(argv)

    try:
        result = install(
            Path(args.manifest),
            Path(args.site),
            tag=os.environ.get("GEORIVA_TAG", ""),
            constraints=Path(args.constraints),
            fetch=fetch,
            pip=pip,
            environ=os.environ,
            force=args.force,
        )
    except (InstallError, ValueError) as exc:
        print(f"install-plugins: {exc}", file=sys.stderr)
        return 1

    print("install-plugins: installed" if result.installed else "install-plugins: up to date")
    for warning in result.warnings:
        print(f"install-plugins: warning: {warning}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
