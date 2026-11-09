# Plugin updates restart what loads them

## Status

accepted. Amends [ADR-0035](0035-source-plugins-install-once-into-a-shared-volume.md).

## Context

ADR 0035 says how plugins reach a server: a one-shot `georiva-plugins` service installs `deploy/plugins.toml` into a
shared volume, stamped so an unchanged manifest is a no-op. It does not say what has to happen once the volume has
changed.

On a core upgrade nothing has to: `./deploy.sh <version>` pulls new images, compose recreates every backend container,
and each one imports the plugins afresh. A **plugin-only** change is different. Bumping a tag in `plugins.toml` and
running `docker compose up -d` reruns the installer, which swaps the volume, but the images and the compose config are
unchanged, so compose leaves the running containers alone. Every worker keeps the plugin code it imported at startup and
goes on running the old version, with nothing to say so. `./deploy.sh` at the same version ends in the same `up -d` and
has the same gap.

It surfaced as a Forti publisher fix (0.2.1) that would have been "installed" and not run.

## Decision

**`./deploy.sh plugins [--force]` applies an edited `plugins.toml` and restarts what loads it.**

- It is a subcommand of `deploy.sh`, the one script operators already use; `plugins` cannot be mistaken for a version.
  It pulls no image and changes no checkout.
- It applies only what the manifest says. Tags are pinned by the operator; nothing looks up newer ones. A pinned file is
  what makes a server reproducible and a rollback one edit.
- It runs the installer and restarts **only if the installer reinstalled**, read from its last line
  (`install-plugins: installed` / `install-plugins: up to date`). Those lines are a contract between the installer and
  `deploy.sh`, pinned by a test on the Python side. A second run is a no-op.
- It restarts **every running service that mounts the `georiva-plugins` volume**, found from Docker by the volume's
  compose labels rather than listed in the script, so a service an override or a plugin overlay adds is covered.
  `docker compose restart`, not a recreate: only the volume changed.
- **`georiva` first**, and the rest only once its healthcheck passes. It is the container that migrates on startup, so
  plugin migrations have run before any worker executes the new code. If it does not become healthy the script stops,
  leaves the workers on the previous code (whose schema still matches), and prints the logs command and the rollback:
  set the previous tag, run `./deploy.sh plugins` again.
- `--force` passes through to the installer and always restarts. It is how an untagged plugin, which follows its branch
  tip and so never changes the stamp, picks up new commits.

**The installer records and reports what it installed.** Beside the stamp it writes `site-plugins/installed.json`: per
plugin, the source, tag, version (its `georiva_plugin_info.json`, else its `pyproject.toml`, else the tag) and, for git,
the commit. Each run compares the new install with the previous record and prints one line per plugin — `+` added,
`~` changed (`0.2.0 → 0.2.1`, or the commit when the version did not move), `-` removed, `=` unchanged — so the
operator sees what an update changed before anything restarts.

## Consequences

- A plugin update is `edit plugins.toml; ./deploy.sh plugins`. A bare `docker compose up -d` still installs, and still
  leaves the old code running; the docs say so.
- `installed.json` is the one answer to "which plugins does this server run". Nothing reads it but the report yet; an
  admin page could.
- A server installed before this has no `installed.json`, so its first update reports every plugin as added.
- `deploy.sh` is now linted (`bash -n`, `shellcheck`) by `just lint` and CI.
