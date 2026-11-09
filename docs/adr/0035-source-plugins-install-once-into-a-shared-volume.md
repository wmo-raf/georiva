# Source plugins install once into a volume every service shares

> **Amended by [ADR-0040](0040-plugin-updates-restart-what-loads-them.md).**
> Reinstalling swaps the volume but restarts nothing, so a plugin-only change left running services on the old code.
> `./deploy.sh plugins` now reinstalls and restarts what mounts the volume, and the installer records what it
> installed in `installed.json`.

## Status

accepted

## Context

Servers run the images CI publishes and build nothing (`deploy/README.md`). Those images carry core only: source plugins
are installed at image build, from a `plugins.toml` a CI build does not have. A server that needs
`georiva-source-cds` would have to build its own image, which is the thing image-only deploy exists to avoid.

The runtime path that exists (`GEORIVA_PLUGIN_URLS`, `startup_plugin_setup`) installs on every container start, in all
seven backend services, each into its own container layer, from PyPI with unpinned dependency versions. Nothing is
shared and nothing survives `up` recreating a container.

## Decision

**One install, shared by every service, with no network use after it.**

- `deploy/plugins.toml` (gitignored; `plugins.toml.sample` is committed) lists plugins in the existing manifest format,
  `git` + `tag` or `url` only. `folder` is refused: a path inside a container means nothing to a server.
- A one-shot `georiva-plugins` service on the georiva image runs `install-plugins` (`python -m georiva.site_plugins`)
  into the named volume `georiva-plugins`. Every backend service mounts it read-only at `/georiva/site-plugins` and
  `depends_on` it with `service_completed_successfully`.
- The installer puts each plugin's source at `site-plugins/src/<package>/`. Discovery already scans directories
  (`GEORIVA_PLUGIN_DIRS`), so the plugin's own code needs no install; only its third-party dependencies do.
- Dependencies go to `site-plugins/lib` with `pip install -c /georiva/constraints.txt`. The constraints are exported
  from `georiva/uv.lock` when the image is built, so a plugin can add packages but never move a version core depends on;
  a conflict fails the installer, before any app container starts. A dry run against the venv first names what is
  actually missing, so `lib` holds only that, not a second copy of numpy.
- `lib` reaches `sys.path` through a `.pth` file in the venv, which appends it *after* site-packages: core wins.
- A stamp of `sha256(plugins.toml + GEORIVA_TAG)` makes an unchanged run exit at once. An upgrade or rollback changes
  the tag, which reinstalls against that release's constraints. `--force` reinstalls regardless.
- The install is built beside the live one and swapped in, so a failed run leaves the previous install standing.

Settled alongside:

- **Hooks are not run.** `build.sh` and `runtime_setup.sh` in a plugin must hold no commands (comments and `set` lines
  are fine, which is all today's plugins contain); otherwise the installer fails and says so. A plugin that needs setup
  work is a design question to raise, not something to run as arbitrary shell.
- **No compilers.** pip runs with `--only-binary=:all:`. A dependency with no linux/amd64 wheel fails by name. A
  build-tools image variant is not published; add one when a real plugin needs it.
- **`requires_env`** is documented, and the installer warns about any unset required variable. It sees only what
  compose gives the `georiva-plugins` service, so the warning is a hint: each service's own `georiva.W001` check
  remains the enforcement. The site's `docker-compose.override.yml` carries the variables.
- **Dev is unchanged.** `dev-plugins/` stays editable and bind-mounted with reload; a fetched, read-only, stamped
  install is the opposite of what dev wants. The shared volume is for servers.

## Consequences

- Installing a plugin needs internet access at install time. **Air-gapped sites are out of scope** for now
  (`file://` tarballs and a local wheelhouse); the manifest format does not preclude them.
- Every backend service now waits on `georiva-plugins`. With an empty manifest that is a sub-second no-op.
- Removing a plugin from the manifest removes it from the volume on the next run.
- The old startup path is gone: `GEORIVA_PLUGIN_URLS`, `GEORIVA_DISABLE_PLUGIN_INSTALL_ON_STARTUP`, the
  `install-plugin` / `uninstall-plugin` entrypoint commands and `install_plugin.sh --runtime`. What remains of
  `startup_plugin_setup` is `install_dev_plugins`, the editable install of `dev-plugins/`, which never runs on a
  server. `install_plugin.sh` stays as the build-time installer for your own image.
