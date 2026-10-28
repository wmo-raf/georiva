# Deploying GeoRiva

A server holds no source code and builds nothing. CI publishes three images to
GitHub's container registry for every release:

| Image                                  |                                                     |
|----------------------------------------|-----------------------------------------------------|
| `ghcr.io/wmo-raf/georiva`              | Django/Wagtail app, Celery workers, event consumers |
| `ghcr.io/wmo-raf/georiva-titiler-app`  | Raster tile server                                  |
| `ghcr.io/wmo-raf/georiva-stac-browser` | STAC Browser                                        |

The server keeps a sparse checkout of this `deploy/` folder and the root
`.env.sample`, plus its own `.env`. Upgrading means picking a version: the
checkout moves to it, the images are pulled, the stack restarts.

```
georiva/                      sparse checkout, nothing else
├── .env.sample               the full list of settings, with safe defaults
└── deploy/
    ├── .env                  yours: this server's settings (gitignored)
    ├── docker-compose.yml    the stack, image-only
    ├── docker-compose.override.yml   yours, optional (gitignored)
    ├── deploy.sh             install / upgrade / rollback
    ├── docker/               yours: data — database, objects, media (gitignored)
    ├── nginx/  martin/       service config, owned by the release
    └── ...
```

Everything marked *yours* survives upgrades. Everything else belongs to the
release, and `deploy.sh` refuses to run if you have edited it — site changes go
in `.env` and `docker-compose.override.yml`.

## Versions

| Version       | Images                                    | Use                     |
|---------------|-------------------------------------------|-------------------------|
| `v0.5.0`      | the release tag, exactly as in git        | production              |
| `main`        | the tip of `main`, moves with every merge | a staging server        |
| `sha-abc1234` | one commit of `main`                      | pinning a staging build |

Releases are listed at <https://github.com/wmo-raf/georiva/releases>. There is
no `latest`: a server always names the version it runs.

## Requirements

- Linux, amd64, with Docker Engine and the Compose plugin (`docker compose version`)
- Git 2.35 or newer (`sparse-checkout set --no-cone`)
- Ports 80 (and 443 for TLS) reachable, and DNS for your domain — see [TLS](#tls)
- About 10 GB of disk for images, plus whatever your data needs

## First install

```bash
git clone --filter=blob:none --no-checkout https://github.com/wmo-raf/georiva.git
cd georiva
git sparse-checkout set --no-cone '/deploy/' '/.env.sample'
git checkout v0.5.0
cp .env.sample deploy/.env
```

**Fill in `deploy/.env`.** Leave `GEORIVA_TAG` blank — `deploy.sh` writes it.
Every other blank either falls back to a safe default or stops `docker compose`
with a message saying what to set. At minimum:

| Setting                                                     |                                                                               |
|-------------------------------------------------------------|-------------------------------------------------------------------------------|
| `SECRET_KEY`                                                | a long random string: `openssl rand -base64 48`                               |
| `GEORIVA_DB_USER`, `GEORIVA_DB_PASSWORD`, `GEORIVA_DB_NAME` | database credentials (created on first start)                                 |
| `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`                    | object-store credentials (password at least 8 characters)                     |
| `GEORIVA_BASE_DOMAIN`                                       | e.g. `georiva.example.org`; organisations live at `<org>.georiva.example.org` |
| `MINIO_PUBLIC_ENDPOINT`                                     | `s3.<base domain>` — where push-feed senders upload                           |

Behind TLS also set `GEORIVA_SITE_PORT=443` and `MINIO_PUBLIC_ENDPOINT_USE_SSL=true`.
The base domain and all its subdomains are already allowed hosts and trusted
`https://` CSRF origins; `ALLOWED_HOSTS` and `CSRF_TRUSTED_ORIGINS` are only for
hostnames beyond those.

**Create the data directories.** The containers run as fixed users — the
GeoRiva image as uid 9999, the database as uid 1000 — and Docker would
otherwise create these as root, which neither can write to. The users need not
exist on the host; ownership is just a number.

```bash
cd deploy
sudo install -d -o 9999 -g 9999 docker/static docker/media docker/backup
sudo install -d -o 1000 -g 1000 docker/db_data
```

(If you point `GEORIVA_*_VOLUME` in `.env` elsewhere, create those instead.
`deploy.sh` checks, and prints the exact command if one is missing.)

**Start it:**

```bash
./deploy.sh v0.5.0
docker compose logs -f georiva        # migrations run as it starts; wait for gunicorn
docker compose exec georiva python manage.py createsuperuser
```

The first start binds Wagtail's default Site to a *central* organisation on
your base domain (`GEORIVA_BOOTSTRAP_CENTRAL_ORG`). Further institutions get
their own organisation, and hostname, with `python manage.py create_organisation`.

Open `https://<base domain>/` — the admin is at `/<ADMIN_URL_PATH>/`
(`georiva-admin` by default), STAC Browser at `/stac-browser/`.

## Upgrade

```bash
cd georiva/deploy
./deploy.sh v0.6.0
```

`deploy.sh`:

1. checks the data directories and that no release-owned file was edited;
2. fetches and checks out that version;
3. warns about settings the new `.env.sample` lists that your `.env` does not —
   copy over any you need from `../.env.sample`;
4. validates the compose file against your `.env`, then pulls the images;
5. restarts the stack (`docker compose up -d`), running any migrations;
6. removes GeoRiva images older than the version it replaced.

If step 4 fails — a new required setting, or images CI has not finished
publishing — nothing has restarted, and the checkout and `GEORIVA_TAG` are put
back as they were.

Read the release notes before upgrading: they call out anything to do by hand.

## Rollback

The same command, with the version you came from:

```bash
./deploy.sh v0.5.0
```

The previous version's images are kept, so this needs no download.

**Migrations do not roll back.** Code is rolled back, the database is not: if
the newer release migrated the database, the older code may not run against
it. Before upgrading a production server, back up the database:

```bash
docker compose exec -T georiva-db sh -c 'pg_dump -U "$POSTGRES_USER" -Fc "$POSTGRES_DB"' > georiva-$(date +%F).dump
```

and restore it before rolling back across a migration. Objects in MinIO are not
touched by migrations; `docker/minio/` is backed up as plain files.

## Overrides

Site-specific changes to the stack go in `docker-compose.override.yml`, which
compose merges over `docker-compose.yml` on its own:

```bash
cp docker-compose.override.sample.yml docker-compose.override.yml
```

The sample has commented examples: joining an existing Docker network,
publishing the database or MinIO console on loopback for an SSH tunnel, and
resource limits. Settings go in `.env`, not here.

Only the web proxy publishes a host port (`GEORIVA_WEB_PROXY_PORT`, default
80). The database, MinIO, Redis and the app servers are reachable only inside
the stack's own network.

## TLS

TLS terminates **in front of** this stack: a host-level reverse proxy or load
balancer (Caddy, Traefik, nginx, a cloud LB) holds the certificates and forwards
plain HTTP to `GEORIVA_WEB_PROXY_PORT`. Do not edit `nginx/nginx.conf` for it —
the file belongs to the release, carries GeoRiva's routing, and is replaced on
every upgrade.

The front proxy must answer for, and forward with the original `Host` header:

- `<base domain>` — the central organisation
- `*.<base domain>` — every other organisation (a wildcard certificate)
- `s3.<base domain>` — push-feed uploads; needs no body-size limit and long timeouts

and set `X-Forwarded-Proto: https`. Then set `GEORIVA_SITE_PORT=443` and
`MINIO_PUBLIC_ENDPOINT_USE_SSL=true` in `.env` and re-run `./deploy.sh <current version>`.
If the front proxy runs in Docker on the same host, the override sample shows
how to put both on one network. If you choose a different `GEORIVA_WEB_PROXY_PORT`
so the front proxy can take 80/443, remember `GEORIVA_SITE_PORT` is the port
*users* see (443), not that one.

## Day to day

Run from `deploy/`:

```bash
docker compose ps                     # what is running
docker compose logs -f --tail 100 georiva-celery-ingestion-worker
docker compose exec georiva bash      # a shell in the app container
docker compose restart georiva-celery-processing-worker
```

## Plugins

Not yet supported on image-only servers: the published images carry GeoRiva's
core only, and source plugins are installed when an image is built. A design
for installing them on a server is tracked in
[#532](https://github.com/wmo-raf/georiva/issues/532). A server that needs a
plugin today has to build its own image (`plugins.toml` and the root
`docker-compose.yml`, as before). The older install-at-container-start path,
[
`GEORIVA_PLUGIN_URLS`](https://github.com/wmo-raf/georiva/blob/main/docs/plugins/installation.md#method-2-runtime-georiva_plugin_urls),
is switched off in `docker-compose.yml` and untested against the published
images.

## For maintainers

- Images are published by the `publish` job in `.github/workflows/ci.yml`, once
  every other check is green: `main` and `sha-<short>` on pushes to main, the tag
  itself on a `v*` tag.
- A new package on ghcr.io starts **private**. After the first publish, make
  each of the three public once: the org's **Packages** tab → package →
  **Package settings** → **Change visibility**.
- `docker-compose.yml` here is the image-only copy of the root one. CI's
  `compose-drift` job fails when they diverge; see
  `.github/scripts/check-compose-drift.sh` for the differences it allows.
