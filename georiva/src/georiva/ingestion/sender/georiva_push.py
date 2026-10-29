#!/usr/bin/env python3
"""
georiva_push.py

Send files to a GeoRiva push feed, as one of the feed's machine senders.

The feed's page in GeoRiva shows a block like this once, when a sender is added
or its secret is rotated. Save it to a file only you can read (chmod 600):

    [georiva]
    type = s3
    provider = Minio
    access_key_id = ...
    secret_access_key = ...
    endpoint = https://georiva.example.org
    region = us-east-1
    georiva_dest = georiva-incoming/<org>/<catalog>

Each file goes to <georiva_dest>/<its name> in one PUT. Sending a file again
under the same name replaces it, and GeoRiva ingests it again.

After a file is sent, a marker <file>.sent records its checksum. A file whose
marker still matches is skipped, so running twice sends nothing twice; a file
that changed since is sent again. --force sends regardless.

Plain HTTP is refused unless the block says georiva_allow_http = true, which
GeoRiva writes only while its own endpoint is plain HTTP.

Any value in the block can be overridden from the environment as
GEORIVA_PUSH_<KEY>, e.g. GEORIVA_PUSH_SECRET_ACCESS_KEY.

Requirements: Python 3.6+, nothing else. boto3 is used instead when installed.

Examples:
    python3 georiva_push.py send /data/georiva/wrf_20261027T06Z.nc --config ~/.georiva/sender.conf

Exit status: 0 when every file was sent or already sent, 1 when a file could not
be sent, 2 for a usage or configuration error.
"""

import argparse
import configparser
import datetime as dt
import hashlib
import hmac
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

__version__ = "1.0.0"

USER_AGENT = f"georiva-push/{__version__}"
MARKER_SUFFIX = ".sent"
REQUIRED = ["access_key_id", "secret_access_key", "endpoint", "region", "georiva_dest"]
#: Waits between attempts: five tries over about a quarter of an hour.
RETRY_DELAYS = [60, 120, 240, 480]
TIMEOUT = 300
CHUNK = 1024 * 1024
#: Above this one PUT is refused by S3; nothing a push feed takes should come near it.
MAX_SINGLE_PUT = 5 * 1024**3


class ConfigError(Exception):
    """The block or the command line cannot be used as given."""


class SendError(Exception):
    """A send failed. ``retry`` says whether trying again could help."""

    def __init__(self, message, retry):
        super().__init__(message)
        self.retry = retry


def log(msg):
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"[{stamp}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def load_config(path, remote, environ=None):
    """The sender's settings: the block's ``[remote]`` section, then GEORIVA_PUSH_* overrides."""
    environ = os.environ if environ is None else environ
    values = {}
    if path:
        parser = configparser.ConfigParser(interpolation=None)
        if not parser.read(os.path.expanduser(path)):
            raise ConfigError(f"cannot read config file {path}")
        if not parser.has_section(remote):
            raise ConfigError(f"{path} has no [{remote}] section")
        values.update(parser.items(remote))
    for key in REQUIRED + ["georiva_allow_http"]:
        override = environ.get("GEORIVA_PUSH_" + key.upper())
        if override:
            values[key] = override

    missing = [k for k in REQUIRED if not values.get(k)]
    if missing:
        raise ConfigError("missing from the sender config: {}".format(", ".join(missing)))

    endpoint = urllib.parse.urlsplit(values["endpoint"].strip())
    if endpoint.scheme not in ("http", "https") or not endpoint.netloc:
        raise ConfigError("endpoint must be http(s)://host[:port], not {!r}".format(values["endpoint"]))
    allow_http = values.get("georiva_allow_http", "").strip().lower() in ("1", "true", "yes")
    if endpoint.scheme == "http" and not allow_http:
        raise ConfigError(
            f"refusing plain HTTP to {endpoint.netloc}: files and the access key would travel unencrypted. "
            "Use the block GeoRiva issued, which allows HTTP only while its endpoint is HTTP."
        )

    return {
        "access_key": values["access_key_id"].strip(),
        "secret_key": values["secret_access_key"].strip(),
        "scheme": endpoint.scheme,
        "netloc": endpoint.netloc,
        "region": values["region"].strip(),
        "dest": values["georiva_dest"].strip().strip("/"),
    }


# ---------------------------------------------------------------------------
# Markers
# ---------------------------------------------------------------------------


def sha256_of(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(CHUNK), b""):
            digest.update(block)
    return digest.hexdigest()


def marker_path(path):
    return path + MARKER_SUFFIX


def already_sent(path, checksum):
    """True when ``path``'s marker records this checksum."""
    try:
        with open(marker_path(path)) as fh:
            return json.load(fh).get("sha256") == checksum
    except (OSError, ValueError):
        return False


def write_marker(path, checksum, key):
    tmp = marker_path(path) + ".part"
    with open(tmp, "w") as fh:
        json.dump(
            {"sha256": checksum, "key": key, "sent_at": dt.datetime.now(dt.timezone.utc).isoformat()},
            fh,
        )
    os.replace(tmp, marker_path(path))


# ---------------------------------------------------------------------------
# Signature Version 4
# ---------------------------------------------------------------------------


def _hmac(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def signing_key(secret_key, datestamp, region, service="s3"):
    k = _hmac(("AWS4" + secret_key).encode("utf-8"), datestamp)
    k = _hmac(k, region)
    k = _hmac(k, service)
    return _hmac(k, "aws4_request")


def authorization(method, path, headers, payload_hash, access_key, secret_key, region, amz_date):
    """The Authorization header for an S3 request with no query string.

    ``path`` is already URI-encoded; every header in ``headers`` is signed.
    """
    lowered = {k.lower(): " ".join(str(v).split()) for k, v in headers.items()}
    signed = sorted(lowered)
    canonical_headers = "".join(f"{k}:{lowered[k]}\n" for k in signed)
    signed_headers = ";".join(signed)
    canonical_request = "\n".join([method, path, "", canonical_headers, signed_headers, payload_hash])
    scope = f"{amz_date[:8]}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()]
    )
    signature = hmac.new(
        signing_key(secret_key, amz_date[:8], region), string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"


def host_header(scheme, netloc):
    """The Host header http.client will send: the port is dropped when it is the scheme's default."""
    default = ":443" if scheme == "https" else ":80"
    return netloc[: -len(default)] if netloc.endswith(default) else netloc


# ---------------------------------------------------------------------------
# Sending
# ---------------------------------------------------------------------------


def object_key(cfg, path):
    return "{}/{}".format(cfg["dest"], os.path.basename(path))


def put_stdlib(cfg, path, key, checksum):
    size = os.path.getsize(path)
    encoded = "/" + urllib.parse.quote(key, safe="/~")
    amz_date = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    headers = {
        "Host": host_header(cfg["scheme"], cfg["netloc"]),
        "x-amz-content-sha256": checksum,
        "x-amz-date": amz_date,
    }
    headers["Authorization"] = authorization(
        "PUT", encoded, headers, checksum, cfg["access_key"], cfg["secret_key"], cfg["region"], amz_date
    )
    # Unsigned, so they may be anything; without a type urllib would claim a form post.
    headers["Content-Length"] = str(size)
    headers["Content-Type"] = "application/octet-stream"
    headers["User-Agent"] = USER_AGENT

    url = "{}://{}{}".format(cfg["scheme"], cfg["netloc"], encoded)
    with open(path, "rb") as body:
        request = urllib.request.Request(url, data=body, headers=headers, method="PUT")
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:500]
            raise SendError(_explain(exc.code, detail), retry=_retryable(exc.code)) from None
        except (urllib.error.URLError, socket.timeout, ConnectionError, OSError) as exc:
            raise SendError("cannot reach {}: {}".format(cfg["netloc"], exc), retry=True) from None


def put_boto3(cfg, path, key, checksum):
    import boto3
    import botocore.exceptions
    from botocore.config import Config

    client = boto3.client(
        "s3",
        endpoint_url="{}://{}".format(cfg["scheme"], cfg["netloc"]),
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name=cfg["region"],
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 1},
            user_agent_extra=USER_AGENT,
            connect_timeout=60,
            read_timeout=TIMEOUT,
        ),
    )
    bucket, _, name = key.partition("/")
    try:
        with open(path, "rb") as body:
            client.put_object(Bucket=bucket, Key=name, Body=body)
    except botocore.exceptions.ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        raise SendError(_explain(status, str(exc)), retry=_retryable(status)) from None
    except (botocore.exceptions.BotoCoreError, OSError) as exc:
        raise SendError("cannot reach {}: {}".format(cfg["netloc"], exc), retry=True) from None


def _retryable(status):
    return status in (408, 429) or status >= 500


def _explain(status, detail):
    if status == 403:
        return (
            "refused (403): the access key or secret is wrong, the sender was revoked, "
            f"or georiva_dest is not this sender's feed. {detail}"
        )
    return f"HTTP {status}: {detail}"


def _have_boto3():
    try:
        import boto3  # noqa: F401
    except ImportError:
        return False
    return True


def send_file(cfg, path, force=False, put=None, sleep=time.sleep, delays=None):
    """Send one file, retrying what may pass on another try. True when sent or already sent."""
    if not os.path.isfile(path):
        log(f"ERROR: {path} not found")
        return False
    if os.path.getsize(path) > MAX_SINGLE_PUT:
        log(f"ERROR: {path} is larger than 5 GB, which one PUT cannot carry")
        return False

    checksum = sha256_of(path)
    if not force and already_sent(path, checksum):
        log(f"Already sent {os.path.basename(path)} (marker matches), skipping")
        return True

    key = object_key(cfg, path)
    put = put or (put_boto3 if _have_boto3() else put_stdlib)
    delays = RETRY_DELAYS if delays is None else delays
    for attempt in range(1, len(delays) + 2):
        try:
            put(cfg, path, key, checksum)
        except SendError as exc:
            if not exc.retry or attempt > len(delays):
                log(f"ERROR: could not send {os.path.basename(path)}: {exc}")
                return False
            wait = delays[attempt - 1]
            log(f"Attempt {attempt} failed ({exc}), retrying in {wait}s")
            sleep(wait)
            continue
        write_marker(path, checksum, key)
        log(f"Sent {os.path.basename(path)} to {key}")
        return True
    return False


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


def main(argv=None):
    p = argparse.ArgumentParser(description="Send files to a GeoRiva push feed.")
    p.add_argument("--version", action="version", version=USER_AGENT)
    sub = p.add_subparsers(dest="command")
    s = sub.add_parser("send", help="send files")
    s.add_argument("files", nargs="+", help="files to send")
    s.add_argument("--config", help="the sender block from the feed's page (or set GEORIVA_PUSH_* variables)")
    s.add_argument("--remote", default="georiva", help="section of the config to use (default: georiva)")
    s.add_argument("--force", action="store_true", help="send even when the marker matches")
    a = p.parse_args(argv)

    if a.command != "send":
        p.print_help()
        return 2
    try:
        cfg = load_config(a.config, a.remote)
    except ConfigError as exc:
        log(f"ERROR: {exc}")
        return 2

    results = [send_file(cfg, path, force=a.force) for path in a.files]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
