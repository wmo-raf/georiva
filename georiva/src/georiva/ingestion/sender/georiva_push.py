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

With --dir, one pass sends every file in a folder whose marker does not match
it, oldest first; run it from cron and a sender catches up after an outage on
its own. A pass leaves alone dot-files, .part and .tmp names, files modified in
the last --settle seconds (120) and files whose size changes over 10 seconds, so
whatever writes the folder should write under a .part name and rename when done.
--keep N then removes sent files beyond the newest N; a file without a matching
marker is never removed. A file is locked while it is sent (where the system has
flock), so overlapping passes, or a pass and a single send, never send it twice.

Plain HTTP is refused unless the block says georiva_allow_http = true, which
GeoRiva writes only while its own endpoint is plain HTTP.

Any value in the block can be overridden from the environment as
GEORIVA_PUSH_<KEY>, e.g. GEORIVA_PUSH_SECRET_ACCESS_KEY.

Requirements: Python 3.6+, nothing else. boto3 is used instead when installed.

Examples:
    python3 georiva_push.py send /data/georiva/wrf_20261027T06Z.nc --config ~/.georiva/sender.conf

    # crontab, every 5 minutes, keeping the newest 20 sent files (one line in the crontab):
    */5 * * * * python3 /opt/georiva/georiva_push.py send --dir /data/georiva --keep 20
        --config ~/.georiva/sender.conf >> ~/.georiva/push.log 2>&1

Exit status: 0 when every file was sent or already sent, 1 when a file could not
be sent, 2 for a usage or configuration error.
"""

import argparse
import configparser
import contextlib
import datetime as dt
import hashlib
import hmac
import json
import os
import socket
import stat
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

try:
    import fcntl
except ImportError:  # Windows: sends there go unlocked
    fcntl = None

__version__ = "1.1.0"

USER_AGENT = f"georiva-push/{__version__}"
MARKER_SUFFIX = ".sent"
REQUIRED = ["access_key_id", "secret_access_key", "endpoint", "region", "georiva_dest"]
#: Waits between attempts: five tries over about a quarter of an hour.
RETRY_DELAYS = [60, 120, 240, 480]
TIMEOUT = 300
CHUNK = 1024 * 1024
#: Above this one PUT is refused by S3; nothing a push feed takes should come near it.
MAX_SINGLE_PUT = 5 * 1024**3
#: Folder mode leaves alone what something else may still be writing.
SKIP_SUFFIXES = (".part", ".tmp", MARKER_SUFFIX)
SETTLE = 120
SIZE_WAIT = 10


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


def read_marker(path):
    """``path``'s marker as a dict, empty when there is none or it cannot be read."""
    try:
        with open(marker_path(path)) as fh:
            marker = json.load(fh)
    except (OSError, ValueError):
        return {}
    return marker if isinstance(marker, dict) else {}


def fingerprint(st):
    """What a file's stat says about whether it changed: its size and mtime."""
    return (st.st_size, st.st_mtime_ns)


def already_sent(path, checksum):
    """True when ``path``'s marker records this checksum."""
    return read_marker(path).get("sha256") == checksum


def unchanged_since_sent(path, st):
    """True when ``path`` has the size and mtime its marker recorded, so hashing it again would find the same.

    Content rewritten in place under the same size and mtime (``touch -r``) goes unnoticed;
    ``send FILE --force`` sends such a file.
    """
    marker = read_marker(path)
    return "sha256" in marker and (marker.get("size"), marker.get("mtime_ns")) == fingerprint(st)


def is_sent(path):
    """True when ``path``'s marker matches it: unchanged since it was sent, or the same bytes."""
    try:
        st = os.stat(path)
        return unchanged_since_sent(path, st) or already_sent(path, sha256_of(path))
    except OSError:
        return False


def write_marker(path, checksum, key, st, sent_at=None):
    """Record that ``path`` was sent; ``st`` is its stat from before it was hashed."""
    size, mtime_ns = fingerprint(st)
    tmp = marker_path(path) + ".part"
    with open(tmp, "w") as fh:
        json.dump(
            {
                "sha256": checksum,
                "key": key,
                "size": size,
                "mtime_ns": mtime_ns,
                "sent_at": sent_at or dt.datetime.now(dt.timezone.utc).isoformat(),
            },
            fh,
        )
    os.replace(tmp, marker_path(path))


@contextlib.contextmanager
def file_lock(path, block=True):
    """Hold an advisory lock on ``path`` itself, so two senders never send it at once.

    Yields False when another sender holds it and ``block`` is false. Where there is no
    flock (Windows) or the filesystem refuses one, it yields True and nothing is locked.
    """
    with open(path, "rb") as fh:
        held = True
        if fcntl is not None:
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | (0 if block else fcntl.LOCK_NB))
            except BlockingIOError:
                held = False
            except OSError as exc:
                log(f"Cannot lock {os.path.basename(path)} ({exc}); going on unlocked")
        yield held


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


def send_file(cfg, path, force=False, put=None, sleep=time.sleep, delays=None, block=True):
    """Send one file, retrying what may pass on another try. True when sent or already sent.

    The file is locked while it is sent. ``block`` false skips a file another sender is
    already sending instead of waiting for it, and counts that as success.
    """
    if not os.path.isfile(path):
        log(f"ERROR: {path} not found")
        return False
    if os.path.getsize(path) > MAX_SINGLE_PUT:
        log(f"ERROR: {path} is larger than 5 GB, which one PUT cannot carry")
        return False

    try:
        with file_lock(path, block=block) as held:
            if not held:
                log(f"Skipping {os.path.basename(path)}: another sender is sending it")
                return True
            return _send_locked(cfg, path, force, put, sleep, delays)
    except OSError as exc:
        log(f"ERROR: could not send {os.path.basename(path)}: {exc}")
        return False


def _send_locked(cfg, path, force, put, sleep, delays):
    st = os.stat(path)
    checksum = sha256_of(path)
    marker = read_marker(path)
    if not force and marker.get("sha256") == checksum:
        if (marker.get("size"), marker.get("mtime_ns")) != fingerprint(st):
            # Same bytes, new stat (touched, or a marker from before 1.1): record the stat
            # so the next folder pass need not hash it again.
            write_marker(path, checksum, marker.get("key") or object_key(cfg, path), st, marker.get("sent_at"))
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
        write_marker(path, checksum, key, st)
        log(f"Sent {os.path.basename(path)} to {key}")
        return True
    return False


# ---------------------------------------------------------------------------
# Folder mode
# ---------------------------------------------------------------------------


def data_files(directory):
    """``(path, stat)`` for the files a pass considers, oldest first.

    Dot-files, partial names (.part, .tmp), markers and anything not a regular file are left out.
    """
    found = []
    for name in os.listdir(directory):
        if name.startswith(".") or name.endswith(SKIP_SUFFIXES):
            continue
        path = os.path.join(directory, name)
        try:
            st = os.stat(path)
        except OSError:
            continue
        if stat.S_ISREG(st.st_mode):
            found.append((st.st_mtime_ns, name, path, st))
    return [(path, st) for _, _, path, st in sorted(found)]


def send_dir(cfg, directory, keep=None, settle=SETTLE, put=None, sleep=time.sleep, now=time.time, delays=None):
    """One pass over ``directory``: send every settled file whose marker does not match it.

    A file modified within ``settle`` seconds, or whose size or mtime changes over
    SIZE_WAIT seconds, is still being written and is left for the next pass. With
    ``keep``, sent files beyond the newest ``keep`` are then removed. True when no send failed.
    """
    pending = []
    for path, st in data_files(directory):
        if unchanged_since_sent(path, st):
            continue
        if now() - st.st_mtime < settle:
            log(f"Skipping {os.path.basename(path)}: modified in the last {settle}s")
            continue
        pending.append((path, st))

    steady = []
    if pending:
        sleep(SIZE_WAIT)
        for path, st in pending:
            try:
                again = os.stat(path)
            except OSError:
                continue
            if fingerprint(again) != fingerprint(st):
                log(f"Skipping {os.path.basename(path)}: still being written")
                continue
            steady.append(path)

    results = []
    for path in steady:
        if not os.path.exists(path):
            continue  # removed since, by an overlapping pass's retention or by whoever owns the folder
        results.append(send_file(cfg, path, put=put, sleep=sleep, delays=delays, block=False))
    if keep is not None:
        prune(directory, keep)
    return all(results)


def prune(directory, keep):
    """Remove sent files, oldest first, keeping the newest ``keep``.

    A file without a matching marker is never removed.
    """
    sent = [path for path, _ in data_files(directory) if is_sent(path)]
    for path in sent[: max(len(sent) - keep, 0)]:
        try:
            with file_lock(path, block=False) as held:
                if not held or not is_sent(path):
                    continue
                os.remove(path)
        except OSError as exc:
            log(f"Could not remove {os.path.basename(path)}: {exc}")
            continue
        try:
            os.remove(marker_path(path))
        except FileNotFoundError:
            pass
        log(f"Removed {os.path.basename(path)} (sent, beyond the newest {keep})")


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


def main(argv=None):
    p = argparse.ArgumentParser(description="Send files to a GeoRiva push feed.")
    p.add_argument("--version", action="version", version=USER_AGENT)
    sub = p.add_subparsers(dest="command")
    s = sub.add_parser("send", help="send files, or every new file in a folder")
    s.add_argument("files", nargs="*", help="files to send")
    s.add_argument("--dir", help="send every settled file in this folder whose marker does not match it")
    s.add_argument("--config", help="the sender block from the feed's page (or set GEORIVA_PUSH_* variables)")
    s.add_argument("--remote", default="georiva", help="section of the config to use (default: georiva)")
    s.add_argument("--force", action="store_true", help="send even when the marker matches")
    s.add_argument("--keep", type=int, help="with --dir: after the pass, remove sent files beyond the newest N")
    s.add_argument(
        "--settle",
        type=int,
        default=SETTLE,
        help=f"with --dir: leave files modified within this many seconds (default: {SETTLE})",
    )
    a = p.parse_args(argv)

    if a.command != "send":
        p.print_help()
        return 2
    if bool(a.files) == bool(a.dir):
        s.error("give either FILE... or --dir DIR")
    if not a.dir and (a.keep is not None or a.settle != SETTLE):
        s.error("--keep and --settle go with --dir")
    if a.dir and a.force:
        s.error("--force goes with FILE...: a folder pass would send every file again")
    if a.keep is not None and a.keep < 0:
        s.error("--keep must be 0 or more")
    try:
        cfg = load_config(a.config, a.remote)
    except ConfigError as exc:
        log(f"ERROR: {exc}")
        return 2

    if a.dir:
        if not os.path.isdir(a.dir):
            log(f"ERROR: {a.dir} is not a folder")
            return 2
        return 0 if send_dir(cfg, a.dir, keep=a.keep, settle=a.settle) else 1
    results = [send_file(cfg, path, force=a.force) for path in a.files]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
