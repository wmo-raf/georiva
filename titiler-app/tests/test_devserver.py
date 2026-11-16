"""The dev server does not outlive its worker (#699).

In dev, Titiler runs under uvicorn's reloader: a parent that holds the port and
one worker child that answers. When the kernel OOM-killed that child, the parent
kept the port and started nothing, so every tile request hung and nothing
looked unhealthy. A worker killed by a signal must now take the dev server down
with it, so Docker's restart policy brings the container back.

A worker that exits because the code it loaded is broken is a different case:
the reloader waits for the next edit, as before, rather than crash-looping the
container while somebody is mid-edit.

These run the real dev server as a subprocess against a throwaway app.
"""

import os
import queue
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

TITILER_APP_ROOT = Path(__file__).resolve().parent.parent

_WORKER_PID = re.compile(r"Started server process \[(\d+)\]")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class DevServer:
    """The dev server as a subprocess, its output read on a thread so a wait can time out."""

    def __init__(self, app_dir: Path):
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "app.devserver",
                "throwaway:app",
                "--app-dir",
                str(app_dir),
                "--host",
                "127.0.0.1",
                "--port",
                str(_free_port()),
                "--reload",
                "--reload-dir",
                str(app_dir),
                "--lifespan",
                "off",
            ],
            cwd=TITILER_APP_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.lines = queue.Queue()
        self.seen = []
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.process.stdout:
            self.lines.put(line)

    def wait_for(self, pattern: re.Pattern, timeout: float = 30) -> re.Match:
        deadline = time.monotonic() + timeout
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                line = self.lines.get(timeout=remaining)
            except queue.Empty:
                break
            self.seen.append(line)
            if match := pattern.search(line):
                return match
        raise AssertionError(f"never saw {pattern.pattern!r}; output was:\n{''.join(self.seen)}")

    def wait(self, timeout: float) -> int:
        return self.process.wait(timeout=timeout)

    def stop(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)


@pytest.fixture
def start_devserver():
    started = []

    def start(app_dir):
        started.append(DevServer(app_dir))
        return started[-1]

    yield start

    for devserver in started:
        devserver.stop()


def test_a_worker_killed_by_a_signal_takes_the_dev_server_down(tmp_path, start_devserver):
    (tmp_path / "throwaway.py").write_text("async def app(scope, receive, send):\n    pass\n")
    devserver = start_devserver(tmp_path)
    worker_pid = int(devserver.wait_for(_WORKER_PID).group(1))

    os.kill(worker_pid, signal.SIGKILL)

    assert devserver.wait(timeout=15) != 0


def test_a_worker_that_fails_to_load_its_app_leaves_the_reloader_waiting(tmp_path, start_devserver):
    (tmp_path / "throwaway.py").write_text("raise RuntimeError('mid-edit')\n")
    devserver = start_devserver(tmp_path)
    devserver.wait_for(re.compile("mid-edit"))

    with pytest.raises(subprocess.TimeoutExpired):
        devserver.wait(timeout=5)


def test_an_edit_reloads_the_worker_without_taking_the_dev_server_down(tmp_path, start_devserver):
    source = tmp_path / "throwaway.py"
    source.write_text("async def app(scope, receive, send):\n    pass\n")
    devserver = start_devserver(tmp_path)
    first = int(devserver.wait_for(_WORKER_PID).group(1))

    # The file watcher starts a moment after the first worker does, so an edit
    # made too early goes unseen: edit until the worker is replaced.
    for attempt in range(10):
        source.write_text(f"async def app(scope, receive, send):\n    return {attempt}\n")
        try:
            second = int(devserver.wait_for(_WORKER_PID, timeout=3).group(1))
            break
        except AssertionError:
            continue
    else:
        raise AssertionError("an edit never reloaded the worker")

    assert second != first
    with pytest.raises(subprocess.TimeoutExpired):
        devserver.wait(timeout=3)
