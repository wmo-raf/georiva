"""``uvicorn --reload`` that does not outlive its worker (#699).

The dev server (``docker-entrypoint.sh uvicorn-dev``) is uvicorn's reloader: a
parent that holds the port and one worker child that answers. Uvicorn's
reloader never looks at that child between edits. When the kernel OOM-killed
it, the parent kept the port open and started nothing, so every tile request
hung while the container still looked up.

Here the parent watches its worker. A worker killed by a signal it was not
sent by the reloader (an OOM kill is SIGKILL) takes the dev server down with a
non-zero exit, and Docker's restart policy brings the container back. A worker
that exits on its own — the app failed to import mid-edit — is left as uvicorn
leaves it: the reloader waits for the next edit.

Run it exactly as uvicorn's CLI: ``python -m app.devserver app.main:app --reload …``.
Production does not use it; gunicorn replaces its own dead workers.
"""

import importlib
import logging
import sys
import threading

from uvicorn.supervisors import ChangeReload

logger = logging.getLogger("uvicorn.error")

#: How often, in seconds, the parent looks at its worker.
WORKER_CHECK_INTERVAL = 1.0


class WorkerWatchingReload(ChangeReload):
    """Leans on uvicorn's reloader internals (``process``, ``should_exit``,
    ``startup``/``restart``/``run``); uvicorn is pinned in ``pyproject.toml`` and
    ``tests/test_devserver.py`` runs the real thing, so an upgrade that moves
    them fails there."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._worker_lock = threading.Lock()
        self.worker_killed_by = None

    def startup(self) -> None:
        super().startup()
        threading.Thread(target=self._watch_worker, name="worker-watch", daemon=True).start()

    def restart(self) -> None:
        # The reloader terminates the old worker itself; holding the lock keeps
        # that from being read as a death.
        with self._worker_lock:
            super().restart()

    def run(self) -> None:
        super().run()
        if self.worker_killed_by is not None:
            sys.exit(1)

    def _watch_worker(self) -> None:
        while not self.should_exit.wait(WORKER_CHECK_INTERVAL):
            with self._worker_lock:
                process = self.process
                if process.is_alive():
                    continue
                exitcode = process.exitcode
            if exitcode is not None and exitcode < 0:
                self.worker_killed_by = -exitcode
                logger.error(
                    "Worker process [%s] was killed by signal %s; stopping so the container restarts.",
                    process.pid,
                    self.worker_killed_by,
                )
                self.should_exit.set()
                return


def main() -> None:
    # ``uvicorn.main`` as an attribute is the CLI function the package
    # re-exports, not the module whose ``run`` picks the reloader.
    cli = importlib.import_module("uvicorn.main")
    cli.ChangeReload = WorkerWatchingReload
    cli.main()


if __name__ == "__main__":
    main()
