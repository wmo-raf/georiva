"""Every failed fetch says what it means for the rest of the run (#477).

HTTP is exercised against a real local server, so the classification is of the
exceptions and responses `requests` actually produces, not of hand-made ones.
"""

import ftplib
import socket
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import MagicMock

from django.test import SimpleTestCase

from georiva.sources.fetch import FailureScope, FileRequest, FTPFetchStrategy, HTTPFetchStrategy
from georiva.sources.fetch.http import AuthenticatedHTTPFetchStrategy

BODY = b"x" * 2048


class _Handler(BaseHTTPRequestHandler):
    """Answers each path as the test arranged in `server.routes`."""

    def do_GET(self):
        route = self.server.routes[self.path]
        if route.get("delay"):
            time.sleep(route["delay"])
        self.send_response(route.get("status", 200))
        for name, value in route.get("headers", {}).items():
            self.send_header(name, value)
        body = route.get("body", BODY)
        self.send_header("Content-Length", str(route.get("content_length", len(body))))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        if route.get("hang_up"):
            self.connection.shutdown(socket.SHUT_RDWR)

    def log_message(self, *args):
        pass


def _unused_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class HTTPFailureScopeTests(SimpleTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        cls.server.daemon_threads = True
        cls.server.routes = {}
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        super().tearDownClass()

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def _fetch(self, url, strategy_class=HTTPFetchStrategy, local_path=None, **config):
        strategy = strategy_class({"max_retries": 0, "backoff_factor": 0, "timeout": 0.5, **config})
        strategy.connect()
        try:
            request = FileRequest(identifier="f", filename="f.grib", params={"url": url} if url else {})
            return strategy.fetch(request, local_path or self.tmp / "f.grib")
        finally:
            strategy.disconnect()

    def _serve(self, path, **route):
        self.server.routes[path] = route
        return self.base + path

    def assertScope(self, result, scope):
        self.assertFalse(result.success)
        self.assertEqual(result.failure_scope, scope, result.error)

    def test_a_stored_file_carries_no_scope(self):
        result = self._fetch(self._serve("/ok"))

        self.assertTrue(result.success)
        self.assertIsNone(result.failure_scope)

    def test_a_missing_file_is_this_files_problem(self):
        self.assertScope(self._fetch(self._serve("/404", status=404)), FailureScope.FILE)

    def test_another_client_error_is_this_files_problem(self):
        self.assertScope(self._fetch(self._serve("/400", status=400)), FailureScope.FILE)

    def test_a_refusal_to_authenticate_or_authorise_is_the_sources(self):
        for status in (401, 403):
            with self.subTest(status=status):
                self.assertScope(self._fetch(self._serve(f"/{status}", status=status)), FailureScope.SOURCE)

    def test_the_authenticated_strategy_classifies_the_same_way(self):
        result = self._fetch(self._serve("/auth-401", status=401), AuthenticatedHTTPFetchStrategy, auth_type="bearer")

        self.assertScope(result, FailureScope.SOURCE)

    def test_being_told_to_back_off_is_the_sources_and_says_for_how_long(self):
        for status in (429, 503):
            with self.subTest(status=status):
                url = self._serve(f"/{status}-ra", status=status, headers={"Retry-After": "120"})

                result = self._fetch(url)

                self.assertScope(result, FailureScope.SOURCE)
                self.assertEqual(result.retry_after, 120)

    def test_a_retry_after_date_becomes_seconds(self):
        from email.utils import formatdate

        url = self._serve("/429-date", status=429, headers={"Retry-After": formatdate(time.time() + 300, usegmt=True)})

        result = self._fetch(url)

        self.assertAlmostEqual(result.retry_after, 300, delta=5)

    def test_too_many_requests_without_retry_after_is_still_the_sources(self):
        self.assertScope(self._fetch(self._serve("/429", status=429)), FailureScope.SOURCE)

    def test_a_server_error_that_outlasts_the_retries_is_transient(self):
        for status in (500, 502, 503, 504):
            with self.subTest(status=status):
                result = self._fetch(self._serve(f"/{status}", status=status))

                self.assertScope(result, FailureScope.TRANSIENT)
                self.assertIsNone(result.retry_after)

    def test_a_request_timeout_from_the_server_is_transient(self):
        self.assertScope(self._fetch(self._serve("/408", status=408)), FailureScope.TRANSIENT)

    def test_a_host_that_does_not_resolve_is_the_sources(self):
        self.assertScope(self._fetch("http://georiva-no-such-host.invalid/f.grib"), FailureScope.SOURCE)

    def test_a_refused_connection_is_the_sources(self):
        self.assertScope(self._fetch(f"http://127.0.0.1:{_unused_port()}/f.grib"), FailureScope.SOURCE)

    def test_a_read_timeout_is_transient(self):
        self.assertScope(self._fetch(self._serve("/slow", delay=1.5), timeout=0.2), FailureScope.TRANSIENT)

    def test_a_connection_dropped_mid_transfer_is_transient(self):
        url = self._serve("/drop", content_length=len(BODY) * 4, hang_up=True)

        self.assertScope(self._fetch(url), FailureScope.TRANSIENT)

    def test_a_size_mismatch_is_this_files_problem(self):
        strategy = HTTPFetchStrategy({})
        strategy._session = MagicMock()
        response = strategy._session.get.return_value
        response.status_code = 200
        response.headers = {"content-length": "999999"}
        response.iter_content.return_value = [BODY]

        result = strategy.fetch(FileRequest(identifier="f", filename="f", params={"url": "http://x/f"}), self.tmp / "f")

        self.assertScope(result, FailureScope.FILE)

    def test_a_request_with_no_url_is_this_files_problem(self):
        self.assertScope(self._fetch(None), FailureScope.FILE)

    def test_a_local_write_failure_is_ours_so_the_sources(self):
        blocker = self.tmp / "not-a-directory"
        blocker.write_bytes(b"")

        result = self._fetch(self._serve("/ok-local"), local_path=blocker / "f.grib")

        self.assertScope(result, FailureScope.SOURCE)


class FTPFailureScopeTests(SimpleTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.strategy = FTPFetchStrategy({"host": "ftp.example.org"})
        self.strategy._connection = MagicMock()

    def _fetch_raising(self, exc, local_path=None):
        self.strategy._connection.retrbinary.side_effect = exc
        request = FileRequest(identifier="f", filename="f.grib", params={"remote_path": "/data/f.grib"})
        return self.strategy.fetch(request, local_path or self.tmp / "f.grib")

    def assertScope(self, result, scope):
        self.assertFalse(result.success)
        self.assertEqual(result.failure_scope, scope, result.error)

    def test_classifies_each_server_failure(self):
        cases = [
            (ftplib.error_perm("550 Failed to open file."), FailureScope.FILE),
            (ftplib.error_perm("530 Login incorrect."), FailureScope.SOURCE),
            (ftplib.error_temp("421 Service not available"), FailureScope.TRANSIENT),
            (ftplib.error_temp("426 Connection closed; transfer aborted."), FailureScope.TRANSIENT),
            (socket.gaierror(-2, "Name or service not known"), FailureScope.SOURCE),
            (ConnectionRefusedError(111, "Connection refused"), FailureScope.SOURCE),
            (TimeoutError("timed out"), FailureScope.TRANSIENT),
            (ConnectionResetError(104, "Connection reset by peer"), FailureScope.TRANSIENT),
            (EOFError(), FailureScope.TRANSIENT),
        ]
        for exc, scope in cases:
            with self.subTest(exc=repr(exc)):
                self.assertScope(self._fetch_raising(exc), scope)

    def test_a_full_disk_on_our_side_is_the_sources(self):
        import errno

        self.assertScope(self._fetch_raising(OSError(errno.ENOSPC, "No space left on device")), FailureScope.SOURCE)

    def test_a_local_file_that_cannot_be_opened_is_the_sources(self):
        blocker = self.tmp / "not-a-directory"
        blocker.write_bytes(b"")

        self.assertScope(self._fetch_raising(None, local_path=blocker / "f.grib"), FailureScope.SOURCE)

    def test_a_request_with_no_remote_path_is_this_files_problem(self):
        result = self.strategy.fetch(FileRequest(identifier="f", filename="f"), self.tmp / "f")

        self.assertScope(result, FailureScope.FILE)

    def test_a_stored_file_carries_no_scope(self):
        result = self._fetch_raising(None)

        self.assertTrue(result.success)
        self.assertIsNone(result.failure_scope)
