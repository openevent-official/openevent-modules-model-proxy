"""The API probe uses the worker transport without contacting OpenEvent."""

from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest

import yaml

from openevent.model_proxy.check_api import main
from openai_fixtures import chat_body, chat_chunks
from test_config import valid_config
from test_provider import provider_server, response


def stream_body():
    return b"".join(b"data: " + json.dumps(chunk).encode() + b"\n\n"
                    for chunk in chat_chunks()) + b"data: [DONE]\n\n"


class HeaderOutput(io.StringIO):
    """The response body is withheld until HTTP progress has been flushed."""

    def __init__(self, ready):
        super().__init__()
        self.ready = ready

    def flush(self):
        if "HTTP 200" in self.getvalue():
            self.ready.set()
        super().flush()


class CheckAPITests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="model-proxy-api-check-")
        self.addCleanup(directory.cleanup)
        self.config_path = Path(directory.name) / "model-proxy.yaml"

    def config(self, url, *, header_ms=1000, idle_ms=1000):
        config = valid_config()
        # A successful probe must not need this intentionally unreachable server.
        config["open_event"]["addr"] = "127.0.0.1:1"
        config["token"] = "openevent-credential-must-not-be-printed"
        config["providers"]["main"].update(
            base_url=url, api_key="provider-credential-must-not-be-printed",
            timeout={"response_header_ms": header_ms, "idle_ms": idle_ms})
        return config

    def run_probe(self, config, *args, output=None):
        self.config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
        return self.run_path(*args, output=output)

    def run_path(self, *args, output=None):
        output = output if output is not None else io.StringIO()
        with redirect_stdout(output), redirect_stderr(output):
            status = main(["--config", str(self.config_path), "--model", "probe-model", *args])
        text = output.getvalue()
        self.assertNotIn("provider-credential-must-not-be-printed", text)
        self.assertNotIn("openevent-credential-must-not-be-printed", text)
        return status, text

    def test_streaming_probe_reports_progress_before_body_and_sends_one_minimal_request(self):
        flushed = threading.Event()
        saw_progress = []
        body = stream_body()

        def respond(sock):
            wire = response(body, "text/event-stream")
            sock.sendall(wire[:-len(body)])
            saw_progress.append(flushed.wait(0.8))
            sock.sendall(body)

        with provider_server(respond) as (url, requests):
            status, output = self.run_probe(self.config(url), output=HeaderOutput(flushed))
        self.assertEqual(status, 0, output)
        self.assertEqual(saw_progress, [True], "HTTP progress must be flushed before waiting for model data")
        self.assertEqual(len(requests), 1)
        headers, body = requests[0]
        self.assertTrue(headers.startswith(b"POST /gateway/v1/chat/completions HTTP/1.1\r\n"))
        self.assertIn(b"Authorization: Bearer provider-credential-must-not-be-printed\r\n", headers)
        self.assertEqual(json.loads(body), {
            "model": "probe-model", "messages": [{"role": "user", "content": "Reply with OK."}], "stream": True})
        self.assertIn("HTTP 200", output)
        self.assertIn("First data event", output)
        self.assertIn("PASS: request completed", output)
        self.assertNotIn('"choices"', output)

    def test_streaming_request_accepts_a_complete_json_response(self):
        body = json.dumps(chat_body("model-body-must-not-be-printed")).encode()
        with provider_server(lambda sock: sock.sendall(response(body))) as (url, requests):
            status, output = self.run_probe(self.config(url))
        self.assertEqual(status, 0, output)
        self.assertEqual(len(requests), 1)
        self.assertIn("PASS: request completed", output)
        self.assertNotIn("model-body-must-not-be-printed", output)

    def test_http_error_is_reported_before_waiting_for_or_parsing_its_body(self):
        release = threading.Event()
        completed = threading.Event()
        self.addCleanup(release.set)

        def respond(sock):
            sock.sendall(b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/event-stream\r\n"
                         b"Content-Length: 999\r\n\r\n")
            release.wait(1)
            completed.set()

        with provider_server(respond) as (url, requests):
            status, output = self.run_probe(self.config(url, idle_ms=80))
            returned_before_body = not completed.is_set()
            release.set()
        self.assertEqual(status, 1, output)
        self.assertTrue(returned_before_body)
        self.assertEqual(len(requests), 1, "the diagnostic must not automatically retry")
        self.assertIn("FAIL: HTTP 503", output)
        self.assertNotIn("60000", output)
        self.assertNotIn("60007", output)

    def test_header_and_response_idle_timeouts_identify_the_waiting_stage(self):
        for received_headers in (False, True):
            with self.subTest(received_headers=received_headers):
                release = threading.Event()
                self.addCleanup(release.set)

                def respond(sock):
                    if received_headers:
                        sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                                     b"Content-Length: 999\r\n\r\n")
                    release.wait(1)

                with provider_server(respond) as (url, requests):
                    status, output = self.run_probe(self.config(url, header_ms=80, idle_ms=80))
                    release.set()
                self.assertEqual(status, 1, output)
                self.assertEqual(len(requests), 1)
                self.assertIn("60000", output)
                stage = "while reading response" if received_headers else "before response headers"
                self.assertIn(stage, output)
                self.assertNotIn("PASS:", output)

    def test_a_stream_without_its_terminal_event_is_not_available(self):
        chunk = b'data: {"choices":[]}\n\n'
        with provider_server(lambda sock: sock.sendall(response(chunk, "text/event-stream"))) as (url, requests):
            status, output = self.run_probe(self.config(url))
        self.assertEqual(status, 1, output)
        self.assertEqual(len(requests), 1)
        self.assertIn("60007", output)
        self.assertIn("while reading response", output)
        self.assertNotIn("PASS:", output)

    def test_explicit_provider_selection_uses_its_own_url_and_credentials(self):
        with provider_server(lambda sock: sock.sendall(response(stream_body(), "text/event-stream"))) as (url, requests):
            config = self.config("http://127.0.0.1:1")
            config["providers"]["selected"] = {
                **config["providers"]["main"], "base_url": url, "api_key": "selected-provider-secret"}
            status, output = self.run_probe(config, "--provider", "selected")
        self.assertEqual(status, 0, output)
        self.assertEqual(len(requests), 1)
        self.assertIn(b"Authorization: Bearer selected-provider-secret\r\n", requests[0][0])
        self.assertNotIn("selected-provider-secret", output)

    def test_configuration_errors_do_not_make_an_http_request(self):
        with provider_server(lambda sock: self.fail("invalid configuration made an HTTP request")) as (url, requests):
            config = self.config(url)
            status, output = self.run_probe(config, "--provider", "missing")
            self.assertEqual(status, 2, output)
            config["providers"]["main"]["timeout"]["idle_ms"] = 0
            status, output = self.run_probe(config)
            self.assertEqual(status, 2, output)
            self.config_path.write_text("api_key: [provider-credential-must-not-be-printed\n", encoding="utf-8")
            status, output = self.run_path()
            self.assertEqual(status, 2, output)
        self.assertEqual(requests, [])

    def test_blank_model_is_rejected_before_making_an_http_request(self):
        with provider_server(lambda sock: self.fail("blank model made an HTTP request")) as (url, requests):
            with self.assertRaises(SystemExit) as error:
                self.run_probe(self.config(url), "--model", "   ")
            self.assertEqual(error.exception.code, 2)
        self.assertEqual(requests, [])


if __name__ == "__main__":
    unittest.main()
