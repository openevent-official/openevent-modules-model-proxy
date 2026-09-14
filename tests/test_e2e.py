"""Exercise the built wheel against an installed SDK and real OpenEvent server."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import yaml
from openevent.sdk import OpenEventClient, AdminClient
from openevent.model_proxy_sdk import (
    OpenAI, InferRequestInput, InferCancelInput, publish_infer_request, publish_infer_cancel,
    create_client, parse_message, BadRequestError, RateLimitError, APIError,
)


def eventually(predicate, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError("Condition did not become true before timeout")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def stop_process(process):
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


class ProviderHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with self.server.guard:
            self.server.requests.append((self.path, body, dict(self.headers)))
        mode = body.get("test_mode", "normal")
        try:
            if mode == "headers_timeout":
                time.sleep(0.5)
            if not body.get("stream") or mode in ("json", "null", "http_error", "large"):
                value = (None if mode == "null" else {"error": {"message": "rate limited"}}
                         if mode == "http_error" else {"data": "x" * 12000}
                         if mode == "large" else {"answer": "ok", "echo": body})
                encoded = json.dumps(value).encode()
                self.send_response(429 if mode == "http_error" else 200)
                self.send_header("Content-Type", "application/json")
                self.send_header("X-Request-Id", "fake-request")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
                self.wfile.flush()
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("X-RateLimit-Test", "one")
            self.send_header("X-RateLimit-Test", "two")
            self.send_header("Connection", "close")
            self.end_headers()
            is_responses = self.path.endswith("/responses")
            for value in ({"delta": "hello"}, {"delta": "world"}):
                if is_responses:
                    value["type"] = "response.output_text.delta"
                self.wfile.write(b"data: " + json.dumps(value).encode() + b"\r\n\r\n")
                self.wfile.flush()
            if mode == "hold":
                while not self.server.stopping.wait(0.04):
                    self.wfile.write(b": heartbeat\n\n")
                    self.wfile.flush()
                return
            if mode == "disconnect":
                self.close_connection = True
                return
            if mode == "stream_large":
                value = ({"type": "response.output_text.delta", "delta": "x" * 12000}
                         if is_responses else {"data": "x" * 12000})
                self.wfile.write(b"data: " + json.dumps(value).encode() + b"\n\n")
            elif is_responses:
                kind = ("response.incomplete" if mode == "incomplete" else
                        "response.failed" if mode == "failed" else "response.completed")
                terminal = {"type": kind, "answer": "done"}
                if mode == "incomplete":
                    terminal = {
                        "type": kind, "sequence_number": 3,
                        "response": {"id": "resp_incomplete", "status": "incomplete",
                                     "incomplete_details": {"reason": "max_output_tokens"},
                                     "output": []},
                    }
                self.wfile.write(b"data: " + json.dumps(terminal).encode() + b"\n\n")
            else:
                self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            self.close_connection = True
        except (BrokenPipeError, ConnectionResetError):
            pass


@unittest.skipUnless(os.environ.get("OPENEVENT_RUN_E2E") == "1", "run with make e2e")
class EndToEndTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="model-proxy-e2e-")
        cls.addClassCleanup(cls.directory.cleanup)
        cls.root = Path(cls.directory.name)
        cls.event_addr = f"127.0.0.1:{free_port()}"
        cls.admin_addr = f"127.0.0.1:{free_port()}"
        config = cls.root / "server.yaml"
        config.write_text(yaml.safe_dump({
            "grpc": {"listen_addr": cls.event_addr}, "admin": {"listen_addr": cls.admin_addr},
            "storage": {"path": str(cls.root / "data")}, "limits": {"max_payload_bytes": 65536},
        }))
        cls.server_log = (cls.root / "server.log").open("w+")
        cls.addClassCleanup(cls.server_log.close)
        cls.server = subprocess.Popen([os.environ["OPENEVENT_SERVER_BIN"], str(config)],
                                      stdout=cls.server_log, stderr=subprocess.STDOUT)
        cls.addClassCleanup(stop_process, cls.server)
        cls.admin = AdminClient(cls.admin_addr, timeout_ms=200)
        cls.addClassCleanup(cls.admin.close)

        def ready():
            if cls.server.poll() is not None:
                cls.server_log.flush()
                raise AssertionError("OpenEvent server exited during startup: " +
                                     (cls.root / "server.log").read_text())
            try:
                cls.admin.list_messages()
                return True
            except Exception:
                return False

        eventually(ready)
        cls.caller, cls.proxy = 101, 201
        cls.caller_token = cls.admin.add_token(cls.caller).binding.token
        cls.proxy_token = cls.admin.add_token(cls.proxy).binding.token
        cls.transport = OpenEventClient(cls.event_addr, timeout_ms=1000)
        cls.addClassCleanup(cls.transport.close)
        cls.protocol = create_client(cls.transport, cls.caller_token, max_retries=1, retry_interval_ms=10)
        cls.http = ThreadingHTTPServer(("127.0.0.1", 0), ProviderHandler)
        cls.http.daemon_threads = True
        cls.http.requests, cls.http.guard, cls.http.stopping = [], threading.Lock(), threading.Event()
        cls.http_thread = threading.Thread(target=cls.http.serve_forever, daemon=True)
        cls.http_thread.start()
        cls.addClassCleanup(cls.stop_http)

    @classmethod
    def stop_http(cls):
        cls.http.stopping.set()
        cls.http.shutdown()
        cls.http.server_close()
        cls.http_thread.join(2)

    def setUp(self):
        response = self.transport.create_channel(
            principal=self.caller, token=self.caller_token, name=self.id(),
            visibility=1, protocol="llm.v1", members=[self.caller, self.proxy],
            description=json.dumps({"version": "v1", "updated_at_ms": 0, "metadata": {}}))
        self.channel = response.channel.channel_id
        self.processes = []
        self.clients = []
        self.log_files = []
        self.addCleanup(self.cleanup)

    def cleanup(self):
        failures = []
        for client in self.clients:
            try:
                client.close()
            except Exception as exc:
                failures.append(exc)
        for process in self.processes:
            try:
                stop_process(process)
            except Exception as exc:
                failures.append(exc)
        for log in self.log_files:
            log.close()
        if failures:
            raise failures[0]

    def start_worker(self, **overrides):
        config = {
            "protocol": "llm.v1", "open_event": {"addr": self.event_addr, "rpc_timeout_ms": 1000},
            "worker": {"max_concurrency": 1, "max_retries": 1, "retry_interval_ms": 10},
            "principal": self.proxy, "token": self.proxy_token, "channels": [self.channel],
            "max_payload_bytes": 8192, "default_provider": "fake",
            "providers": {"fake": {"type": "openai_compatible",
                "base_url": f"http://127.0.0.1:{self.http.server_port}", "api_key": "test-key",
                "timeout": {"response_header_ms": 2000, "idle_ms": 2000}}},
        }
        config.update(overrides)
        path = self.root / f"worker-{self.channel}-{len(self.processes)}.yaml"
        path.write_text(yaml.safe_dump(config))
        log = path.with_suffix(".log").open("w+")
        self.log_files.append(log)
        process = subprocess.Popen(
            [sys.executable, "-B", "-m", "openevent.model_proxy.cli", "--config", str(path)],
            stdout=log, stderr=subprocess.STDOUT)
        self.processes.append(process)

        def ready():
            log.flush()
            text = path.with_suffix(".log").read_text()
            if process.poll() is not None:
                raise AssertionError(f"Worker exited with {process.returncode}: {text}")
            return "subscription_ready" in text

        eventually(ready)
        return process

    def openai(self, **kwargs):
        client = OpenAI(openevent_addr=self.event_addr, openevent_token=self.caller_token,
                        openevent_channel_id=self.channel, openevent_principal=self.caller,
                        rpc_timeout_ms=1000, max_retries=1, retry_interval_ms=10, **kwargs)
        self.clients.append(client)
        return client

    def messages(self):
        return [parse_message(m) for m in self.transport.fetch(
            principal=self.caller, token=self.caller_token, from_seq=1, limit=1000,
            channels=[self.channel], only_my_recipient=False).messages]

    def request(self, stream_id, *, stream=False, **body):
        return publish_infer_request(self.protocol, self.channel, self.caller, InferRequestInput(
            stream_id=stream_id, method="POST", path="/v1/responses", body=dict(body, stream=stream)))

    def test_ordinary_json_and_readonly_response(self):
        self.start_worker()
        response = self.openai().responses.create(model="fake", input="hello")
        self.assertEqual(response.answer, "ok")
        self.assertEqual(response.echo.input, "hello")
        self.assertGreater(response.result_openevent_seq, response.request_seq)
        copy = response.model_dump()
        copy["answer"] = "changed"
        self.assertEqual(response.answer, "ok")
        self.assertEqual([m.payload.kind for m in self.messages()], ["infer.request", "infer.result"])

    def test_stream_metadata_updates_without_iteration(self):
        self.start_worker()
        stream = self.openai().responses.create(model="fake", input="hello", stream=True)
        self.assertIsInstance(stream.request_seq, int)
        eventually(lambda: stream.terminal_has_body is True)
        self.assertEqual(stream.response_status_code, 200)
        self.assertEqual(stream.terminal_body["type"], "response.completed")
        self.assertEqual([c.body["delta"] for c in stream], ["hello", "world"])
        self.assertEqual([h["value"] for h in stream.response_headers if h["name"] == "x-ratelimit-test"],
                         ["one", "two"])
        self.assertEqual([m.payload.kind for m in self.messages()],
                         ["infer.request", "infer.result", "infer.append", "infer.append", "infer.end"])
        stream.close()
        self.assertFalse(any(m.payload.kind == "infer.cancel" for m in self.messages()))

    def test_chat_done_and_json_null_terminal(self):
        self.start_worker()
        client = self.openai()
        chat = client.chat.completions.create(stream=True)
        self.assertEqual(len(list(chat)), 2)
        self.assertFalse(chat.terminal_has_body)
        null = client.responses.create(stream=True, test_mode="null")
        self.assertEqual(list(null), [])
        self.assertTrue(null.terminal_has_body)
        self.assertIsNone(null.terminal_body)

    def test_provider_http_error_and_failed_event(self):
        self.start_worker()
        client = self.openai()
        with self.assertRaises(RateLimitError) as ordinary:
            client.responses.create(test_mode="http_error")
        self.assertEqual(ordinary.exception.body, {"error": {"message": "rate limited"}})
        stream = client.responses.create(stream=True, test_mode="http_error")
        with self.assertRaises(RateLimitError):
            list(stream)
        self.assertEqual(stream.terminal_body, {"error": {"message": "rate limited"}})
        failed = client.responses.create(stream=True, test_mode="failed")
        chunks = []
        with self.assertRaises(APIError) as error:
            for chunk in failed:
                chunks.append(chunk.body)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(error.exception.end_status, "failed")
        self.assertEqual(error.exception.body["type"], "response.failed")

    def test_disconnect_preserves_chunks(self):
        self.start_worker()
        stream = self.openai().responses.create(stream=True, test_mode="disconnect")
        chunks = []
        with self.assertRaises(APIError) as error:
            for chunk in stream:
                chunks.append(chunk.body)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(error.exception.status_code, 60007)
        self.assertEqual(error.exception.end_status, "interrupted")

    def test_incomplete_response_preserves_output_and_original_failed_terminal(self):
        self.start_worker()
        client = self.openai()
        stream = client.responses.create(stream=True, test_mode="incomplete")
        chunks = []
        with self.assertRaises(APIError) as error:
            for chunk in stream:
                chunks.append(chunk.body["delta"])
        self.assertEqual(chunks, ["hello", "world"])
        self.assertEqual(error.exception.status_code, 200)
        self.assertEqual(error.exception.end_status, "failed")
        body = error.exception.body
        self.assertEqual(body["type"], "response.incomplete")
        self.assertEqual(body["response"]["status"], "incomplete")
        self.assertEqual(body["response"]["incomplete_details"], {"reason": "max_output_tokens"})
        messages = self.messages()
        self.assertEqual([message.payload.kind for message in messages], [
            "infer.request", "infer.result", "infer.append", "infer.append", "infer.end",
        ])
        terminal = messages[-1].payload
        self.assertEqual(terminal.status_code, 200)
        self.assertEqual(terminal.end_status, "failed")
        self.assertEqual(terminal.body, body)
        self.assertEqual(client.responses.create(input="next call").answer, "ok")

    def test_output_size_limit(self):
        self.start_worker()
        client = self.openai()
        with self.assertRaises(APIError) as ordinary:
            client.responses.create(test_mode="large")
        self.assertEqual(ordinary.exception.status_code, 60008)
        stream = client.responses.create(stream=True, test_mode="stream_large")
        chunks = []
        with self.assertRaises(APIError) as error:
            for chunk in stream:
                chunks.append(chunk.body)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(error.exception.status_code, 60008)
        raw = self.transport.fetch(principal=self.caller, token=self.caller_token,
                                   from_seq=1, limit=1000, channels=[self.channel])
        self.assertTrue(all(len(m.payload) <= 8192 for m in raw.messages))

    def test_control_rejection_does_not_wait_for_provider_slot(self):
        self.start_worker()
        client = self.openai()
        stream = client.responses.create(stream=True, stream_id="held", test_mode="hold")
        next(stream)
        with self.assertRaises(APIError) as duplicate:
            client.responses.create(stream_id="held")
        self.assertEqual(duplicate.exception.status_code, 60005)
        with self.assertRaises(BadRequestError) as missing:
            client.responses.create(provider="missing")
        self.assertEqual(missing.exception.status_code, 60009)
        self.assertEqual(next(stream).body["delta"], "world")
        stream.close()
        self.assertEqual(list(stream), [])
        response = client.responses.create(input="after cancel")
        self.assertEqual(response.answer, "ok")
        eventually(lambda: any(m.payload.kind == "infer.cancel" for m in self.messages()))

    def test_restart_only_completes_unfinished_history(self):
        first = self.request("historical", stream=True)
        duplicate = self.request("historical", stream=True)
        ordinary = self.request("ordinary-history")
        with self.http.guard:
            before = len(self.http.requests)
        self.start_worker()
        replies = eventually(lambda: [m for m in self.messages() if m.payload.kind != "infer.request"])
        self.assertEqual(len(replies), 3)
        by_request = {getattr(m.payload, "request_seq", None) or m.payload.prev_seq: m.payload for m in replies}
        self.assertEqual(by_request[first].kind, "infer.end")
        self.assertEqual(by_request[duplicate].kind, "infer.result")
        self.assertEqual(by_request[ordinary].kind, "infer.result")
        self.assertTrue(all(m.payload.status_code == 60007 for m in replies))
        with self.http.guard:
            self.assertEqual(len(self.http.requests), before)

    def test_oversized_request_rejected_without_provider(self):
        self.start_worker()
        client = self.openai()
        with self.http.guard:
            before = len(self.http.requests)
        stream = client.responses.create(stream=True, input="x" * 10000)
        with self.assertRaises(APIError) as error:
            list(stream)
        self.assertEqual(error.exception.status_code, 60008)
        self.assertEqual([m.payload.kind for m in self.messages()], ["infer.request", "infer.result"])
        with self.http.guard:
            self.assertEqual(len(self.http.requests), before)

    def test_raw_cancel_from_other_member_is_terminal(self):
        self.start_worker()
        client = self.openai()
        stream = client.responses.create(stream=True, test_mode="hold")
        next(stream)
        other_principal = 301
        other_token = self.admin.add_token(other_principal).binding.token
        self.transport.add_member(self.caller, self.caller_token, self.channel, other_principal)
        other = create_client(self.transport, other_token, max_retries=0)
        publish_infer_cancel(other, self.channel, other_principal, InferCancelInput(
            stream_id=stream.stream_id, request_seq=stream.request_seq))
        with self.assertRaises(APIError) as error:
            list(stream)
        self.assertEqual(error.exception.status_code, 60004)

    def test_provider_header_timeout_has_correct_ordinary_and_stream_shape(self):
        self.start_worker(providers={"fake": {
            "type": "openai_compatible", "base_url": f"http://127.0.0.1:{self.http.server_port}",
            "api_key": "test-key", "timeout": {"response_header_ms": 100, "idle_ms": 2000},
        }})
        client = self.openai()
        with self.assertRaises(APIError) as ordinary:
            client.responses.create(test_mode="headers_timeout")
        self.assertEqual(ordinary.exception.status_code, 60000)
        stream = client.responses.create(stream=True, test_mode="headers_timeout")
        with self.assertRaises(APIError) as streaming:
            list(stream)
        self.assertEqual(streaming.exception.status_code, 60000)
        self.assertEqual(streaming.exception.end_status, "interrupted")
        self.assertEqual([m.payload.kind for m in self.messages()],
                         ["infer.request", "infer.result", "infer.request", "infer.end"])

    def test_invalid_message_exits_worker_with_active_provider(self):
        process = self.start_worker()
        stream = self.openai().responses.create(stream=True, test_mode="hold")
        next(stream)
        invalid = self.transport.publish_auto_seq(
            principal=self.caller, token=self.caller_token, channel_id=self.channel,
            payload=b"not JSON", uuid=self.transport.get_uuid())
        eventually(lambda: process.poll() is not None, timeout=2)
        self.assertNotEqual(process.returncode, 0)
        raw = self.transport.fetch(principal=self.caller, token=self.caller_token,
                                   from_seq=invalid.seq, limit=1000, channels=[self.channel])
        self.assertEqual([m.seq for m in raw.messages], [invalid.seq])
