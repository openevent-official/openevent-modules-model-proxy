from contextlib import contextmanager, nullcontext
import gzip
import json
import os
import socket
import socketserver
import sys
import threading
import time
import unittest
from unittest.mock import patch

from openevent.model_proxy.config import ProviderConfig, ProviderTimeout
from openevent.model_proxy.provider import ProviderCall, ProviderCancelled, ProviderError
from openevent.model_proxy_sdk import InferEndInput, parse_payload
from openevent.model_proxy_sdk import _json


@contextmanager
def provider_server(respond):
    requests = []

    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.settimeout(2)
            request = bytearray()
            while not request.endswith(b"\r\n\r\n"):
                part = self.request.recv(1)
                if not part:
                    return
                request.extend(part)
            length = next(int(line.split(b":", 1)[1]) for line in request.split(b"\r\n") if line.lower().startswith(b"content-length:"))
            body = bytearray()
            while len(body) < length:
                body.extend(self.request.recv(length - len(body)))
            requests.append((bytes(request), bytes(body)))
            try:
                respond(self.request)
            except (OSError, TimeoutError):
                pass

    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    server = Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/gateway/", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(1)


def response(body, content_type="application/json", extra=b"", status=200):
    return f"HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\nContent-Length: {len(body)}\r\n".encode() + extra + b"\r\n" + body


def call(url, stream=False, path="/v1/chat/completions", limit=4096, header_ms=1000, idle_ms=1000):
    config = ProviderConfig("openai_compatible", url, "secret", ProviderTimeout(header_ms, idle_ms))
    return ProviderCall(config, {"model": "m", "stream": stream}, path, limit)


class ProviderTests(unittest.TestCase):
    def high_fd_operation(self):
        try:
            import fcntl
        except ImportError:
            self.skipTest("High socket descriptors require Unix fcntl")
        if not hasattr(fcntl, "F_DUPFD") or not hasattr(socket, "socketpair"):
            self.skipTest("High socket descriptor allocation is unavailable")
        original, peer = socket.socketpair()
        self.addCleanup(original.close)
        self.addCleanup(peer.close)
        try:
            descriptor = fcntl.fcntl(original.fileno(), fcntl.F_DUPFD, 2048)
        except OSError as exc:
            self.skipTest(f"Cannot allocate a socket descriptor >= 2048: {exc}")
        try:
            sock = socket.socket(fileno=descriptor)
        except BaseException:
            os.close(descriptor)
            raise
        self.addCleanup(sock.close)
        original.close()
        sock.setblocking(False)
        peer.settimeout(1)
        operation = call("http://unused.invalid")
        operation._install(sock)
        operation._deadline = time.monotonic() + 2
        self.addCleanup(operation.abort)
        self.assertGreaterEqual(sock.fileno(), 2048)
        return operation, peer

    def test_high_socket_descriptor_read_and_write_readiness(self):
        operation, peer = self.high_fd_operation()
        operation._wait(False)
        operation._send(b"request")
        self.assertEqual(peer.recv(7), b"request")
        peer.sendall(b"response")
        operation._wait(True)
        self.assertEqual(operation._recv(8), b"response")

    def test_high_socket_descriptor_idle_wait_times_out(self):
        operation, _ = self.high_fd_operation()
        operation._deadline = time.monotonic() + 0.05
        with self.assertRaises(ProviderError) as captured:
            operation._wait(True)
        self.assertEqual(captured.exception.status_code, 60000)

    def test_abort_interrupts_high_socket_descriptor_wait(self):
        operation, _ = self.high_fd_operation()
        started, finished = threading.Event(), threading.Event()
        errors = []

        def wait():
            started.set()
            try:
                operation._wait(True)
            except Exception as exc:
                errors.append(exc)
            finally:
                finished.set()

        thread = threading.Thread(target=wait, daemon=True)
        thread.start()
        try:
            self.assertTrue(started.wait(1))
            self.assertFalse(finished.wait(0.05), "An idle socket must remain waiting before abort")
            operation.abort()
            self.assertTrue(finished.wait(1), "Abort must interrupt the socket wait")
        finally:
            operation.abort()
            thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], ProviderCancelled)

    def test_large_json_integers_are_preserved_in_http_requests_and_responses(self):
        before = getattr(sys, "get_int_max_str_digits", lambda: None)()
        integer = 10 ** 5000 + 7
        encoded = b"1" + b"0" * 4999 + b"7"
        body = b'{"error":{"code":' + encoded + b'}}'
        with provider_server(lambda sock: sock.sendall(response(body, status=429))) as (url, requests):
            operation = call(url, limit=20000)
            operation.request_body["input"] = {"large": integer, "negative": -integer, "text": "中文\ud800"}
            events = list(operation)
        self.assertEqual(events[0].status_code, 429)
        self.assertEqual(events[0].body["error"]["code"], integer)
        sent = _json.loads(requests[0][1])
        self.assertEqual(sent["input"], operation.request_body["input"])
        self.assertEqual(getattr(sys, "get_int_max_str_digits", lambda: None)(), before)

    def test_large_sse_integer_survives_provider_and_protocol_terminal_conversion(self):
        integer = 10 ** 5000
        encoded = b"1" + b"0" * 5000
        data = (b'data: {"type":"response.output_text.delta","sequence_number":' + encoded + b',"delta":"text"}\n\n'
                b'data: {"type":"response.failed","error":{"code":-' + encoded + b'}}\n\n')
        with provider_server(lambda sock: sock.sendall(response(data, "text/event-stream"))) as (url, _):
            events = list(call(url, True, "/v1/responses", limit=20000))
        self.assertEqual([event.kind for event in events], ["headers", "append", "failed"])
        self.assertEqual(events[1].body["sequence_number"], integer)
        terminal = InferEndInput(
            stream_id="big", request_seq=1, status_code=200, end_status="failed", body=events[2].body,
        )
        self.assertEqual(parse_payload(terminal.to_payload(2)).body["error"]["code"], -integer)

    def test_json_http_error_preserved_and_gateway_and_headers(self):
        body = b'{"error":{"message":"rate limit"}}'
        wire = response(body, "text/plain", b"X-Request-ID: one\r\nX-Request-ID: two\r\nX-Discard: omitted\r\n", 429)
        with provider_server(lambda sock: sock.sendall(wire)) as (url, requests):
            events = list(call(url))
        self.assertEqual(len(events), 1)
        self.assertEqual((events[0].kind, events[0].status_code, events[0].body), ("result", 429, json.loads(body)))
        self.assertEqual(events[0].headers, [{"name": "content-type", "value": "text/plain"}, {"name": "x-request-id", "value": "one"}, {"name": "x-request-id", "value": "two"}])
        self.assertTrue(requests[0][0].startswith(b"POST /gateway/v1/chat/completions HTTP/1.1\r\n"))
        self.assertEqual(requests[0][0].count(b"Authorization:"), 1)
        self.assertIn(b"Authorization: Bearer secret\r\n", requests[0][0])

    def test_sse_boundaries_multiline_comments_and_terminal(self):
        data = b'\xef\xbb\xbf: keepalive\r\ndata: {"a":\r\ndata: 1}\r\n\r\nevent: ignored\r\rdata: [DONE]\n\n'
        def send(sock):
            for byte in response(data, "Text/Event-Stream; charset=utf-8"):
                sock.sendall(bytes([byte]))
        with provider_server(send) as (url, _):
            events = list(call(url, True))
        self.assertEqual([e.kind for e in events], ["headers", "append", "completed"])
        self.assertEqual(events[1].body, {"a": 1})
        self.assertFalse(events[2].has_body)

    def test_split_leading_sse_bom_does_not_merge_empty_event_or_consume_limit(self):
        suffix = b"\ndata: {}\n\n"
        complete_event = b":" + b"a" * (4096 - 1 - len(suffix)) + suffix
        self.assertEqual(len(complete_event), 4096)
        for initial_empty_line in (b"", b"\n", b"\r\n", b"\r"):
            with self.subTest(initial_empty_line=initial_empty_line):
                data = b"\xef\xbb\xbf" + initial_empty_line + complete_event + b"data: [DONE]\n\n"
                with provider_server(lambda sock: sock.sendall(response(data, "text/event-stream"))) as (url, _):
                    operation = call(url, True)
                    stream = iter(operation)
                    head = next(stream)
                    recv = operation._recv
                    # Force the three BOM bytes into distinct real socket reads.
                    first_reads = iter((1, 1, 1))

                    def split_prefix(size):
                        return recv(min(size, next(first_reads, size)))

                    with patch.object(operation, "_recv", side_effect=split_prefix):
                        events = [head, *stream]
                self.assertEqual([event.kind for event in events], ["headers", "append", "completed"])
                self.assertEqual(events[1].body, {})

    def test_request_json_string_values_are_forwarded_losslessly(self):
        with provider_server(lambda sock: sock.sendall(response(b"{}"))) as (url, requests):
            operation = call(url)
            operation.request_body["input"] = "中文\ud800"
            list(operation)
        self.assertEqual(json.loads(requests[0][1])["input"], "中文\ud800")

    def test_responses_terminal_not_append(self):
        for terminal, expected in (("response.completed", "completed"),
                                   ("response.failed", "failed"),
                                   ("response.incomplete", "failed")):
            with self.subTest(terminal=terminal):
                payload = {"type": terminal, "sequence_number": 1,
                           "response": {"id": "r", "status": terminal.split(".")[1]}}
                if terminal == "response.incomplete":
                    payload["response"]["incomplete_details"] = {"reason": "max_output_tokens"}
                data = b"data: " + json.dumps(payload).encode() + b"\n\n"
                with provider_server(lambda sock: sock.sendall(response(data, "text/event-stream"))) as (url, _):
                    events = list(call(url, True, "/v1/responses"))
                self.assertEqual([e.kind for e in events], ["headers", expected])
                self.assertEqual(events[-1].status_code, 200)
                self.assertTrue(events[-1].has_body)
                self.assertEqual(events[-1].body, payload)

    def test_non_sse_stream_json_null_retains_body_presence(self):
        with provider_server(lambda sock: sock.sendall(response(b"null", status=429))) as (url, _):
            events = list(call(url, True))
        self.assertEqual([e.kind for e in events], ["headers", "completed"])
        self.assertEqual(events[-1].status_code, 429)
        self.assertTrue(events[-1].has_body)
        self.assertIsNone(events[-1].body)

    def test_json_utf8_compressed_input_and_sse_eof_failures(self):
        wires = [response(b"garbage"), response(b'"\xff"'), response(b"NaN"), response(b"1e999"), response(b"data: {}\n\n", "text/event-stream")]
        for wire in wires:
            with self.subTest(wire=wire):
                with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
                    with self.assertRaises(ProviderError) as error:
                        list(call(url, stream=b"event-stream" in wire))
                self.assertEqual(error.exception.status_code, 60007)

    def test_limits_before_header_filtering_and_during_decompression(self):
        wires = [
            b"HTTP/1.1 200 OK\r\nX-Unrecorded: " + b"x" * 4096 + b"\r\n\r\n{}",
            response(gzip.compress(b'"' + b"a" * 1000000 + b'"'), extra=b"Content-Encoding: gzip\r\n"),
            response(b":" + b"a" * 4094 + b"\r\n\r\n", "text/event-stream"),
        ]
        for index, wire in enumerate(wires):
            with self.subTest(index=index):
                with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
                    with self.assertRaises(ProviderError) as error:
                        list(call(url, stream=index == 2))
                self.assertEqual(error.exception.status_code, 60008)

    def test_sse_normalized_size_and_incremental_chunked_gzip(self):
        data = b"data: 1\r\n\r\ndata: [DONE]\r\n\r\n"
        compressed = gzip.compress(data)
        wire = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n" + b"".join(f"{len(part):x}\r\n".encode() + part + b"\r\n" for part in (compressed[:5], compressed[5:])) + b"0\r\n\r\n"
        with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
            events = list(call(url, True))
        self.assertEqual([e.kind for e in events], ["headers", "append", "completed"])
        self.assertEqual(events[1].body, 1)

    def test_chunk_extension_quoted_tab_is_ignored(self):
        wire = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Transfer-Encoding: chunked\r\n\r\n"
                b'2;audit="a\tb"\r\n{}\r\n0\r\n\r\n')
        with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
            events = list(call(url))
        self.assertEqual([(event.kind, event.status_code, event.body) for event in events],
                         [("result", 200, {})])

    def test_chunk_extensions_handle_quotes_escapes_and_last_chunk(self):
        quoted = b'quote' + b'\\"' + b';slash' + b'\\\\' + b';tab' + b'\\\t'
        wire = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Transfer-Encoding: chunked\r\n\r\n"
                b'1;flag;audit="a\t;b"\r\n{\r\n'
                b'1;token=ok;escaped="' + quoted + b'";empty=""\r\n}\r\n'
                b'0;finished="yes;\tend"\r\nX-Audit: ignored\r\n\r\n')
        with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
            events = list(call(url))
        self.assertEqual(events[0].body, {})
        self.assertEqual(events[0].headers, [{"name": "content-type", "value": "application/json"}])

    def test_invalid_chunk_extensions_remain_protocol_errors(self):
        for extension in (b';audit="unterminated', b';audit="bad\x00value"',
                          b';audit=', b';=value', b';audit="bad\\\rvalue"'):
            with self.subTest(extension=extension):
                wire = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                        b"Transfer-Encoding: chunked\r\n\r\n2" + extension + b"\r\n{}\r\n0\r\n\r\n")
                with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
                    with self.assertRaises(ProviderError) as captured:
                        list(call(url))
                self.assertEqual(captured.exception.status_code, 60007)

    def test_limits_count_normalized_sse_utf8_and_header_fields(self):
        # Wire CRLF bytes exceed the limit, while normalized LF bytes fit exactly.
        data = b":" + b"a" * 4093 + b"\r\n\r\ndata: [DONE]\r\n\r\n"
        with provider_server(lambda sock: sock.sendall(response(data, "text/event-stream"))) as (url, _):
            events = list(call(url, True))
        self.assertEqual([e.kind for e in events], ["headers", "completed"])
        # HTTP separators are not counted, and Latin-1 field bytes count after
        # conversion to UTF-8 (each non-ASCII byte occupies two UTF-8 bytes).
        for extra in (b"X: " + b"x" * 4095, b"X: " + b"\xe9" * 2047 + b"x"):
            wire = b"HTTP/1.1 200 OK\r\n" + extra + b"\r\n\r\n{}"
            with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
                self.assertEqual(list(call(url))[0].body, {})

    def test_body_progress_resets_idle_and_early_eof_is_connection_failure(self):
        def send(sock):
            sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n")
            for part in (b'"', b"a", b"b", b"c", b'"'):
                sock.sendall(part)
                time.sleep(0.015)
        with provider_server(send) as (url, _):
            self.assertEqual(list(call(url, idle_ms=40))[0].body, "abc")
        with provider_server(lambda sock: sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n{}")) as (url, _):
            with self.assertRaises(ProviderError) as error:
                list(call(url))
        self.assertEqual(error.exception.status_code, 60003)

    def test_publish_pause_does_not_consume_idle_budget(self):
        wire = response(b"data: 1\n\ndata: [DONE]\n\n", "text/event-stream")
        with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
            stream = iter(call(url, True, idle_ms=30))
            self.assertEqual(next(stream).kind, "headers")
            time.sleep(0.06)
            self.assertEqual(next(stream).kind, "append")
            time.sleep(0.06)
            self.assertEqual(next(stream).kind, "completed")

    def test_heartbeat_does_not_extend_sse_idle_budget(self):
        def send(sock):
            sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
            for _ in range(20):
                sock.sendall(b":heartbeat\n\n")
                time.sleep(0.01)
        with provider_server(send) as (url, _):
            with self.assertRaises(ProviderError) as error:
                list(call(url, True, idle_ms=40))
        self.assertEqual(error.exception.status_code, 60000)

    def test_abort_unblocks_response_and_dns_wait(self):
        for dns in (False, True):
            ready, release = threading.Event(), threading.Event()
            def delayed(*args, **kwargs):
                ready.set()
                release.wait(2)
                return []
            with provider_server(lambda sock: delayed()) as (url, _):
                operation = call(url, header_ms=2000)
                errors = []
                def consume():
                    try:
                        list(operation)
                    except Exception as exc:
                        errors.append(exc)
                with patch("openevent.model_proxy.provider.socket.getaddrinfo", side_effect=delayed) if dns else nullcontext():
                    thread = threading.Thread(target=consume, daemon=True)
                    thread.start()
                    self.assertTrue(ready.wait(1))
                    operation.abort()
                    thread.join(0.5)
                    release.set()
                self.assertFalse(thread.is_alive())
                self.assertIsInstance(errors[0], ProviderCancelled)

    def test_dns_failure_and_dns_deadline_are_distinct(self):
        with patch("openevent.model_proxy.provider.socket.getaddrinfo", side_effect=socket.gaierror("dns failure")):
            with self.assertRaises(ProviderError) as error:
                list(call("http://missing.invalid"))
        self.assertEqual(error.exception.status_code, 60001)
        release = threading.Event()
        with patch("openevent.model_proxy.provider.socket.getaddrinfo", side_effect=lambda *a, **k: release.wait(1)):
            with self.assertRaises(ProviderError) as error:
                list(call("http://missing.invalid", header_ms=30))
        release.set()
        self.assertEqual(error.exception.status_code, 60000)
