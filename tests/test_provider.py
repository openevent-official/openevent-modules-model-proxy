from contextlib import contextmanager, nullcontext
import gzip
import json
import socket
import socketserver
import threading
import time
import unittest
from unittest.mock import patch

import msgspec

from openevent.model_proxy.config import ProviderConfig, ProviderTimeout
from openevent.model_proxy.provider import ProviderCall, ProviderCancelled, ProviderError
from openevent.model_proxy_sdk import InferRequestInput, parse_payload, prepare_payload
from openevent.model_proxy_sdk import _json
from openai_fixtures import (
    chat_body, chat_chunks, chat_request, http_error_body, response_body,
    response_end_events, response_start_events, responses_request,
)
from test_e2e import fake_provider


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
    body = _json.raw_body(_json.dump_bytes({"model": "m", "stream": stream}))
    return ProviderCall(config, body, path, limit, streaming=stream)


class ProviderTests(unittest.TestCase):
    def test_json_http_error_preserved_and_gateway_and_headers(self):
        body = b'{"error":{"message":"rate limit"}}'
        wire = response(body, "text/plain", b"X-Request-ID: one\r\nX-Request-ID: two\r\nX-Discard: omitted\r\n", 429)
        with provider_server(lambda sock: sock.sendall(wire)) as (url, requests):
            events = list(call(url))
        self.assertEqual(len(events), 1)
        self.assertEqual((events[0].kind, events[0].status_code, _json.body_copy(events[0].body)),
                         ("result", 429, json.loads(body)))
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
        self.assertEqual(_json.body_copy(events[1].body), {"a": 1})
        self.assertIsNone(events[0].body)
        self.assertIsNone(events[2].body)

    def test_split_leading_sse_bom_does_not_merge_empty_event_or_consume_limit(self):
        suffix = b"\ndata: {}\n\n"
        complete_event = b":" + b"a" * (4096 - 1 - len(suffix)) + suffix
        self.assertEqual(len(complete_event), 4096)
        for initial_empty_line in (b"", b"\n", b"\r\n", b"\r"):
            with self.subTest(initial_empty_line=initial_empty_line):
                data = b"\xef\xbb\xbf" + initial_empty_line + complete_event + b"data: [DONE]\n\n"
                release = threading.Event()
                self.addCleanup(release.set)

                def send(sock):
                    wire = response(data, "text/event-stream")
                    sock.sendall(wire[:-len(data)])
                    release.wait(2)
                    sock.sendall(data)

                with provider_server(send) as (url, _):
                    operation = call(url, True)
                    stream = iter(operation)
                    head = next(stream)
                    recv = operation._recv
                    # Force the three BOM bytes into distinct real socket reads.
                    first_reads = iter((1, 1, 1))

                    def split_prefix(size):
                        return recv(min(size, next(first_reads, size)))

                    with patch.object(operation, "_recv", side_effect=split_prefix):
                        release.set()
                        events = [head, *stream]
                self.assertEqual([event.kind for event in events], ["headers", "append", "completed"])
                self.assertEqual(_json.body_copy(events[1].body), {})

    def test_request_unicode_and_json_escapes_reach_provider(self):
        body = {"input": "中文é😀\x7f", "escaped": '"\\\n\x00'}
        request = InferRequestInput(stream_id="unicode", method="POST",
                                    path="/v1/chat/completions", body=body)
        parsed = parse_payload(prepare_payload(request).payload)
        with provider_server(lambda sock: sock.sendall(response(b"{}"))) as (url, requests):
            config = ProviderConfig("openai_compatible", url, "密钥", ProviderTimeout(1000, 1000))
            with patch("openevent.model_proxy_sdk._json.request_stream",
                       side_effect=AssertionError("Provider must reuse the parsed stream flag")):
                operation = ProviderCall(config, parsed._data["body"], parsed.path, 4096,
                                         streaming=parsed._streaming)
                events = list(operation)
        headers, sent = requests[0]
        self.assertEqual(sent, bytes(parsed._data["body"]))
        self.assertIsInstance(events[0].body, msgspec.Raw)
        self.assertEqual(json.loads(sent), body)
        self.assertIn('"input":"中文é😀\x7f"'.encode("utf-8"), sent)
        self.assertIn(b'"escaped":"\\\"\\\\\\n\\u0000"', sent)
        self.assertIn(f"Content-Length: {len(sent)}\r\n".encode("ascii"), headers)
        self.assertIn("Authorization: Bearer 密钥\r\n".encode(), headers)

    def test_openai_fixtures_preserved_and_terminal_not_append(self):
        cases = [
            ("chat JSON", "/v1/chat/completions", chat_request(),
             [("result", 200, chat_body())]),
            ("responses JSON", "/v1/responses", responses_request(),
             [("result", 200, response_body())]),
            ("chat stream", "/v1/chat/completions", chat_request(stream=True),
             [("headers", 200, None)] + [("append", 200, body) for body in chat_chunks()]
             + [("completed", 200, None)]),
            ("non-SSE stream", "/v1/responses",
             responses_request(stream=True, metadata={"test_mode": "json"}),
             [("headers", 200, None), ("completed", 200, response_body())]),
        ]
        for status, terminal in (("completed", "completed"), ("failed", "failed"),
                                 ("incomplete", "failed")):
            bodies = response_start_events() + response_end_events(status=status)
            cases.append((status, "/v1/responses",
                          responses_request(stream=True, metadata={"test_mode": status}),
                          [("headers", 200, None)]
                          + [("append", 200, body) for body in bodies[:-1]]
                          + [(terminal, 200, bodies[-1])]))
        for streaming in (False, True):
            cases.append((f"HTTP error, stream={streaming}", "/v1/responses",
                          responses_request(stream=streaming, metadata={"test_mode": "http_error"}),
                          [("headers", 429, None), ("completed", 429, http_error_body())]
                          if streaming else [("result", 429, http_error_body())]))

        with fake_provider() as provider:
            config = ProviderConfig("openai_compatible", f"http://127.0.0.1:{provider.server_port}",
                                    "secret", ProviderTimeout(1000, 1000))
            for name, path, body, expected in cases:
                with self.subTest(case=name):
                    operation = ProviderCall(config, _json.raw_body(_json.dump_bytes(body)), path,
                                             8192, streaming=body.get("stream", False))
                    events = list(operation)
                    self.assertEqual([(event.kind, event.status_code,
                                       None if event.body is None else _json.body_copy(event.body))
                                      for event in events], expected)
                    self.assertEqual(provider.requests[-1][:2], (path, body))
            self.assertEqual(len(provider.requests), len(cases))

    def test_json_utf8_compressed_input_and_sse_eof_failures(self):
        wires = [response(b"garbage"), response(b'"\xff"'), response(b"NaN"), response(b"data: {}\n\n", "text/event-stream")]
        for wire in wires:
            with self.subTest(wire=wire):
                with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
                    with self.assertRaises(ProviderError) as error:
                        list(call(url, stream=b"event-stream" in wire))
                self.assertEqual(error.exception.status_code, 60007)

    def test_input_limits_during_decompression_and_sse_parsing(self):
        cases = [
            (False, response(gzip.compress(b'{"output":"' + b"a" * 1000000 + b'"}'), extra=b"Content-Encoding: gzip\r\n")),
            (True, response(b":" + b"a" * 4094 + b"\r\n\r\n", "text/event-stream")),
        ]
        for streaming, wire in cases:
            with self.subTest(streaming=streaming):
                with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
                    with self.assertRaises(ProviderError) as error:
                        list(call(url, stream=streaming))
                self.assertEqual(error.exception.status_code, 60008)

    def test_sse_normalized_size_and_incremental_chunked_gzip(self):
        data = b'data: {"choices":[]}\r\n\r\ndata: [DONE]\r\n\r\n'
        compressed = gzip.compress(data)
        wire = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n" + b"".join(f"{len(part):x}\r\n".encode() + part + b"\r\n" for part in (compressed[:5], compressed[5:])) + b"0\r\n\r\n"
        with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
            events = list(call(url, True))
        self.assertEqual([e.kind for e in events], ["headers", "append", "completed"])
        self.assertEqual(_json.body_copy(events[1].body), {"choices": []})

    def test_chunked_json_response_with_extension(self):
        wire = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Transfer-Encoding: chunked\r\n\r\n"
                b"2;trace=sample\r\n{}\r\n0\r\n\r\n")
        with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
            events = list(call(url))
        self.assertEqual([(event.kind, event.status_code, _json.body_copy(event.body)) for event in events],
                         [("result", 200, {})])

    def test_sse_limit_counts_normalized_utf8_bytes(self):
        # Wire CRLF bytes exceed the limit, while normalized LF bytes fit exactly.
        data = b":" + b"a" * 4093 + b"\r\n\r\ndata: [DONE]\r\n\r\n"
        with provider_server(lambda sock: sock.sendall(response(data, "text/event-stream"))) as (url, _):
            events = list(call(url, True))
        self.assertEqual([e.kind for e in events], ["headers", "completed"])

    def test_header_limit_counts_complete_wire_block_before_filtering(self):
        prefix, suffix = b"HTTP/1.1 200 OK\r\nX-Ignored: ", b"\r\n\r\n"
        for byte in (b"x", b"\xe9"):
            for excess in (0, 1):
                with self.subTest(byte=byte, excess=excess):
                    headers = prefix + byte * (4096 - len(prefix) - len(suffix) + excess) + suffix
                    self.assertEqual(len(headers), 4096 + excess)
                    with provider_server(lambda sock: sock.sendall(headers + b"{}")) as (url, _):
                        if excess:
                            with self.assertRaises(ProviderError) as error:
                                list(call(url))
                            self.assertEqual(error.exception.status_code, 60008)
                        else:
                            self.assertEqual(_json.body_copy(list(call(url))[0].body), {})

    def test_header_limit_accumulates_multiple_fields(self):
        fields = b"X-Ignored: " + b"x" * 100 + b"\r\n"
        wire = b"HTTP/1.1 200 OK\r\n" + fields * 40 + b"\r\n{}"
        with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
            with self.assertRaises(ProviderError) as error:
                list(call(url))
        self.assertEqual(error.exception.status_code, 60008)

    def test_informational_responses_share_header_limit_with_final_response(self):
        prefix = (b"HTTP/1.1 100 Continue\r\nX-Interim: first\r\n\r\n"
                  b"HTTP/1.1 103 Early Hints\r\nLink: </static>; rel=preload\r\n\r\n"
                  b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-Ignored: ")
        suffix = b"\r\n\r\n"
        for excess in (0, 1):
            with self.subTest(excess=excess):
                headers = prefix + b"x" * (4096 - len(prefix) - len(suffix) + excess) + suffix
                with provider_server(lambda sock: sock.sendall(headers + b"{}")) as (url, _):
                    if excess:
                        with self.assertRaises(ProviderError) as error:
                            list(call(url))
                        self.assertEqual(error.exception.status_code, 60008)
                    else:
                        result = list(call(url))[0]
                        self.assertEqual((result.status_code, _json.body_copy(result.body)), (200, {}))

    def test_unterminated_oversized_header_fails_without_waiting_for_newline(self):
        release = threading.Event()

        def send(sock):
            sock.sendall(b"HTTP/1.1 200 OK\r\nX-Ignored: " + b"x" * 4096)
            release.wait(2)

        try:
            with provider_server(send) as (url, _):
                with self.assertRaises(ProviderError) as error:
                    list(call(url, header_ms=200))
            self.assertEqual(error.exception.status_code, 60008)
        finally:
            release.set()

    def test_body_progress_resets_idle_and_early_eof_is_connection_failure(self):
        for chunked in (False, True):
            with self.subTest(chunked=chunked):
                def send(sock):
                    framing = b"Transfer-Encoding: chunked\r\n\r\n10\r\n" if chunked else b"Content-Length: 16\r\n\r\n"
                    sock.sendall(b"HTTP/1.1 200 OK\r\n" + framing)
                    for part in (b'{"output":"', b"a", b"b", b"c", b'"}'):
                        sock.sendall(part)
                        time.sleep(0.015)
                    if chunked:
                        sock.sendall(b"\r\n0\r\n\r\n")
                with provider_server(send) as (url, _):
                    self.assertEqual(_json.body_copy(list(call(url, idle_ms=40))[0].body), {"output": "abc"})

        for framing in (b"Content-Length: 10\r\n\r\n{}",
                        b"Transfer-Encoding: chunked\r\n\r\na\r\n{}",
                        b"Transfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n"):
            with self.subTest(framing=framing):
                with provider_server(lambda sock: sock.sendall(b"HTTP/1.1 200 OK\r\n" + framing)) as (url, _):
                    with self.assertRaises(ProviderError) as error:
                        list(call(url))
                self.assertEqual(error.exception.status_code, 60003)

    def test_redirect_and_connection_failure_do_not_repeat_requests(self):
        with provider_server(lambda sock: sock.sendall(response(b"{}", extra=b"Location: /elsewhere\r\n", status=302))) as (url, requests):
            events = list(call(url))
            self.assertEqual(len(requests), 1)
        self.assertEqual(events[0].status_code, 302)
        self.assertEqual(_json.body_copy(events[0].body), {})

        with provider_server(lambda sock: None) as (url, requests):
            with self.assertRaises(ProviderError) as error:
                list(call(url))
            self.assertEqual(len(requests), 1)
        self.assertEqual(error.exception.status_code, 60003)

    def test_publish_pause_does_not_consume_idle_budget(self):
        wire = response(b'data: {"choices":[]}\n\ndata: [DONE]\n\n', "text/event-stream")
        with provider_server(lambda sock: sock.sendall(wire)) as (url, _):
            stream = iter(call(url, True, idle_ms=30))
            self.assertEqual(next(stream).kind, "headers")
            time.sleep(0.06)
            self.assertEqual(next(stream).kind, "append")
            time.sleep(0.06)
            self.assertEqual(next(stream).kind, "completed")

    def test_sse_yields_event_before_rest_of_response_arrives(self):
        first, last = b'data: {"choices":[]}\n\n', b"data: [DONE]\n\n"
        for chunked in (False, True):
            with self.subTest(chunked=chunked):
                release = threading.Event()

                def send(sock):
                    size = len(first) + len(last)
                    framing = (b"Transfer-Encoding: chunked\r\n\r\n" + f"{size:x}\r\n".encode()
                               if chunked else f"Content-Length: {size}\r\n\r\n".encode())
                    sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n" + framing + first)
                    release.wait(2)
                    sock.sendall(last + (b"\r\n0\r\n\r\n" if chunked else b""))

                try:
                    with provider_server(send) as (url, _):
                        stream = iter(call(url, True, idle_ms=500))
                        self.assertEqual(next(stream).kind, "headers")
                        event = next(stream)
                        self.assertEqual((event.kind, _json.body_copy(event.body)), ("append", {"choices": []}))
                        release.set()
                        self.assertEqual(next(stream).kind, "completed")
                        self.assertEqual(list(stream), [])
                finally:
                    release.set()

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

    def test_abort_unblocks_response_but_dns_finishes_when_resolver_returns(self):
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
                    try:
                        self.assertTrue(ready.wait(1))
                        started = time.monotonic()
                        operation.abort()
                        self.assertLess(time.monotonic() - started, 0.5)
                        thread.join(0.05 if dns else 0.5)
                        if dns:
                            self.assertTrue(thread.is_alive(), "System DNS resolution cannot be interrupted")
                        else:
                            self.assertFalse(thread.is_alive(), "Abort must interrupt the response read")
                    finally:
                        release.set()
                        thread.join(1)
                self.assertFalse(thread.is_alive())
                self.assertEqual(len(errors), 1)
                self.assertIsInstance(errors[0], ProviderCancelled)

    def test_dns_failure_and_expired_budget_after_resolution_are_distinct(self):
        with patch("openevent.model_proxy.provider.socket.getaddrinfo", side_effect=socket.gaierror("dns failure")):
            with self.assertRaises(ProviderError) as error:
                list(call("http://missing.invalid"))
        self.assertEqual(error.exception.status_code, 60001)
        ready, release, finished = threading.Event(), threading.Event(), threading.Event()
        errors = []

        def delayed(*args, **kwargs):
            ready.set()
            release.wait(2)
            return []

        def consume():
            try:
                list(call("http://missing.invalid", header_ms=30))
            except Exception as exc:
                errors.append(exc)
            finally:
                finished.set()

        with patch("openevent.model_proxy.provider.socket.getaddrinfo", side_effect=delayed):
            thread = threading.Thread(target=consume, daemon=True)
            thread.start()
            try:
                self.assertTrue(ready.wait(1))
                self.assertFalse(finished.wait(0.1), "The budget cannot interrupt system DNS resolution")
            finally:
                release.set()
                thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], ProviderError)
        self.assertEqual(errors[0].status_code, 60000)
