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
        peer.settimeout(1)
        operation = call("http://unused.invalid")
        operation._install(sock)
        operation._deadline = time.monotonic() + 2
        self.addCleanup(operation.abort)
        self.assertGreaterEqual(sock.fileno(), 2048)
        return operation, peer

    def test_high_socket_descriptor_reads_and_writes(self):
        operation, peer = self.high_fd_operation()
        operation._send(b"request")
        self.assertEqual(peer.recv(7), b"request")
        peer.sendall(b"response")
        self.assertEqual(operation._recv(8), b"response")

    def test_high_socket_descriptor_idle_read_times_out(self):
        operation, _ = self.high_fd_operation()
        operation._deadline = time.monotonic() + 0.05
        with self.assertRaises(TimeoutError):
            operation._recv(1)

    def test_abort_interrupts_high_socket_descriptor_read(self):
        operation, _ = self.high_fd_operation()
        started, finished = threading.Event(), threading.Event()
        errors = []

        def receive():
            started.set()
            try:
                operation._recv(1)
            except Exception as exc:
                errors.append(exc)
            finally:
                finished.set()

        thread = threading.Thread(target=receive, daemon=True)
        thread.start()
        try:
            self.assertTrue(started.wait(1))
            self.assertFalse(finished.wait(0.05), "An idle socket must remain waiting before abort")
            operation.abort()
            self.assertTrue(finished.wait(1), "Abort must interrupt the socket read")
        finally:
            operation.abort()
            thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], (ProviderCancelled, OSError))

    def test_large_json_integers_are_preserved_in_http_requests_and_responses(self):
        before = getattr(sys, "get_int_max_str_digits", lambda: None)()
        integer = 10 ** 5000 + 7
        encoded = b"1" + b"0" * 4999 + b"7"
        body = b'{"error":{"code":' + encoded + b'}}'
        request_input = {"large": integer, "negative": -integer, "text": "中文\ud800"}
        with provider_server(lambda sock: sock.sendall(response(body, status=429))) as (url, requests):
            operation = call(url, limit=20000)
            operation.request_body["input"] = request_input
            events = list(operation)
        self.assertEqual(events[0].status_code, 429)
        self.assertEqual(events[0].body["error"]["code"], integer)
        sent = _json.loads(requests[0][1])
        self.assertEqual(sent["input"], request_input)
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
                self.assertEqual(events[1].body, {})

    def test_request_unicode_values_are_forwarded_losslessly(self):
        with provider_server(lambda sock: sock.sendall(response(b"{}"))) as (url, requests):
            config = ProviderConfig("openai_compatible", url, "密钥", ProviderTimeout(1000, 1000))
            operation = ProviderCall(config, {"input": "中文\ud800"}, "/v1/chat/completions", 4096)
            list(operation)
        self.assertEqual(json.loads(requests[0][1])["input"], "中文\ud800")
        self.assertIn("Authorization: Bearer 密钥\r\n".encode(), requests[0][0])

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
                            self.assertEqual(list(call(url))[0].body, {})

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
                        self.assertEqual((result.status_code, result.body), (200, {}))

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
                    framing = b"Transfer-Encoding: chunked\r\n\r\n5\r\n" if chunked else b"Content-Length: 5\r\n\r\n"
                    sock.sendall(b"HTTP/1.1 200 OK\r\n" + framing)
                    for part in (b'"', b"a", b"b", b"c", b'"'):
                        sock.sendall(part)
                        time.sleep(0.015)
                    if chunked:
                        sock.sendall(b"\r\n0\r\n\r\n")
                with provider_server(send) as (url, _):
                    self.assertEqual(list(call(url, idle_ms=40))[0].body, "abc")

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
        self.assertEqual(events[0].body, {})

        with provider_server(lambda sock: None) as (url, requests):
            with self.assertRaises(ProviderError) as error:
                list(call(url))
            self.assertEqual(len(requests), 1)
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

    def test_sse_yields_event_before_rest_of_response_arrives(self):
        first, last = b"data: 1\n\n", b"data: [DONE]\n\n"
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
                        self.assertEqual((event.kind, event.body), ("append", 1))
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
