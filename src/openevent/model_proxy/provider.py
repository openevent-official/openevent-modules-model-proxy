"""Bounded, cancellable HTTP/1.1 and SSE reader for the two provider endpoints.

The generator stops reading while its caller publishes an event. There are no
HTTP retries and no background response readers.
"""

from dataclasses import dataclass, field
import errno
import math
import selectors
import socket
import ssl
import threading
import time
from urllib.parse import quote, urlsplit
import zlib

from openevent.model_proxy_sdk import _json as json


_CODES = {60000: "MODEL_API_TIMEOUT", 60001: "MODEL_API_DNS_ERROR", 60002: "MODEL_API_TLS_ERROR", 60003: "MODEL_API_CONNECTION_ERROR", 60007: "MODEL_PROXY_INTERRUPTED", 60008: "PAYLOAD_TOO_LARGE"}
_DNS_SLOTS = threading.BoundedSemaphore(16)
_TOKEN = frozenset(b"!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")


class ProviderError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.error_code = _CODES[status_code]
        self.message = message
        super().__init__(message)


class ProviderCancelled(Exception):
    """The local HTTP operation was aborted; the cancel event is the terminal."""


@dataclass(frozen=True)
class ProviderEvent:
    kind: str
    status_code: int
    headers: list[dict[str, str]] = field(default_factory=list)
    body: object = None
    has_body: bool = False


def _json(data):
    def invalid_constant(value):
        raise ValueError(f"invalid JSON constant {value}")

    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("Provider JSON number is out of range")
        return number

    try:
        return json.loads(data.decode("utf-8"), parse_constant=invalid_constant, parse_float=finite_float)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ProviderError(60007, "Provider response is not valid UTF-8 JSON") from exc


class ProviderCall:
    def __init__(self, config, request_body, path, max_payload_bytes):
        self.config = config
        self.request_body = request_body
        self.path = path
        self.limit = max_payload_bytes
        self._cancelled = threading.Event()
        self._lock = threading.Lock()
        self._socket = None
        self._deadline = 0.0
        self._started = False

    def abort(self):
        """Never wait for the HTTP task, DNS resolver or a remote cancellation."""
        self._cancelled.set()
        self._close()

    def _close(self):
        with self._lock:
            sock, self._socket = self._socket, None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

    def _check(self):
        if self._cancelled.is_set():
            raise ProviderCancelled()
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise ProviderError(60000, "Provider response timed out")
        return remaining

    def _install(self, sock):
        with self._lock:
            if self._cancelled.is_set():
                sock.close()
                raise ProviderCancelled()
            self._socket = sock

    def _wait(self, reading):
        self._check()
        sock = self._socket
        if sock is None:
            self._check()
            raise ProviderError(60003, "Provider connection closed")
        try:
            # Use the platform selector without select()'s descriptor-number limit.
            with selectors.DefaultSelector() as selector:
                selector.register(sock, selectors.EVENT_READ if reading else selectors.EVENT_WRITE)
                while True:
                    remaining = self._check()
                    if selector.select(min(remaining, 0.1)):
                        self._check()
                        return
        except (OSError, ValueError) as exc:
            self._check()
            raise ProviderError(60003, "Provider connection closed") from exc

    def _io(self, operation, reading):
        while True:
            self._check()
            try:
                return operation()
            except ssl.SSLWantReadError:
                self._wait(True)
            except ssl.SSLWantWriteError:
                self._wait(False)
            except (BlockingIOError, InterruptedError):
                self._wait(reading)

    def _resolve(self, host, port):
        # libc resolution cannot be interrupted. Bound its outstanding daemon
        # tasks and stop waiting on deadline/cancel, including queueing time.
        while not _DNS_SLOTS.acquire(timeout=min(self._check(), 0.05)):
            pass
        done, result = threading.Event(), []

        def resolve():
            try:
                result.append(socket.getaddrinfo(host, port, type=socket.SOCK_STREAM))
            except OSError as exc:
                result.append(exc)
            finally:
                _DNS_SLOTS.release()
                done.set()

        threading.Thread(target=resolve, daemon=True, name="provider-dns").start()
        while not done.wait(min(self._check(), 0.05)):
            pass
        self._check()
        if isinstance(result[0], OSError):
            raise ProviderError(60001, "DNS resolution failed") from result[0]
        return result[0]

    def _connect(self, url):
        host = url.hostname.encode("idna").decode("ascii")
        port = url.port or (443 if url.scheme == "https" else 80)
        addresses = self._resolve(host, port)
        for family, socktype, proto, _, address in addresses:
            sock = socket.socket(family, socktype, proto)
            sock.setblocking(False)
            self._install(sock)
            try:
                result = sock.connect_ex(address)
                if result not in (0, errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY, errno.EINTR):
                    raise OSError(result, "connect failed")
                if result:
                    self._wait(False)
                    result = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                    if result:
                        raise OSError(result, "connect failed")
                break
            except OSError:
                self._check()
                self._close()
        else:
            raise ProviderError(60003, "Provider connection failed")
        if url.scheme == "https":
            try:
                wrapped = ssl.create_default_context().wrap_socket(sock, server_hostname=host, do_handshake_on_connect=False)
                self._install(wrapped)
                self._io(wrapped.do_handshake, True)
            except (ssl.SSLError, OSError, ValueError) as exc:
                self._check()
                raise ProviderError(60002, "TLS handshake failed") from exc

    def _send(self, data):
        view = memoryview(data)
        while view:
            sock = self._socket
            if sock is None:
                self._check()
            count = self._io(lambda: sock.send(view), False)
            if not count:
                raise ProviderError(60003, "Provider connection closed while sending")
            view = view[count:]

    def _recv(self, size):
        sock = self._socket
        if sock is None:
            self._check()
        return self._io(lambda: sock.recv(size), True)

    def _byte(self):
        value = self._recv(1)
        if not value:
            raise ProviderError(60003, "Provider response ended prematurely")
        return value[0]

    def _finish_line(self, char):
        if char != 13 or self._byte() != 10:
            raise ProviderError(60007, "Invalid HTTP line ending")

    def _status(self):
        prefix = bytes(self._byte() for _ in range(12))
        if prefix[:9] not in {b"HTTP/1.0 ", b"HTTP/1.1 "} or not prefix[9:].isdigit() or not 100 <= int(prefix[9:]) <= 599:
            raise ProviderError(60007, "Invalid HTTP status")
        char = self._byte()
        if char == 32:
            char = self._byte()
            while char != 13:
                if char == 10 or (char < 32 and char != 9) or char == 127:
                    raise ProviderError(60007, "Invalid HTTP reason phrase")
                char = self._byte()
        self._finish_line(char)
        return int(prefix[9:])

    def _chunk_size(self):
        def whitespace(char):
            while char in (9, 32):
                char = self._byte()
            return char

        def token(char):
            if char not in _TOKEN:
                raise ProviderError(60007, "Invalid HTTP chunk extension")
            while char in _TOKEN:
                char = self._byte()
            return char

        char, size = self._byte(), 0
        if char not in b"0123456789abcdefABCDEF":
            raise ProviderError(60007, "Invalid HTTP chunk size")
        while char in b"0123456789abcdefABCDEF":
            size = size * 16 + int(chr(char), 16)
            char = self._byte()
        char = whitespace(char)
        while char != 13:
            if char != 59:
                raise ProviderError(60007, "Invalid HTTP chunk size")
            char = whitespace(token(whitespace(self._byte())))
            if char == 61:
                char = whitespace(self._byte())
                if char == 34:
                    char = self._byte()
                    while char != 34:
                        if char == 92:
                            char = self._byte()
                        if (char < 32 and char != 9) or char == 127:
                            raise ProviderError(60007, "Invalid HTTP quoted chunk extension")
                        char = self._byte()
                    char = self._byte()
                else:
                    char = token(char)
                char = whitespace(char)
            # Extensions affect neither the body nor response metadata. Scan
            # quoted values before looking for the next semicolon or CRLF.
        self._finish_line(char)
        return size

    def _trailers(self):
        while True:
            char = self._byte()
            if char == 13:
                self._finish_line(char)
                return
            while char != 13:
                if char == 10 or (char < 32 and char != 9) or char == 127:
                    raise ProviderError(60007, "Invalid HTTP trailer")
                char = self._byte()
            self._finish_line(char)

    def _headers(self):
        # Read names and values byte by byte. No library first materializes an
        # unbounded header block. HTTP field bytes are decoded using ISO-8859-1.
        status = self._status()
        headers, size = [], 0
        while True:
            first = self._byte()
            if first == 13:
                if self._byte() != 10:
                    raise ProviderError(60007, "Invalid HTTP header ending")
                return status, headers
            name, value = bytearray(), bytearray()
            char = first
            while char != 58:
                if char not in _TOKEN:
                    raise ProviderError(60007, "Invalid HTTP header name")
                size += 1
                if size > self.limit:
                    headers.clear()
                    name.clear()
                    raise ProviderError(60008, "Provider response headers exceed max_payload_bytes")
                name.append(char)
                char = self._byte()
            if not name:
                raise ProviderError(60007, "Invalid HTTP header name")
            leading = True
            while True:
                char = self._byte()
                if char == 13:
                    if self._byte() != 10:
                        raise ProviderError(60007, "Invalid HTTP header ending")
                    break
                if char == 10 or (char < 32 and char != 9) or char == 127:
                    raise ProviderError(60007, "Invalid HTTP header value")
                if leading and char in (9, 32):
                    continue
                leading = False
                size += 1 if char < 128 else 2
                if size > self.limit:
                    headers.clear()
                    name.clear()
                    value.clear()
                    raise ProviderError(60008, "Provider response headers exceed max_payload_bytes")
                value.append(char)
            headers.append((name.decode("ascii").lower(), value.decode("latin-1")))

    def _raw_body(self, headers, status, allowance):
        transfer = ",".join(v.strip().lower() for k, v in headers if k == "transfer-encoding")
        lengths = [v.strip() for k, v in headers if k == "content-length"]
        if status in (204, 304):
            return
        if transfer:
            if transfer != "chunked":
                raise ProviderError(60007, "Unsupported HTTP transfer encoding")
            while True:
                chunk_size = self._chunk_size()
                if not chunk_size:
                    # Trailers do not change the original response metadata.
                    # Parse without retaining their contents.
                    self._trailers()
                    return
                while chunk_size:
                    chunk = self._recv(min(chunk_size, 8192, allowance()))
                    if not chunk:
                        raise ProviderError(60003, "Provider response ended in a chunk")
                    chunk_size -= len(chunk)
                    yield chunk
                if self._byte() != 13 or self._byte() != 10:
                    raise ProviderError(60007, "Invalid HTTP chunk ending")
        elif lengths:
            if len(set(lengths)) != 1 or not lengths[0].isascii() or not lengths[0].isdigit():
                raise ProviderError(60007, "Invalid HTTP Content-Length")
            length = int(lengths[0])
            while length:
                chunk = self._recv(min(length, 8192, allowance()))
                if not chunk:
                    raise ProviderError(60003, "Provider response ended before Content-Length")
                length -= len(chunk)
                yield chunk
        else:
            while True:
                chunk = self._recv(min(8192, allowance()))
                if not chunk:
                    return
                yield chunk

    def _decoded_body(self, headers, status, allowance, streaming):
        encoding = ",".join(v.strip().lower() for k, v in headers if k == "content-encoding")
        if encoding in ("", "identity"):
            decoder = None
        elif encoding in ("gzip", "x-gzip", "deflate"):
            decoder = zlib.decompressobj(31 if encoding != "deflate" else 15)
        else:
            raise ProviderError(60007, "Unsupported HTTP content encoding")
        raw_allowance = allowance if decoder is None else lambda: 8192
        try:
            for raw in self._raw_body(headers, status, raw_allowance):
                if not streaming:
                    self._deadline = time.monotonic() + self.config.timeout.idle_ms / 1000
                if decoder is None:
                    yield raw
                    continue
                while raw:
                    decoded = decoder.decompress(raw, max(1, allowance()))
                    raw = decoder.unconsumed_tail
                    if decoded:
                        yield decoded
                    if decoder.unused_data:
                        # Concatenated gzip members are one decoded HTTP body.
                        if encoding in ("gzip", "x-gzip"):
                            raw = decoder.unused_data
                            decoder = zlib.decompressobj(31)
                        else:
                            raise ProviderError(60007, "Extra compressed response data")
            if decoder is not None and not decoder.eof:
                raise ProviderError(60007, "Incomplete compressed response")
        except zlib.error as exc:
            raise ProviderError(60007, "Invalid compressed response") from exc

    def _json_body(self, headers, status):
        body = bytearray()
        for part in self._decoded_body(headers, status, lambda: self.limit - len(body) + 1, False):
            if len(body) + len(part) > self.limit:
                body.clear()
                raise ProviderError(60008, "Provider response body exceeds max_payload_bytes")
            body.extend(part)
        return _json(body)

    def _sse(self, headers, status):
        event = bytearray()
        line_start, previous_cr = 0, False
        prefix = bytearray()
        for chunk in self._decoded_body(headers, status, lambda: self.limit - len(event) + 1, True):
            if prefix is not None:
                # A stream BOM precedes SSE lines and must not affect event
                # boundaries or their sizes, even when split across reads.
                needed = 3 - len(prefix)
                prefix.extend(chunk[:needed])
                chunk = chunk[needed:]
                if len(prefix) < 3:
                    continue
                if prefix != b"\xef\xbb\xbf":
                    chunk = bytes(prefix) + chunk
                prefix = None
            for byte in chunk:
                if previous_cr:
                    previous_cr = False
                    if byte == 10:
                        continue
                if byte == 13:
                    byte, previous_cr = 10, True
                if len(event) >= self.limit:
                    event.clear()
                    raise ProviderError(60008, "Provider SSE event exceeds max_payload_bytes")
                event.append(byte)
                if byte != 10:
                    continue
                empty_line = len(event) - 1 == line_start
                line_start = len(event)
                if not empty_line:
                    continue
                try:
                    text = event.decode("utf-8")
                    lines = text.split("\n")
                except UnicodeError as exc:
                    raise ProviderError(60007, "Provider SSE event is not UTF-8") from exc
                event.clear()
                line_start = 0
                data = []
                for line in lines:
                    key, _, value = line.partition(":")
                    if key == "data":
                        data.append(value[1:] if value.startswith(" ") else value)
                if not data:
                    continue
                value = "\n".join(data)
                if self.path == "/v1/chat/completions" and value == "[DONE]":
                    self._deadline = time.monotonic() + self.config.timeout.idle_ms / 1000
                    yield ProviderEvent("completed", status)
                    return
                parsed = _json(value.encode("utf-8"))
                self._deadline = time.monotonic() + self.config.timeout.idle_ms / 1000
                if self.path == "/v1/responses":
                    kind = parsed["type"]
                    if kind in {"response.completed", "response.failed", "response.incomplete"}:
                        yield ProviderEvent("completed" if kind == "response.completed" else "failed", status, body=parsed, has_body=True)
                        return
                yield ProviderEvent("append", status, body=parsed, has_body=True)
        raise ProviderError(60007, "Provider SSE ended without a terminal event")

    def __iter__(self):
        if self._started:
            raise RuntimeError("ProviderCall can only be consumed once")
        self._started = True
        self._deadline = time.monotonic() + self.config.timeout.response_header_ms / 1000
        try:
            url = urlsplit(self.config.base_url.rstrip("/") + self.path)
            self._connect(url)
            body = json.dumps(self.request_body, ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
            host = url.hostname.encode("idna").decode("ascii")
            if ":" in host:
                host = "[" + host + "]"
            if url.port is not None:
                host += ":" + str(url.port)
            target = quote(url.path, safe="/%:@!$&'()*+,;=-._~") or "/"
            head = (f"POST {target} HTTP/1.1\r\nHost: {host}\r\nAuthorization: Bearer {self.config.api_key}\r\nContent-Type: application/json\r\nAccept-Encoding: gzip, deflate\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode("utf-8")
            self._send(head)
            self._send(body)
            status, raw_headers = self._headers()
            while 100 <= status < 200 and status != 101:
                status, raw_headers = self._headers()
            self._deadline = time.monotonic() + self.config.timeout.idle_ms / 1000
            headers = [{"name": k, "value": v} for k, v in raw_headers if k in {"content-type", "retry-after", "x-request-id"} or k.startswith("x-ratelimit-")]
            if not self.request_body.get("stream", False):
                yield ProviderEvent("result", status, headers, self._json_body(raw_headers, status), True)
                return
            pause = time.monotonic()
            yield ProviderEvent("headers", status, headers)
            self._deadline += time.monotonic() - pause
            media_types = [v.split(";", 1)[0].strip().lower() for k, v in raw_headers if k == "content-type"]
            if media_types and media_types[0] == "text/event-stream":
                for event in self._sse(raw_headers, status):
                    pause = time.monotonic()
                    yield event
                    self._deadline += time.monotonic() - pause
            else:
                yield ProviderEvent("completed", status, body=self._json_body(raw_headers, status), has_body=True)
        except (OSError, ssl.SSLError) as exc:
            self._check()
            raise ProviderError(60003, "Provider connection failed or reset") from exc
        finally:
            self._close()
