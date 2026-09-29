"""Bounded, cancellable HTTP/1.1 and SSE reader for the two provider endpoints.

The generator stops reading while its caller publishes an event. There are no
HTTP retries and no background response readers.
"""

from contextlib import closing
from dataclasses import dataclass, field
import http.client
import io
import math
import socket
import ssl
import threading
import time
from urllib.parse import quote, urlsplit
import zlib

from openevent.model_proxy_sdk import _json as json


class ProviderError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
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


class _SocketReader(io.RawIOBase):
    def __init__(self, call):
        self.call = call

    def readable(self):
        return True

    def readinto(self, buffer):
        data = self.call._recv(len(buffer))
        buffer[:len(data)] = data
        return len(data)


class _ResponseReader(io.BufferedReader):
    def __init__(self, call):
        super().__init__(_SocketReader(call), buffer_size=8192)
        self.header_remaining = call.limit

    def readline(self, size=-1):
        if self.header_remaining is None:
            return super().readline(size)
        bound = self.header_remaining + 1
        line = super().readline(min(size, bound) if size >= 0 else bound)
        self.header_remaining -= len(line)
        if self.header_remaining < 0:
            raise ProviderError(60008, "Provider response headers exceed max_payload_bytes")
        return line


class _HTTPTransport:
    """Expose cancellable I/O to http.client without parsing HTTP here."""

    def __init__(self, call):
        self.call = call

    def sendall(self, data):
        self.call._send(data)

    def makefile(self, mode):
        return _ResponseReader(self.call)

    def close(self):
        self.call._close()


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
        """Mark cancellation and close the socket without waiting for the task."""
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

    def _connect(self, url):
        host = url.hostname.encode("idna").decode("ascii")
        port = url.port or (443 if url.scheme == "https" else 80)
        self._check()
        try:
            addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except OSError as exc:
            self._check()
            raise ProviderError(60001, "DNS resolution failed") from exc
        self._check()
        for family, socktype, proto, _, address in addresses:
            sock = socket.socket(family, socktype, proto)
            self._install(sock)
            try:
                sock.settimeout(self._check())
                sock.connect(address)
                self._check()
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
                wrapped.settimeout(self._check())
                wrapped.do_handshake()
                self._check()
            except (ssl.SSLError, OSError, ValueError) as exc:
                self._check()
                raise ProviderError(60002, "TLS handshake failed") from exc

    def _send(self, data):
        remaining = self._check()
        sock = self._socket
        if sock is None:
            self._check()
        sock.settimeout(remaining)
        sock.sendall(data)
        self._check()

    def _recv(self, size):
        remaining = self._check()
        sock = self._socket
        if sock is None:
            self._check()
        sock.settimeout(remaining)
        data = sock.recv(size)
        self._check()
        return data

    def _raw_body(self, response, allowance):
        while True:
            self._check()
            # read1 returns available data without waiting to fill the buffer.
            chunk = response.read1(min(8192, allowance()))
            if not chunk:
                if response.length:
                    raise ProviderError(60003, "Provider response ended before Content-Length")
                return
            yield chunk

    def _decoded_body(self, response, allowance, streaming):
        encoding = ",".join(v.strip().lower() for k, v in response.getheaders() if k.lower() == "content-encoding")
        if encoding in ("", "identity"):
            decoder = None
        elif encoding in ("gzip", "x-gzip", "deflate"):
            decoder = zlib.decompressobj(31 if encoding != "deflate" else 15)
        else:
            raise ProviderError(60007, "Unsupported HTTP content encoding")
        raw_allowance = allowance if decoder is None else lambda: 8192
        try:
            for raw in self._raw_body(response, raw_allowance):
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

    def _json_body(self, response):
        body = bytearray()
        for part in self._decoded_body(response, lambda: self.limit - len(body) + 1, False):
            if len(body) + len(part) > self.limit:
                body.clear()
                raise ProviderError(60008, "Provider response body exceeds max_payload_bytes")
            body.extend(part)
        return _json(body)

    def _sse(self, response):
        status = response.status
        event = bytearray()
        line_start, previous_cr = 0, False
        prefix = bytearray()
        for chunk in self._decoded_body(response, lambda: self.limit - len(event) + 1, True):
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
            streaming = self.request_body.get("stream", False)
            body = json.dumps(self.request_body, ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
            port = url.port or (443 if url.scheme == "https" else 80)
            target = quote(url.path, safe="/%:@!$&'()*+,;=-._~") or "/"
            with closing(http.client.HTTPConnection(url.hostname, port)) as connection:
                connection.sock = _HTTPTransport(self)
                connection.request("POST", target, body, {
                    "Authorization": ("Bearer " + self.config.api_key).encode("utf-8"),
                    "Content-Type": "application/json",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "close",
                })
                self.request_body = None
                del body
                # Read the response directly: this call owns the transport until
                # the body is consumed, including Connection: close responses.
                with http.client.HTTPResponse(connection.sock, method="POST") as response:
                    while True:
                        response.begin()
                        if not 100 <= response.status < 200 or response.status == 101:
                            break
                        # Continue past informational responses using the same
                        # parser, buffered input and response-header budget.
                        response.headers = None
                    response.fp.header_remaining = None
                    self._check()
                    self._deadline = time.monotonic() + self.config.timeout.idle_ms / 1000
                    status = response.status
                    headers = [{"name": k.lower(), "value": v} for k, v in response.getheaders()
                               if k.lower() in {"content-type", "retry-after", "x-request-id"}
                               or k.lower().startswith("x-ratelimit-")]
                    if not streaming:
                        yield ProviderEvent("result", status, headers, self._json_body(response), True)
                        return
                    pause = time.monotonic()
                    yield ProviderEvent("headers", status, headers)
                    self._deadline += time.monotonic() - pause
                    media_type = response.getheader("content-type", "").split(";", 1)[0].strip().lower()
                    if media_type == "text/event-stream":
                        for event in self._sse(response):
                            pause = time.monotonic()
                            yield event
                            self._deadline += time.monotonic() - pause
                    else:
                        yield ProviderEvent("completed", status, body=self._json_body(response), has_body=True)
        except (http.client.IncompleteRead, http.client.RemoteDisconnected) as exc:
            self._check()
            raise ProviderError(60003, "Provider response ended prematurely") from exc
        except http.client.HTTPException as exc:
            self._check()
            raise ProviderError(60007, "Invalid Provider HTTP response") from exc
        except (OSError, ssl.SSLError) as exc:
            self._check()
            raise ProviderError(60003, "Provider connection failed or reset") from exc
        finally:
            self._close()
