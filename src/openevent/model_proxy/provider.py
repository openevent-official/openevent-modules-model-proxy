from __future__ import annotations

import base64
import json
import socket
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass

from openevent.model_proxy_sdk.model import Header

from .config import ProviderConfig


@dataclass(frozen=True)
class ProviderHTTPResult:
    status_code: int
    headers: list[Header]
    body: object


@dataclass(frozen=True)
class ProviderError:
    status_code: int
    message: str


class ProviderClient:
    def __init__(self, config: ProviderConfig):
        self.config = config

    def call(self, method: str, path: str, body: object) -> ProviderHTTPResult | ProviderError:
        url = self.config.base_url + path
        payload = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        request = urllib.request.Request(url=url, data=payload, method=method)
        request.add_header("Authorization", f"Bearer {self.config.api_key}")
        request.add_header("Content-Type", "application/json")
        request.add_header("Accept", "application/json")
        timeout_s = max(self.config.timeout.total_ms, 1) / 1000
        try:
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                return self._read_response(response.status, response.headers.items(), response.read())
        except urllib.error.HTTPError as exc:
            return self._read_response(exc.code, exc.headers.items(), exc.read())
        except socket.timeout:
            return ProviderError(60000, "model API request timed out")
        except socket.gaierror:
            return ProviderError(60001, "DNS resolution failed")
        except ssl.SSLError:
            return ProviderError(60002, "TLS handshake failed")
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            reason = getattr(exc, "reason", exc)
            if isinstance(reason, socket.gaierror):
                return ProviderError(60001, "DNS resolution failed")
            if isinstance(reason, ssl.SSLError):
                return ProviderError(60002, "TLS handshake failed")
            if isinstance(reason, socket.timeout):
                return ProviderError(60000, "model API request timed out")
            return ProviderError(60003, "connection failed or reset")

    def _read_response(self, status_code: int, headers_items, data: bytes) -> ProviderHTTPResult:
        headers = [Header(name=str(name).lower(), value=str(value)) for name, value in headers_items]
        content_type = _header_value(headers, "content-type")
        try:
            decoded = data.decode("utf-8")
            body = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = {
                "non_json_body": {
                    "encoding": "base64",
                    "content_type": content_type,
                    "data": base64.b64encode(data).decode("ascii"),
                }
            }
        return ProviderHTTPResult(status_code=status_code, headers=headers, body=body)


def _header_value(headers: list[Header], name: str) -> str:
    lowered = name.lower()
    for header in headers:
        if header.name.lower() == lowered:
            return header.value
    return ""
