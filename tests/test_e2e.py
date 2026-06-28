from __future__ import annotations

import json
import os
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from openevent.model_proxy.config import ModelProxyConfig, OpenEventConfig, ProviderConfig, TimeoutConfig
from openevent.model_proxy.worker import ModelProxyWorker
from openevent.model_proxy_sdk import (
    InferRequestInput,
    ModelProxyProtocolClient,
    parse_message,
    publish_infer_request,
)
from openevent.sdk import AdminClient, OpenEventClient
from openevent.sdk.proto import openevent_pb2


E2E_ENABLED = os.environ.get("OPENEVENT_MODEL_PROXY_E2E") == "1"


class MockLLMState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.requests: list[dict[str, Any]] = []
        self.auth_headers: list[str | None] = []

    def record(self, path: str, headers, body: dict[str, Any]) -> None:
        with self.lock:
            self.requests.append({"path": path, "body": body})
            self.auth_headers.append(headers.get("authorization"))


class MockLLMHandler(BaseHTTPRequestHandler):
    state: MockLLMState

    def do_POST(self) -> None:
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return
        length = int(self.headers.get("content-length", "0"))
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        self.state.record(self.path, self.headers, body)
        self._write_json(
            {
                "id": "chatcmpl-mock",
                "object": "chat.completion",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "mock pong"},
                        "finish_reason": "stop",
                    }
                ],
            }
        )

    def log_message(self, format: str, *args: object) -> None:
        return None

    def _write_json(self, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


class MockLLMServer:
    def __init__(self) -> None:
        self.state = MockLLMState()
        MockLLMHandler.state = self.state
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), MockLLMHandler)
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()


def _token(admin: AdminClient, principal: int) -> str:
    return admin.add_token(target_principal=principal).binding.token


def _unique(prefix: str) -> str:
    return f"{prefix}-{time.time_ns()}"


def _e2e_tmp() -> Path:
    return Path(os.environ.get("OPENEVENT_MODEL_PROXY_E2E_TMP", "build/e2e/tmp"))


def _config(proxy_principal: int, proxy_token: str, base_url: str, db_path: Path) -> ModelProxyConfig:
    return ModelProxyConfig(
        protocol="llm.v1",
        open_event=OpenEventConfig(os.environ["OPENEVENT_MODEL_PROXY_E2E_TARGET"]),
        principal=proxy_principal,
        token=proxy_token,
        idempotency_dsn=f"sqlite:///{db_path}",
        max_payload_bytes=16 * 1024 * 1024,
        default_provider="mock_llm",
        providers={
            "mock_llm": ProviderConfig(
                name="mock_llm",
                type="openai_compatible",
                base_url=base_url,
                api_key="test-key",
                timeout=TimeoutConfig(total_ms=3000),
            )
        },
    )


def _publish_request(client: OpenEventClient, caller: int, caller_token: str, channel_id: int) -> int:
    protocol_client = ModelProxyProtocolClient(client, caller_token)
    return publish_infer_request(
        protocol_client,
        channel_id=channel_id,
        principal=caller,
        req=InferRequestInput(
            request_id="req-e2e-1",
            method="POST",
            path="/v1/chat/completions",
            body={
                "model": "mock-model",
                "messages": [{"role": "user", "content": "ping"}],
            },
            ts_ms=int(time.time() * 1000),
        ),
    )


@unittest.skipUnless(E2E_ENABLED, "set OPENEVENT_MODEL_PROXY_E2E=1 and run test-e2e.sh or make e2e")
class ModelProxyE2ETests(unittest.TestCase):
    def test_worker_round_trips_infer_request_with_mock_llm_service(self) -> None:
        admin = AdminClient(os.environ["OPENEVENT_MODEL_PROXY_E2E_ADMIN_TARGET"])
        client = OpenEventClient(os.environ["OPENEVENT_MODEL_PROXY_E2E_TARGET"])

        proxy_principal = 92001
        caller_principal = 92002
        proxy_token = _token(admin, proxy_principal)
        caller_token = _token(admin, caller_principal)

        channel = client.create_channel(
            principal=caller_principal,
            token=caller_token,
            name=_unique("model-proxy-e2e"),
            visibility=openevent_pb2.VISIBILITY_PRIVATE,
            protocol="llm.v1",
            description=json.dumps(
                {
                    "version": "v1",
                    "updated_at_ms": int(time.time() * 1000),
                    "metadata": {"test": "model-proxy-e2e"},
                },
                separators=(",", ":"),
            ),
            members=[proxy_principal],
        ).channel

        request_seq = _publish_request(client, caller_principal, caller_token, channel.channel_id)

        with MockLLMServer() as mock_llm:
            config = _config(
                proxy_principal=proxy_principal,
                proxy_token=proxy_token,
                base_url=mock_llm.base_url,
                db_path=_e2e_tmp() / f"model-proxy-{time.time_ns()}.db",
            )
            worker = ModelProxyWorker(config, client)
            target = int(client.get_status(proxy_principal, proxy_token).max_seq)
            pending = worker.recover(target)
            self.assertEqual([item.seq for item in pending], [request_seq])
            for item in pending:
                worker._process_original(item)

            response = client.fetch(
                principal=caller_principal,
                token=caller_token,
                from_seq=request_seq,
                limit=1000,
                only_my_recipient=False,
                channels=[channel.channel_id],
            )
            parsed = [parse_message(message) for message in response.messages]
            results = [
                item
                for item in parsed
                if item.channel_id == channel.channel_id
                and getattr(item.payload, "kind", None) == "infer.result"
                and item.payload.request_id == "req-e2e-1"
            ]

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result.principal, proxy_principal)
        self.assertEqual(result.recipients, (caller_principal,))
        self.assertEqual(result.payload.prev_seq, request_seq)
        self.assertEqual(result.payload.status_code, 200)
        self.assertEqual(result.payload.body["choices"][0]["message"]["content"], "mock pong")
        self.assertEqual(mock_llm.state.auth_headers, ["Bearer test-key"])
        self.assertEqual(mock_llm.state.requests[0]["path"], "/v1/chat/completions")
        self.assertEqual(mock_llm.state.requests[0]["body"]["messages"][0]["content"], "ping")


if __name__ == "__main__":
    unittest.main()
