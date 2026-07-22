from __future__ import annotations

import time
import uuid
from copy import deepcopy
from typing import Any

from .client import ModelProxyProtocolClient
from .errors import ModelProxySDKError
from .codec import dumps_payload, request_input_to_dict
from .model import InferRequest, InferRequestInput, InferResult
from .openevent_io import parse_message


class APIError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        request_id: str | None = None,
        provider_error: Any = None,
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.request_id = request_id
        self.provider_error = provider_error


class AuthenticationError(APIError):
    pass


class PermissionDeniedError(APIError):
    pass


class RateLimitError(APIError):
    pass


class APITimeoutError(APIError):
    pass


class APIConnectionError(APIError):
    pass


class InternalServerError(APIError):
    pass


class CompatibilityError(APIError):
    pass


class ConfigurationError(ValueError):
    pass


class OpenAIObject:
    def __init__(self, data: dict[str, Any]):
        object.__setattr__(self, "_data", deepcopy(data))
        for key, value in data.items():
            if isinstance(key, str):
                object.__setattr__(self, key, _wrap(value))

    def __getitem__(self, key: str) -> Any:
        return _wrap(self._data[key])

    def get(self, key: str, default: Any = None) -> Any:
        return _wrap(self._data.get(key, default))

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self._data)

    def model_dump(self) -> dict[str, Any]:
        return self.to_dict()

    def __repr__(self) -> str:
        return f"OpenAIObject({self._data!r})"


class OpenAI:
    def __init__(
        self,
        *,
        openevent_addr: str | None = None,
        openevent_token: str | None = None,
        openevent_channel_id: int | None = None,
        openevent_principal: int | None = None,
        request_timeout_ms: int = 60000,
        max_retries: int = 0,
        openevent_client: object | None = None,
        **kwargs: Any,
    ):
        if kwargs:
            names = ", ".join(sorted(kwargs))
            raise ConfigurationError(f"unsupported initialization parameters: {names}")
        if not isinstance(openevent_token, str) or not openevent_token:
            raise ConfigurationError("openevent_token must be a non-empty string")
        if not _positive_int(openevent_channel_id):
            raise ConfigurationError("openevent_channel_id must be a positive integer")
        if not _positive_int(openevent_principal):
            raise ConfigurationError("openevent_principal must be a positive integer")
        if not _positive_int(request_timeout_ms):
            raise ConfigurationError("request_timeout_ms must be a positive integer")
        if not isinstance(max_retries, int) or isinstance(max_retries, bool) or max_retries < 0:
            raise ConfigurationError("max_retries must be a non-negative integer")
        if openevent_client is None:
            if not isinstance(openevent_addr, str) or not openevent_addr:
                raise ConfigurationError("openevent_addr must be a non-empty string")
            openevent_client = _create_openevent_client(openevent_addr)
        self._client = ModelProxyProtocolClient(openevent_client, openevent_token)
        self._channel_id = int(openevent_channel_id)
        self._principal = int(openevent_principal)
        self._request_timeout_ms = int(request_timeout_ms)
        self._max_retries = int(max_retries)
        self.chat = _ChatResource(self)
        self.responses = _ResponsesResource(self)

    def _create(self, path: str, body: dict[str, Any], request_id: str | None) -> OpenAIObject:
        current_request_id = request_id or _new_request_id()
        return self._create_once(path, body, current_request_id)

    def _create_once(self, path: str, body: dict[str, Any], request_id: str) -> OpenAIObject:
        req = InferRequestInput(
            request_id=request_id,
            method="POST",
            path=path,
            body=body,
        )
        deadline = time.monotonic() + self._request_timeout_ms / 1000
        try:
            request_seq = self._publish_request(req, deadline)
        except ModelProxySDKError as exc:
            raise APIError(exc.message, request_id=request_id, provider_error=exc.to_dict()) from exc
        except Exception as exc:
            raise _map_transport_error(exc, request_id=request_id) from exc
        result = self._wait_for_result(request_id, request_seq, deadline)
        if 200 <= result.status_code <= 299:
            data = result.body if isinstance(result.body, dict) else {"data": result.body}
            response = dict(data)
            response["openevent_seq"] = request_seq
            return OpenAIObject(response)
        raise _map_result_error(result)

    def _publish_request(self, req: InferRequestInput, deadline: float) -> int:
        event = self._client.openevent_client
        payload = dumps_payload(request_input_to_dict(req, ts_ms=req.ts_ms or int(time.time() * 1000)))
        attempts = self._max_retries + 1
        scan_from = int(
            event.get_status(
                self._principal,
                self._client.token,
                timeout=_remaining(deadline),
            ).max_seq
        ) + 1

        for attempt in range(attempts):
            try:
                response = event.publish_auto_seq(
                    principal=self._principal,
                    token=self._client.token,
                    channel_id=self._channel_id,
                    payload=payload,
                    recipients=(),
                    timeout=_remaining(deadline),
                )
                return int(response.seq)
            except Exception as exc:
                if _is_guaranteed_not_committed(exc):
                    raise
                matched_seq, reconcile_max_seq = self._reconcile_request(
                    req.request_id, payload, scan_from, deadline
                )
                if matched_seq is not None:
                    return matched_seq
                if attempt + 1 >= attempts:
                    raise exc
                scan_from = reconcile_max_seq + 1
        raise APIError("request failed before it was submitted", request_id=req.request_id)

    def _reconcile_request(
        self, request_id: str, payload: bytes, from_seq: int, deadline: float
    ) -> tuple[int | None, int]:
        event = self._client.openevent_client
        reconcile_max_seq = int(
            event.get_status(
                self._principal,
                self._client.token,
                timeout=_remaining(deadline),
            ).max_seq
        )
        cursor = from_seq
        while cursor <= reconcile_max_seq:
            response = event.fetch(
                principal=self._principal,
                token=self._client.token,
                from_seq=cursor,
                limit=1000,
                only_my_recipient=False,
                channels=[self._channel_id],
                timeout=_remaining(deadline),
            )
            for message in response.messages:
                if int(message.seq) > reconcile_max_seq or int(message.channel_id) != self._channel_id:
                    continue
                if bytes(message.payload) == payload:
                    return int(message.seq), reconcile_max_seq
                try:
                    parsed = parse_message(message)
                except ModelProxySDKError:
                    continue
                if isinstance(parsed.payload, InferRequest) and parsed.payload.request_id == request_id:
                    return parsed.seq, reconcile_max_seq
            next_seq = int(response.next_seq)
            if next_seq <= cursor:
                raise APIConnectionError(
                    "OpenEvent Fetch did not advance during publish reconciliation",
                    request_id=request_id,
                )
            cursor = next_seq
        return None, reconcile_max_seq

    def _wait_for_result(self, request_id: str, request_seq: int, deadline: float) -> InferResult:
        from_seq = request_seq + 1
        while True:
            remaining_s = deadline - time.monotonic()
            if remaining_s <= 0:
                raise APITimeoutError(
                    "timed out waiting for model proxy result",
                    status_code=60000,
                    request_id=request_id,
                )
            try:
                response = self._client.openevent_client.fetch(
                    principal=self._principal,
                    token=self._client.token,
                    from_seq=from_seq,
                    limit=1000,
                    only_my_recipient=True,
                    channels=[self._channel_id],
                    timeout=remaining_s,
                )
            except Exception as exc:
                raise _map_transport_error(exc, request_id=request_id) from exc
            next_seq = int(getattr(response, "next_seq", from_seq))
            last_seq = int(getattr(response, "last_seq", next_seq - 1))
            for message in getattr(response, "messages", ()):
                message_seq = int(message.seq)
                if message_seq >= next_seq:
                    next_seq = message_seq + 1
                if int(message.channel_id) != self._channel_id:
                    continue
                try:
                    parsed = parse_message(message)
                except ModelProxySDKError:
                    continue
                payload = parsed.payload
                if (
                    isinstance(payload, InferResult)
                    and payload.request_id == request_id
                    and payload.prev_seq == request_seq
                ):
                    return payload
            if next_seq > from_seq:
                from_seq = next_seq
            if next_seq <= last_seq:
                continue
            time.sleep(min(0.05, max(0.001, remaining_s)))


class _ChatResource:
    def __init__(self, client: OpenAI):
        self.completions = _ChatCompletionsResource(client)


class _ChatCompletionsResource:
    def __init__(self, client: OpenAI):
        self._client = client

    def create(self, *, model: str, messages: list[Any], **kwargs: Any) -> OpenAIObject:
        body, request_id = _request_body({"model": model, "messages": messages}, kwargs)
        return self._client._create("/v1/chat/completions", body, request_id)


class _ResponsesResource:
    def __init__(self, client: OpenAI):
        self._client = client

    def create(self, *, model: str, input: Any, **kwargs: Any) -> OpenAIObject:
        body, request_id = _request_body({"model": model, "input": input}, kwargs)
        return self._client._create("/v1/responses", body, request_id)


def _request_body(required: dict[str, Any], kwargs: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    request_id = kwargs.pop("request_id", None)
    stream = kwargs.get("stream")
    if stream is True:
        raise CompatibilityError("stream=True is not supported by openevent.model_proxy_sdk.OpenAI")
    body = dict(required)
    body.update(kwargs)
    return body, request_id


def _wrap(value: Any) -> Any:
    if isinstance(value, dict):
        return OpenAIObject(value)
    if isinstance(value, list):
        return [_wrap(item) for item in value]
    return value


def _new_request_id() -> str:
    return f"req_{uuid.uuid4().hex}"


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise APITimeoutError("OpenEvent request deadline exceeded")
    return remaining


def _is_guaranteed_not_committed(exc: Exception) -> bool:
    return _grpc_code_name(exc) in {
        "UNAUTHENTICATED",
        "PERMISSION_DENIED",
        "NOT_FOUND",
        "INVALID_ARGUMENT",
        "RESOURCE_EXHAUSTED",
        "ABORTED",
    }


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _create_openevent_client(addr: str) -> object:
    try:
        from openevent.sdk import OpenEventClient
    except ImportError as exc:
        raise ConfigurationError("openevent-sdk is required when openevent_client is not provided") from exc
    return OpenEventClient(addr)


def _map_result_error(result: InferResult) -> APIError:
    message, provider_error = _extract_error(result.body)
    kwargs = {
        "status_code": result.status_code,
        "request_id": result.request_id,
        "provider_error": provider_error,
    }
    if result.status_code == 401:
        return AuthenticationError(message, **kwargs)
    if result.status_code == 403:
        return PermissionDeniedError(message, **kwargs)
    if result.status_code == 429:
        return RateLimitError(message, **kwargs)
    if result.status_code in {408, 504, 60000}:
        return APITimeoutError(message, **kwargs)
    if result.status_code in {60001, 60002, 60003}:
        return APIConnectionError(message, **kwargs)
    if 500 <= result.status_code <= 599 or result.status_code == 60007:
        return InternalServerError(message, **kwargs)
    return APIError(message, **kwargs)


def _extract_error(body: Any) -> tuple[str, Any]:
    if isinstance(body, dict):
        error = body.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            if isinstance(message, str) and message:
                return message, error
            return "model provider returned an error", error
    return "model provider returned an error", body


def _map_transport_error(exc: Exception, *, request_id: str | None) -> APIError:
    code_name = _grpc_code_name(exc)
    if code_name == "UNAUTHENTICATED":
        return AuthenticationError("OpenEvent authentication failed", request_id=request_id, provider_error=exc)
    if code_name == "PERMISSION_DENIED":
        return PermissionDeniedError("OpenEvent permission denied", request_id=request_id, provider_error=exc)
    if code_name in {"DEADLINE_EXCEEDED", "CANCELLED"}:
        return APITimeoutError("OpenEvent request timed out", request_id=request_id, provider_error=exc)
    if code_name in {"UNAVAILABLE", "RESOURCE_EXHAUSTED", "ABORTED", "INTERNAL"}:
        return APIConnectionError("OpenEvent request failed", request_id=request_id, provider_error=exc)
    return APIConnectionError("OpenEvent request failed", request_id=request_id, provider_error=exc)


def _grpc_code_name(exc: Exception) -> str | None:
    code_fn = getattr(exc, "code", None)
    if not callable(code_fn):
        return None
    code = code_fn()
    name = getattr(code, "name", None)
    return name if isinstance(name, str) else None
