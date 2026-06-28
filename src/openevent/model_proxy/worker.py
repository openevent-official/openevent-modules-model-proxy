from __future__ import annotations

import logging
import sys
import time
from dataclasses import dataclass

from openevent.model_proxy_sdk import ModelProxyProtocolClient, parse_message
from openevent.model_proxy_sdk.errors import ModelProxySDKError
from openevent.model_proxy_sdk.errors_payload import proxy_error_result
from openevent.model_proxy_sdk.model import Header, InferRequest, InferResult, InferResultInput

from .channel_resolver import ChannelResolver
from .config import ModelProxyConfig
from .idempotency import IdempotencyStore
from .provider import ProviderClient, ProviderError, ProviderHTTPResult
from .publisher import ResultPublishFatal, ResultPublisher

LOG = logging.getLogger(__name__)

UNIMPORTANT_RESPONSE_HEADERS = frozenset(
    {
        "accept-ranges",
        "age",
        "alt-svc",
        "cache-control",
        "cf-cache-status",
        "cf-ray",
        "connection",
        "content-encoding",
        "content-length",
        "date",
        "etag",
        "expires",
        "keep-alive",
        "last-modified",
        "pragma",
        "server",
        "set-cookie",
        "strict-transport-security",
        "transfer-encoding",
        "vary",
        "via",
        "x-cache",
        "x-content-type-options",
        "x-envoy-upstream-service-time",
        "x-frame-options",
        "x-powered-by",
        "x-xss-protection",
    }
)


@dataclass(frozen=True)
class WorkItem:
    seq: int
    channel_id: int
    principal: int
    request: InferRequest


@dataclass(frozen=True)
class DeferredResult:
    channel_id: int
    request_principal: int
    result: InferResultInput
    status: str


class ModelProxyWorker:
    def __init__(self, config: ModelProxyConfig, openevent_client):
        self.config = config
        self.openevent_client = openevent_client
        self.protocol_client = ModelProxyProtocolClient(openevent_client, config.token)
        self.store = IdempotencyStore(config.idempotency_dsn)
        self.resolver = ChannelResolver(openevent_client, config.principal, config.token)
        self.publisher = ResultPublisher(self.protocol_client, config.principal, config.max_payload_bytes)
        provider_config = config.providers[config.default_provider]
        self.provider = ProviderClient(provider_config)

    def run(self) -> None:
        target = int(self.openevent_client.get_status(self.config.principal, self.config.token).max_seq)
        pending = self.recover(target)
        for item in pending:
            self._process_original(item)
        next_seq = target + 1
        while True:
            for response in self.openevent_client.subscribe(
                self.config.principal,
                self.config.token,
                from_seq=next_seq,
                only_my_recipient=False,
            ):
                if response.HasField("next_seq"):
                    next_seq = int(response.next_seq)
                    break
                message = response.message
                next_seq = int(message.seq) + 1
                item = self._observe_message(message, realtime=True)
                if item is not None:
                    self._process_original(item)

    def recover(self, scan_target_max_seq: int) -> list[WorkItem]:
        pending: list[WorkItem] = []
        deferred_results: list[DeferredResult] = []
        from_seq = 1
        while from_seq <= scan_target_max_seq:
            response = self.openevent_client.fetch(
                self.config.principal,
                self.config.token,
                from_seq=from_seq,
                limit=1000,
                only_my_recipient=False,
            )
            for message in response.messages:
                if int(message.seq) > scan_target_max_seq:
                    continue
                item = self._observe_message(message, realtime=False, deferred_results=deferred_results)
                if item is not None:
                    pending.append(item)
            next_seq = int(response.next_seq)
            if next_seq <= from_seq:
                raise RuntimeError("Fetch did not advance next_seq during recovery")
            from_seq = next_seq
        for deferred in deferred_results:
            if not self.store.has_result_for_request_seq(deferred.channel_id, deferred.result.prev_seq):
                self._publish_and_record(
                    deferred.channel_id,
                    deferred.request_principal,
                    deferred.result,
                    deferred.status,
                )
        return [item for item in pending if not self.store.has_result_for_request_seq(item.channel_id, item.seq)]

    def _observe_message(
        self,
        message,
        realtime: bool,
        deferred_results: list[DeferredResult] | None = None,
    ) -> WorkItem | None:
        if not self.resolver.is_owned_llm_channel(int(message.channel_id)):
            return None
        try:
            parsed = parse_message(message)
        except ModelProxySDKError as exc:
            self._handle_invalid_message(message, exc, realtime, deferred_results)
            return None
        payload = parsed.payload
        if isinstance(payload, InferResult):
            self.store.record_result(parsed.channel_id, payload.prev_seq, parsed.seq, payload.status_code)
            return None
        if isinstance(payload, InferRequest):
            existing = self.store.get_request(parsed.channel_id, payload.request_id)
            if existing is None:
                self.store.insert_original(parsed.channel_id, payload.request_id, parsed.seq, parsed.principal)
                return WorkItem(parsed.seq, parsed.channel_id, parsed.principal, payload)
            if existing.original_seq == parsed.seq:
                if not self.store.has_result_for_request_seq(parsed.channel_id, parsed.seq):
                    return WorkItem(parsed.seq, parsed.channel_id, parsed.principal, payload)
                return None
            if not self.store.has_result_for_request_seq(parsed.channel_id, parsed.seq):
                duplicate = proxy_error_result(
                    request_id=payload.request_id,
                    prev_seq=parsed.seq,
                    status_code=60005,
                    message="duplicate request_id rejected",
                )
                if realtime:
                    self._publish_and_record(parsed.channel_id, parsed.principal, duplicate, "FAILURE_RESULT_PUBLISHED")
                elif deferred_results is not None:
                    deferred_results.append(
                        DeferredResult(parsed.channel_id, parsed.principal, duplicate, "FAILURE_RESULT_PUBLISHED")
                    )
            return None
        return None

    def _handle_invalid_message(
        self,
        message,
        exc: ModelProxySDKError,
        realtime: bool,
        deferred_results: list[DeferredResult] | None,
    ) -> None:
        # Best-effort extraction only for malformed request payloads with a valid request_id.
        import json

        try:
            data = json.loads(message.payload.decode("utf-8"))
        except Exception:
            LOG.warning("invalid llm payload cannot be parsed", extra={"seq": int(message.seq), "error": exc.code})
            return
        if data.get("kind") != "infer.request":
            return
        request_id = data.get("request_id")
        try:
            from openevent.model_proxy_sdk.validator import validate_request_id

            validate_request_id(request_id)
        except ModelProxySDKError:
            LOG.warning("invalid request has no usable request_id", extra={"seq": int(message.seq), "error": exc.code})
            return
        if self.store.has_result_for_request_seq(int(message.channel_id), int(message.seq)):
            return
        result = proxy_error_result(
            request_id=request_id,
            prev_seq=int(message.seq),
            status_code=60009,
            message="invalid request payload",
            context={"validation_error": exc.code},
        )
        if realtime:
            self._publish_and_record(int(message.channel_id), int(message.principal), result, "FAILURE_RESULT_PUBLISHED")
        elif deferred_results is not None:
            deferred_results.append(
                DeferredResult(int(message.channel_id), int(message.principal), result, "FAILURE_RESULT_PUBLISHED")
            )

    def _process_original(self, item: WorkItem) -> None:
        if self.store.has_result_for_request_seq(item.channel_id, item.seq):
            return
        self.store.set_status(item.channel_id, item.request.request_id, "INFERENCING")
        provider_result = self.provider.call(item.request.method, item.request.path, item.request.body)
        if isinstance(provider_result, ProviderError):
            result = proxy_error_result(
                request_id=item.request.request_id,
                prev_seq=item.seq,
                status_code=provider_result.status_code,
                message=provider_result.message,
            )
            self._publish_and_record(item.channel_id, item.principal, result, "FAILURE_RESULT_PUBLISHED")
            return
        if isinstance(provider_result, ProviderHTTPResult):
            result = InferResultInput(
                request_id=item.request.request_id,
                prev_seq=item.seq,
                status_code=provider_result.status_code,
                headers=self._response_headers_for_result(provider_result.headers),
                body=provider_result.body,
                ts_ms=int(time.time() * 1000),
            )
            self._publish_and_record(item.channel_id, item.principal, result, "RESULT_PUBLISHED")
            return

    def _response_headers_for_result(self, headers: list[Header]) -> list[Header]:
        if not self.config.filter_response_headers:
            return list(headers)
        return [header for header in headers if header.name.lower() not in UNIMPORTANT_RESPONSE_HEADERS]

    def _publish_and_record(self, channel_id: int, request_principal: int, result: InferResultInput, status: str) -> None:
        try:
            result_seq, published_result = self.publisher.publish(channel_id, request_principal, result)
        except ResultPublishFatal:
            LOG.exception("fatal result publish failure", extra={"channel_id": channel_id, "prev_seq": result.prev_seq})
            sys.exit(1)
        published_status = "FAILURE_RESULT_PUBLISHED" if published_result.status_code >= 60000 else status
        self.store.record_result(channel_id, published_result.prev_seq, result_seq, published_result.status_code)
        if self.store.get_request(channel_id, published_result.request_id) is not None:
            self.store.set_status(channel_id, published_result.request_id, published_status)
