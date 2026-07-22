from __future__ import annotations

import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from threading import Event, Lock, Semaphore

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

ALLOWED_RESPONSE_HEADERS = frozenset(
    {
        "content-type",
        "retry-after",
        "x-request-id",
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


class ModelProxyWorker:
    def __init__(self, config: ModelProxyConfig, openevent_client):
        self.config = config
        self.openevent_client = openevent_client
        self.protocol_client = ModelProxyProtocolClient(self.openevent_client, config.token)
        self.store = IdempotencyStore()
        self.resolver = ChannelResolver(
            self.openevent_client, config.principal, config.token, config.channels
        )
        self.publisher = ResultPublisher(self.protocol_client, config.principal, config.max_payload_bytes)
        provider_config = config.providers[config.default_provider]
        self.provider = ProviderClient(provider_config)
        self._executor = ThreadPoolExecutor(
            max_workers=config.worker.max_concurrency,
            thread_name_prefix="model-proxy-request",
        )
        self._slots = Semaphore(config.worker.max_concurrency)
        self._futures: set[Future] = set()
        self._state_lock = Lock()
        self._fatal = Event()
        self._fatal_error: BaseException | None = None

    def run(self) -> None:
        target = int(
            self.openevent_client.get_status(self.config.principal, self.config.token).max_seq
        )
        try:
            pending = self.recover(target)
            for task in pending:
                self._submit(task)
            next_seq = target + 1
            while not self._fatal.is_set():
                try:
                    for response in self.openevent_client.subscribe(
                        self.config.principal,
                        self.config.token,
                        from_seq=next_seq,
                        only_my_recipient=False,
                    ):
                        if self._fatal.is_set():
                            break
                        if response.HasField("next_seq"):
                            next_seq = int(response.next_seq)
                            break
                        message = response.message
                        task = self._observe_message(message)
                        if task is not None:
                            self._submit(task)
                        next_seq = int(message.seq) + 1
                except Exception as exc:
                    if _grpc_code_name(exc) != "DEADLINE_EXCEEDED":
                        raise
            self._raise_fatal()
        finally:
            self._shutdown()

    def recover(self, scan_target_max_seq: int) -> list[WorkItem | DeferredResult]:
        pending: list[WorkItem | DeferredResult] = []
        from_seq = 1
        while from_seq <= scan_target_max_seq:
            response = self.openevent_client.fetch(
                self.config.principal,
                self.config.token,
                from_seq=from_seq,
                limit=1000,
                only_my_recipient=False,
                channels=self.config.channels,
            )
            for message in response.messages:
                if int(message.seq) > scan_target_max_seq:
                    continue
                item = self._observe_message(message)
                if item is not None:
                    pending.append(item)
            next_seq = int(response.next_seq)
            if next_seq <= from_seq:
                raise RuntimeError("Fetch did not advance next_seq during recovery")
            from_seq = next_seq
        return [task for task in pending if not self._task_has_result(task)]

    def _observe_message(
        self,
        message,
    ) -> WorkItem | DeferredResult | None:
        if not self.resolver.is_owned_llm_channel(int(message.channel_id)):
            return None
        try:
            parsed = parse_message(message)
        except ModelProxySDKError as exc:
            return self._invalid_result(message, exc)
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
                return DeferredResult(parsed.channel_id, parsed.principal, duplicate)
            return None
        return None

    def _invalid_result(
        self,
        message,
        exc: ModelProxySDKError,
    ) -> DeferredResult | None:
        # Best-effort extraction only for malformed request payloads with a valid request_id.
        import json

        try:
            data = json.loads(message.payload.decode("utf-8"))
        except Exception:
            LOG.warning("invalid llm payload cannot be parsed", extra={"seq": int(message.seq), "error": exc.code})
            return None
        if data.get("kind") != "infer.request":
            return None
        request_id = data.get("request_id")
        try:
            from openevent.model_proxy_sdk.validator import validate_request_id

            validate_request_id(request_id)
        except ModelProxySDKError:
            LOG.warning("invalid request has no usable request_id", extra={"seq": int(message.seq), "error": exc.code})
            return None
        if self.store.has_result_for_request_seq(int(message.channel_id), int(message.seq)):
            return None
        result = proxy_error_result(
            request_id=request_id,
            prev_seq=int(message.seq),
            status_code=60009,
            message="invalid request payload",
            context={"validation_error": exc.code},
        )
        return DeferredResult(int(message.channel_id), int(message.principal), result)

    def _submit(self, task: WorkItem | DeferredResult) -> None:
        while not self._fatal.is_set():
            if self._slots.acquire(timeout=0.1):
                break
        else:
            self._raise_fatal()
            return
        future = self._executor.submit(self._run_task, task)
        with self._state_lock:
            self._futures.add(future)
        future.add_done_callback(self._task_done)

    def _run_task(self, task: WorkItem | DeferredResult) -> None:
        if isinstance(task, WorkItem):
            self._process_original(task)
        elif not self.store.has_result_for_request_seq(task.channel_id, task.result.prev_seq):
            self._publish_and_record(task.channel_id, task.request_principal, task.result)

    def _task_done(self, future: Future) -> None:
        self._slots.release()
        with self._state_lock:
            self._futures.discard(future)
        exc = future.exception()
        if exc is not None:
            with self._state_lock:
                if not self._fatal.is_set():
                    self._fatal_error = exc
                    self._fatal.set()

    def _task_has_result(self, task: WorkItem | DeferredResult) -> bool:
        request_seq = task.seq if isinstance(task, WorkItem) else task.result.prev_seq
        return self.store.has_result_for_request_seq(task.channel_id, request_seq)

    def _raise_fatal(self) -> None:
        if self._fatal_error is not None:
            raise RuntimeError("model-proxy request task failed") from self._fatal_error

    def _shutdown(self) -> None:
        with self._state_lock:
            futures = list(self._futures)
        if futures:
            wait(futures)
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _process_original(self, item: WorkItem) -> None:
        if self.store.has_result_for_request_seq(item.channel_id, item.seq):
            return
        provider_result = self.provider.call(item.request.method, item.request.path, item.request.body)
        if isinstance(provider_result, ProviderError):
            result = proxy_error_result(
                request_id=item.request.request_id,
                prev_seq=item.seq,
                status_code=provider_result.status_code,
                message=provider_result.message,
                context=provider_result.context,
            )
            self._publish_and_record(item.channel_id, item.principal, result)
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
            self._publish_and_record(item.channel_id, item.principal, result)
            return

    def _response_headers_for_result(self, headers: list[Header]) -> list[Header]:
        return [
            header
            for header in headers
            if header.name.lower() in ALLOWED_RESPONSE_HEADERS
            or header.name.lower().startswith("x-ratelimit-")
            or header.name.lower().startswith("ratelimit-")
        ]

    def _publish_and_record(self, channel_id: int, request_principal: int, result: InferResultInput) -> None:
        try:
            result_seq, published_result = self.publisher.publish(channel_id, request_principal, result)
        except ResultPublishFatal:
            LOG.exception("fatal result publish failure", extra={"channel_id": channel_id, "prev_seq": result.prev_seq})
            raise
        self.store.record_result(channel_id, published_result.prev_seq, result_seq, published_result.status_code)


def _grpc_code_name(exc: Exception) -> str | None:
    code = getattr(exc, "code", None)
    if not callable(code):
        return None
    try:
        value = code()
    except Exception:
        return None
    return getattr(value, "name", None) or str(value).rsplit(".", 1)[-1]
