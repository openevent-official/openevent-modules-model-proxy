from __future__ import annotations

import time
from dataclasses import replace

from openevent.model_proxy_sdk import ModelProxyProtocolClient, ResultPublishError, publish_infer_result
from openevent.model_proxy_sdk.codec import dumps_payload, result_input_to_dict
from openevent.model_proxy_sdk.errors_payload import proxy_error_result
from openevent.model_proxy_sdk.model import InferResultInput


class ResultPublishFatal(RuntimeError):
    pass


class ResultPublisher:
    def __init__(
        self, client: ModelProxyProtocolClient, principal: int, max_payload_bytes: int
    ):
        self.client = client
        self.principal = principal
        self.max_payload_bytes = max_payload_bytes

    def publish(self, channel_id: int, request_principal: int, result: InferResultInput) -> tuple[int, InferResultInput]:
        result = self._fit_payload(result)
        try:
            seq = publish_infer_result(
                self.client,
                channel_id=channel_id,
                principal=self.principal,
                request_principal=request_principal,
                req=result,
            )
        except ResultPublishError as exc:
            raise ResultPublishFatal(str(exc)) from exc
        return seq, result

    def _fit_payload(self, result: InferResultInput) -> InferResultInput:
        frozen = replace(result, ts_ms=result.ts_ms or int(time.time() * 1000))
        if len(dumps_payload(result_input_to_dict(frozen, ts_ms=frozen.ts_ms))) <= self.max_payload_bytes:
            return frozen

        too_large = proxy_error_result(
            request_id=result.request_id,
            prev_seq=result.prev_seq,
            status_code=60008,
            message="payload exceeds OpenEvent limit",
        )
        frozen = replace(too_large, ts_ms=too_large.ts_ms or int(time.time() * 1000))
        if len(dumps_payload(result_input_to_dict(frozen, ts_ms=frozen.ts_ms))) > self.max_payload_bytes:
            raise ResultPublishFatal("60008 error result exceeds max_payload_bytes")
        return frozen
