from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RequestRecord:
    channel_id: int
    request_id: str
    original_seq: int
    principal: int
    status: str


class IdempotencyStore:
    def __init__(self):
        self._requests: dict[tuple[int, str], RequestRecord] = {}
        self._results_by_seq: dict[tuple[int, int], tuple[int, int]] = {}

    def get_request(self, channel_id: int, request_id: str) -> RequestRecord | None:
        return self._requests.get((channel_id, request_id))

    def insert_original(self, channel_id: int, request_id: str, original_seq: int, principal: int) -> bool:
        key = (channel_id, request_id)
        if key in self._requests:
            return False
        self._requests[key] = RequestRecord(
            channel_id=channel_id,
            request_id=request_id,
            original_seq=original_seq,
            principal=principal,
            status="RECEIVED",
        )
        return True

    def set_status(self, channel_id: int, request_id: str, status: str) -> None:
        key = (channel_id, request_id)
        existing = self._requests.get(key)
        if existing is None:
            return
        self._requests[key] = RequestRecord(
            channel_id=existing.channel_id,
            request_id=existing.request_id,
            original_seq=existing.original_seq,
            principal=existing.principal,
            status=status,
        )

    def record_result(self, channel_id: int, request_seq: int, result_seq: int, status_code: int) -> bool:
        key = (channel_id, request_seq)
        if key in self._results_by_seq:
            return False
        self._results_by_seq[key] = (result_seq, status_code)
        return True

    def has_result_for_request_seq(self, channel_id: int, request_seq: int) -> bool:
        return (channel_id, request_seq) in self._results_by_seq
