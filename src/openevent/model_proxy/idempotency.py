from __future__ import annotations

from dataclasses import dataclass
from threading import RLock


@dataclass(frozen=True)
class RequestRecord:
    channel_id: int
    request_id: str
    original_seq: int
    principal: int


class IdempotencyStore:
    def __init__(self):
        self._requests: dict[tuple[int, str], RequestRecord] = {}
        self._results_by_seq: dict[tuple[int, int], tuple[int, int]] = {}
        self._lock = RLock()

    def get_request(self, channel_id: int, request_id: str) -> RequestRecord | None:
        with self._lock:
            return self._requests.get((channel_id, request_id))

    def insert_original(self, channel_id: int, request_id: str, original_seq: int, principal: int) -> bool:
        with self._lock:
            key = (channel_id, request_id)
            if key in self._requests:
                return False
            self._requests[key] = RequestRecord(
                channel_id=channel_id,
                request_id=request_id,
                original_seq=original_seq,
                principal=principal,
            )
            return True

    def record_result(self, channel_id: int, request_seq: int, result_seq: int, status_code: int) -> bool:
        with self._lock:
            key = (channel_id, request_seq)
            if key in self._results_by_seq:
                return False
            self._results_by_seq[key] = (result_seq, status_code)
            return True

    def has_result_for_request_seq(self, channel_id: int, request_seq: int) -> bool:
        with self._lock:
            return (channel_id, request_seq) in self._results_by_seq
