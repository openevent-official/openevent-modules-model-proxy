from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RequestRecord:
    channel_id: int
    request_id: str
    original_seq: int
    principal: int
    status: str


class IdempotencyStore:
    def __init__(self, dsn: str):
        if not dsn.startswith("sqlite:///"):
            raise ValueError("only sqlite:/// idempotency_dsn is supported")
        db_path = dsn.removeprefix("sqlite:///")
        Path(db_path).parent.mkdir(parents=True, exist_ok=True) if "/" in db_path else None
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS requests (
                channel_id INTEGER NOT NULL,
                request_id TEXT NOT NULL,
                original_seq INTEGER NOT NULL,
                principal INTEGER NOT NULL,
                status TEXT NOT NULL,
                PRIMARY KEY (channel_id, request_id)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS results_by_seq (
                channel_id INTEGER NOT NULL,
                request_seq INTEGER NOT NULL,
                result_seq INTEGER NOT NULL,
                status_code INTEGER NOT NULL,
                PRIMARY KEY (channel_id, request_seq)
            )
            """
        )
        self.conn.commit()

    def get_request(self, channel_id: int, request_id: str) -> RequestRecord | None:
        row = self.conn.execute(
            "SELECT * FROM requests WHERE channel_id=? AND request_id=?",
            (channel_id, request_id),
        ).fetchone()
        if row is None:
            return None
        return RequestRecord(
            channel_id=row["channel_id"],
            request_id=row["request_id"],
            original_seq=row["original_seq"],
            principal=row["principal"],
            status=row["status"],
        )

    def insert_original(self, channel_id: int, request_id: str, original_seq: int, principal: int) -> bool:
        try:
            self.conn.execute(
                "INSERT INTO requests(channel_id, request_id, original_seq, principal, status) VALUES (?, ?, ?, ?, ?)",
                (channel_id, request_id, original_seq, principal, "RECEIVED"),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def set_status(self, channel_id: int, request_id: str, status: str) -> None:
        self.conn.execute(
            "UPDATE requests SET status=? WHERE channel_id=? AND request_id=?",
            (status, channel_id, request_id),
        )
        self.conn.commit()

    def record_result(self, channel_id: int, request_seq: int, result_seq: int, status_code: int) -> bool:
        try:
            self.conn.execute(
                "INSERT INTO results_by_seq(channel_id, request_seq, result_seq, status_code) VALUES (?, ?, ?, ?)",
                (channel_id, request_seq, result_seq, status_code),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def has_result_for_request_seq(self, channel_id: int, request_seq: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM results_by_seq WHERE channel_id=? AND request_seq=?",
            (channel_id, request_seq),
        ).fetchone()
        return row is not None
