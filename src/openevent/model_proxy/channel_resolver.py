from __future__ import annotations

import json
import logging


LOG = logging.getLogger(__name__)


class ChannelResolver:
    def __init__(
        self,
        openevent_client,
        principal: int,
        token: str,
        channels: tuple[int, ...],
        rpc_timeout_s: float | None = None,
    ):
        self.openevent_client = openevent_client
        self.principal = principal
        self.token = token
        self.channels = frozenset(channels)
        self.rpc_timeout_s = rpc_timeout_s
        self._cache = {}

    def is_owned_llm_channel(self, channel_id: int) -> bool:
        if channel_id not in self.channels:
            return False
        if channel_id not in self._cache:
            self._cache[channel_id] = self._load(channel_id)
        return self._cache[channel_id]

    def _load(self, channel_id: int) -> bool:
        response = self.openevent_client.get_channel(
            self.principal, self.token, channel_id, timeout=self.rpc_timeout_s
        )
        channel = response.channel
        if channel.protocol != "llm.v1":
            self._log_ignored(channel_id, "protocol_mismatch", protocol=channel.protocol)
            return False
        if int(channel.visibility) == 0:
            self._log_ignored(channel_id, "public_visibility")
            return False
        if self.principal not in {int(v) for v in channel.members}:
            self._log_ignored(channel_id, "proxy_not_member")
            return False
        try:
            desc = json.loads(channel.description)
        except (json.JSONDecodeError, TypeError):
            self._log_ignored(channel_id, "invalid_description_json")
            return False
        if desc.get("version") != "v1":
            self._log_ignored(channel_id, "invalid_description_version")
            return False
        if not isinstance(desc.get("updated_at_ms"), int):
            self._log_ignored(channel_id, "invalid_description_updated_at_ms")
            return False
        metadata = desc.get("metadata")
        if metadata is not None and not isinstance(metadata, dict):
            self._log_ignored(channel_id, "invalid_description_metadata")
            return False
        return True

    def _log_ignored(self, channel_id: int, reason: str, **context) -> None:
        LOG.warning(
            "ignoring channel",
            extra={"channel_id": channel_id, "reason": reason, **context},
        )
