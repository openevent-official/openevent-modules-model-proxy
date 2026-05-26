from __future__ import annotations

import json


class ChannelResolver:
    def __init__(self, openevent_client, principal: int, token: str):
        self.openevent_client = openevent_client
        self.principal = principal
        self.token = token
        self._cache = {}

    def is_owned_llm_channel(self, channel_id: int) -> bool:
        if channel_id not in self._cache:
            self._cache[channel_id] = self._load(channel_id)
        return self._cache[channel_id]

    def _load(self, channel_id: int) -> bool:
        try:
            response = self.openevent_client.get_channel(self.principal, self.token, channel_id)
            channel = response.channel
        except Exception:
            return False
        if channel.protocol != "llm.v1":
            return False
        if int(channel.visibility) == 0:
            return False
        if self.principal not in {int(v) for v in channel.members}:
            return False
        try:
            desc = json.loads(channel.description)
        except json.JSONDecodeError:
            return False
        if desc.get("version") != "v1":
            return False
        if not isinstance(desc.get("updated_at_ms"), int):
            return False
        metadata = desc.get("metadata")
        if metadata is not None and not isinstance(metadata, dict):
            return False
        return True
