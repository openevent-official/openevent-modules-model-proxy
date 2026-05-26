from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelProxyProtocolClient:
    openevent_client: object
    token: str


def create_client(openevent_client: object, token: str) -> ModelProxyProtocolClient:
    return ModelProxyProtocolClient(openevent_client=openevent_client, token=token)
