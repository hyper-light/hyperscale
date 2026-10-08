from __future__ import annotations

from dataclasses import dataclass, field

from hyperscale.distributed.models.message import Message
from hyperscale.distributed.resources.manager_resource_gossip_entry import (
    ManagerResourceGossipEntry,
)


@dataclass(slots=True)
class ManagerResourceGossipMessage(Message):
    """AD-41: a manager's gossip to its datacenter's peer managers -- every
    fresh resource report it holds, its own and the ones it heard."""

    datacenter: str
    entries: list[ManagerResourceGossipEntry] = field(default_factory=list)
