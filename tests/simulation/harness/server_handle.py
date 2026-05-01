"""
Server handle — opaque wrapper the supervisor uses to identify, address,
and reap a single node regardless of whether it is a gate, manager, or
worker.

The handle keeps the harness layered: the rest of the framework reasons
about `ServerHandle` instances without importing the concrete server
classes, so any future server kind drops in via the same interface.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from tests.simulation.harness.worker_ports import WorkerPorts


class ServerKind(StrEnum):
    GATE = "gate"
    MANAGER = "manager"
    WORKER = "worker"


@dataclass(slots=True)
class ServerHandle:
    """Everything the supervisor needs to manage one node.

    `node_id` follows the convention `<dc>.<kind>.<index>` (e.g.
    "east.manager.0", "global.gate.2"). For workers, `worker_ports`
    carries the full port range; for gates and managers `worker_ports`
    is None.

    `instance` is the actual server object; the supervisor does not call
    methods on it directly, but downstream modules (FaultMatrix,
    InvariantChecker, DiagnosticDumper) need access to its state.
    """

    node_id: str
    kind: ServerKind
    dc_id: str
    host: str
    tcp_port: int
    udp_port: int
    instance: Any
    """The concrete `GateServer` / `ManagerServer` / `WorkerServer`."""

    worker_ports: WorkerPorts | None = None
    """Set only for `ServerKind.WORKER`. Contains all derived ports."""

    started: bool = field(default=False)
    """Flipped by the harness once `await instance.start()` returns."""
