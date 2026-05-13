"""Datacenter specification — declarative shape of one DC's local cluster."""

from dataclasses import dataclass

from tests.simulation.harness.env_overrides import EnvOverrides


@dataclass(slots=True, frozen=True)
class DCSpec:
    """How many managers and workers live in one datacenter."""

    managers: int
    """Number of `ManagerServer` instances. Use 3 for quorum tests."""

    workers: int
    """Number of `WorkerServer` instances."""

    cores_per_worker: int = 2
    """Cores each worker advertises. Drives subprocess count."""

    worker_port_block_size: int = 500
    """Contiguous port block reserved per worker.

    Large topologies with one-core workers can set a smaller block to
    avoid exhausting the local port range while retaining collision-free
    derived worker-pool ports.
    """

    env: EnvOverrides | None = None
    """DC-level env overrides applied to every node in this DC."""
