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

    env: EnvOverrides | None = None
    """DC-level env overrides applied to every node in this DC."""
