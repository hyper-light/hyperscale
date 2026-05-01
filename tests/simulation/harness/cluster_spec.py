"""Cluster specification — declarative top-level shape of the system under test."""

from dataclasses import dataclass, field

from tests.simulation.harness.dc_spec import DCSpec
from tests.simulation.harness.env_overrides import EnvOverrides
from tests.simulation.harness.timeouts import HarnessTimeouts


@dataclass(slots=True, frozen=True)
class ClusterSpec:
    """Complete declarative description of a cluster the harness will build.

    `gates=0` means client connects directly to managers (L1/L2 levels).
    `gates>=1` means client connects to gates which route to per-DC manager
    quorums (L3 level).
    """

    gates: int
    datacenters: dict[str, DCSpec]

    env: EnvOverrides = field(default_factory=EnvOverrides)
    """Cluster-wide env overrides; per-DC and per-node overrides layer on top."""

    per_node_env: dict[str, EnvOverrides] = field(default_factory=dict)
    """Per-node env overrides keyed by node id (e.g. "east.manager.0")."""

    timeouts: HarnessTimeouts = field(default_factory=HarnessTimeouts)

    base_port: int = 9000
    """Lowest port the harness will allocate. PortAllocator probes upward from here."""

    host: str = "127.0.0.1"
    """All nodes bind to this host. Multi-host orchestration is a non-goal."""

    def total_node_count(self) -> int:
        manager_total = sum(dc.managers for dc in self.datacenters.values())
        worker_total = sum(dc.workers for dc in self.datacenters.values())
        return self.gates + manager_total + worker_total
