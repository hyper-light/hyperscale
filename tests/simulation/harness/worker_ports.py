"""Port-range descriptor for a single worker.

Workers derive an internal UDP port `udp_port + total_cores ** 2` for
their `LocalServerPool`. The harness must reserve that derived port
along with the primary TCP/UDP pair to avoid collisions with other
workers in the same run.
"""

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class WorkerPorts:
    """All ports a single worker server occupies."""

    tcp: int
    """Primary TCP data port."""

    udp: int
    """Primary UDP SWIM port."""

    derived_local_udp: int
    """Internal UDP port: `udp + total_cores ** 2`. See worker/lifecycle.py:63."""

    @classmethod
    def for_worker(cls, tcp: int, udp: int, cores: int) -> "WorkerPorts":
        return cls(tcp=tcp, udp=udp, derived_local_udp=udp + cores * cores)

    def all_ports(self) -> list[int]:
        return [self.tcp, self.udp, self.derived_local_udp]
