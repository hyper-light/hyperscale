"""Port-range descriptor for a single worker.

Workers derive an internal UDP port `udp_port + total_cores ** 2` for
their `RemoteGraphManager`, then derive one controller UDP port per
core from that local UDP port. The harness must reserve all of these
derived ports along with the primary TCP/UDP pair to avoid collisions
with other workers in the same run.
"""

from dataclasses import dataclass


def worker_derived_ports(tcp: int, udp: int, cores: int) -> tuple[int, ...]:
    """Return every TCP/UDP port a worker runtime derives from its public pair."""
    if cores <= 0:
        raise ValueError("cores must be positive")

    derived_local_udp = udp + cores * cores
    controller_base = derived_local_udp + cores * cores
    return (
        tcp,
        udp,
        derived_local_udp,
        *(
            controller_base + core_index * cores
            for core_index in range(cores)
        ),
    )


def worker_port_offsets(cores: int, tcp_udp_offset: int) -> tuple[int, ...]:
    """Return every worker-derived port as an offset from its TCP block base."""
    return tuple(
        sorted(
            {
                port
                for port in worker_derived_ports(
                    tcp=0,
                    udp=tcp_udp_offset,
                    cores=cores,
                )
            }
        )
    )


@dataclass(slots=True, frozen=True)
class WorkerPorts:
    """All ports a single worker server occupies."""

    tcp: int
    """Primary TCP data port."""

    udp: int
    """Primary UDP SWIM port."""

    derived_local_udp: int
    """Internal UDP port: `udp + total_cores ** 2`. See worker/lifecycle.py:63."""

    controller_udp_ports: tuple[int, ...]
    """Per-core local controller UDP ports. See worker/lifecycle.py:get_worker_ips."""

    @classmethod
    def for_worker(cls, tcp: int, udp: int, cores: int) -> "WorkerPorts":
        worker_ports = worker_derived_ports(tcp=tcp, udp=udp, cores=cores)
        tcp_port, udp_port, derived_local_udp, *controller_udp_ports = worker_ports
        return cls(
            tcp=tcp_port,
            udp=udp_port,
            derived_local_udp=derived_local_udp,
            controller_udp_ports=tuple(controller_udp_ports),
        )

    def all_ports(self) -> list[int]:
        return [
            self.tcp,
            self.udp,
            self.derived_local_udp,
            *self.controller_udp_ports,
        ]
