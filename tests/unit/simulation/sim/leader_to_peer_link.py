"""
The fault the manager datacenter scenarios control: TCP from the datacenter
leader to its peer managers, delayed or cut.
"""


class LeaderToPeerLink:
    """A ``SimulationRuntime`` fault check: TCP from ``leader_address`` to
    any of the peer managers is delayed by ``delay_seconds``, and dropped
    while ``cut`` -- or, to the peers in ``cut_destinations``, dropped to
    them alone. Everything else is delivered."""

    def __init__(self, manager_tcp_addresses: frozenset[tuple[str, int]]) -> None:
        self._manager_tcp_addresses = manager_tcp_addresses
        self.leader_address: tuple[str, int] | None = None
        self.delay_seconds = 0.0
        self.cut = False
        self.cut_destinations: frozenset[tuple[str, int]] = frozenset()

    def __call__(
        self,
        protocol: str,
        from_address: tuple[str, int],
        to_address: tuple[str, int],
    ) -> tuple[bool, float]:
        if (
            protocol != "tcp"
            or from_address != self.leader_address
            or to_address not in self._manager_tcp_addresses
        ):
            return True, 0.0
        return not (self.cut or to_address in self.cut_destinations), self.delay_seconds
