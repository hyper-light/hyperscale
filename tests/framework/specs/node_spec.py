from dataclasses import dataclass


@dataclass(slots=True)
class NodeSpec:
    node_type: str
    dc_id: str | None
    host: str
    tcp_port: int
    udp_port: int
    total_cores: int | None = None
    seed_managers: list[tuple[str, int]] | None = None
    gate_peers: list[tuple[str, int]] | None = None
    gate_udp_peers: list[tuple[str, int]] | None = None
    manager_peers: list[tuple[str, int]] | None = None
    manager_udp_peers: list[tuple[str, int]] | None = None
    env_overrides: dict[str, str] | None = None
