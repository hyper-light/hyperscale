from dataclasses import dataclass

from .node_spec import NodeSpec


@dataclass(slots=True)
class ClusterSpec:
    template: str | None
    gate_count: int
    dc_count: int
    managers_per_dc: int
    workers_per_dc: int
    cores_per_worker: int
    base_gate_tcp: int
    base_manager_tcp: int
    base_worker_tcp: int
    client_port: int
    stabilization_seconds: int
    worker_registration_seconds: int
    nodes: list[NodeSpec] | None = None
    env_overrides: dict[str, str] | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "ClusterSpec":
        template = data.get("template")
        gate_count = int(data.get("gate_count", 1))
        dc_count = int(data.get("dc_count", 1))
        managers_per_dc = int(data.get("managers_per_dc", 1))
        workers_per_dc = int(data.get("workers_per_dc", 1))
        cores_per_worker = int(data.get("cores_per_worker", 1))
        base_gate_tcp = int(data.get("base_gate_tcp", 8000))
        base_manager_tcp = int(data.get("base_manager_tcp", 9000))
        base_worker_tcp = int(data.get("base_worker_tcp", 9500))
        client_port = int(data.get("client_port", 9900))
        stabilization_seconds = int(data.get("stabilization_seconds", 15))
        worker_registration_seconds = int(data.get("worker_registration_seconds", 10))
        nodes_data = data.get("nodes")
        nodes = None
        if nodes_data:
            nodes = [NodeSpec(**node) for node in nodes_data]
        env_overrides = data.get("env_overrides")
        return cls(
            template=template,
            gate_count=gate_count,
            dc_count=dc_count,
            managers_per_dc=managers_per_dc,
            workers_per_dc=workers_per_dc,
            cores_per_worker=cores_per_worker,
            base_gate_tcp=base_gate_tcp,
            base_manager_tcp=base_manager_tcp,
            base_worker_tcp=base_worker_tcp,
            client_port=client_port,
            stabilization_seconds=stabilization_seconds,
            worker_registration_seconds=worker_registration_seconds,
            nodes=nodes,
            env_overrides=env_overrides,
        )
