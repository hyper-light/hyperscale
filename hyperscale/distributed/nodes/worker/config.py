"""
Worker configuration for WorkerServer: ``WorkerConfig`` lives in
``nodes/worker/models/worker_config.py``, its derivations in
``nodes/worker/worker_config_derivation.py``.
"""

from hyperscale.distributed.env import Env, load_env
from hyperscale.distributed.nodes.worker.models.worker_config import WorkerConfig


def create_worker_config_from_env(
    host: str,
    tcp_port: int,
    udp_port: int,
    datacenter_id: str = "default",
    seed_managers: list[tuple[str, int]] | None = None,
) -> WorkerConfig:
    """
    Create worker configuration from environment variables.

    Reads environment variables with WORKER_ prefix for configuration.

    Args:
        host: Worker host address
        tcp_port: Worker TCP port
        udp_port: Worker UDP port
        datacenter_id: Datacenter identifier
        seed_managers: Initial list of manager addresses

    Returns:
        WorkerConfig instance
    """
    env = load_env(Env, env_file="")
    config = WorkerConfig.from_env(
        env,
        host=host,
        tcp_port=tcp_port,
        udp_port=udp_port,
        datacenter_id=datacenter_id,
    )
    return config
