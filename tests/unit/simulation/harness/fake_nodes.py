"""
Node doubles for the continuous-invariant tests: each carries the REAL
production state objects an invariant reads (``ManagerState``,
``JobManager``, ``WorkerState``, ``CoreAllocator``, ``WorkerPool``,
``IncarnationTracker``, ``GossipBuffer``, ``GateJobManager``), so a check
that reads a renamed or reshaped field fails here, not silently in a run.
"""

from types import SimpleNamespace

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.jobs.core_allocator import CoreAllocator
from hyperscale.distributed.jobs.gates.gate_job_manager import GateJobManager
from hyperscale.distributed.jobs.job_manager import JobManager
from hyperscale.distributed.jobs.worker_pool import WorkerPool
from hyperscale.distributed.nodes.manager.leases import ManagerLeaseCoordinator
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.nodes.worker.config import WorkerConfig
from hyperscale.distributed.nodes.worker.state import WorkerState
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.slo import SLOConfig
from hyperscale.distributed.swim.detection.incarnation_tracker import IncarnationTracker
from hyperscale.distributed.swim.gossip.gossip_buffer import GossipBuffer

from tests.simulation.harness.server_handle import ServerHandle, ServerKind

HOST = "127.0.0.1"
WORKER_CORES = 4


def manager_handle(dc_id: str, index: int, tcp_port: int) -> ServerHandle:
    """A started manager double with real manager state."""
    env = Env()
    node_id = f"{dc_id}-manager-{index}"
    state = ManagerState(slo_config=SLOConfig.from_env(env))
    instance = SimpleNamespace(
        env=env,
        _node_id=SimpleNamespace(full=node_id),
        _manager_state=state,
        _job_manager=JobManager(
            datacenter=dc_id,
            manager_id=node_id,
            clock=RealClock(),
            max_budgeted_retries=env.RETRY_BUDGET_PER_WORKFLOW_MAX,
        ),
        _leases=ManagerLeaseCoordinator(
            state=state, config=None, logger=None, node_id=node_id, task_runner=None
        ),
        _worker_pool=WorkerPool(),
        _incarnation_tracker=IncarnationTracker(),
        _gossip_buffer=GossipBuffer(),
    )
    return _started(f"{dc_id}.manager.{index}", ServerKind.MANAGER, dc_id, tcp_port, instance)


def worker_handle(dc_id: str, index: int, tcp_port: int) -> ServerHandle:
    """A started worker double with a real allocator, worker state and config."""
    env = Env()
    config = WorkerConfig.from_env(env, HOST, tcp_port, tcp_port + 1, dc_id, total_cores=WORKER_CORES)
    allocator = CoreAllocator(WORKER_CORES)
    instance = SimpleNamespace(
        env=env,
        _node_id=SimpleNamespace(full=f"{dc_id}-worker-{index}"),
        _config=config,
        _core_allocator=allocator,
        _worker_state=WorkerState(
            allocator,
            throughput_interval_seconds=config.throughput_interval_seconds,
            completion_times_max_samples=config.completion_times_max_samples,
        ),
        _incarnation_tracker=IncarnationTracker(),
        _lifecycle_manager=SimpleNamespace(
            _server_pool=SimpleNamespace(_executor=SimpleNamespace(_processes={}))
        ),
    )
    return _started(f"{dc_id}.worker.{index}", ServerKind.WORKER, dc_id, tcp_port, instance)


def gate_handle(index: int, tcp_port: int, active_peer_count: int) -> ServerHandle:
    """A started gate double; its runtime state reports ``active_peer_count``."""
    instance = SimpleNamespace(
        env=Env(),
        _node_id=SimpleNamespace(full=f"global-gate-{index}"),
        _job_manager=GateJobManager(),
        _modular_state=SimpleNamespace(get_active_peer_count=lambda: active_peer_count),
        _incarnation_tracker=IncarnationTracker(),
        _gossip_buffer=GossipBuffer(),
    )
    return _started(f"global.gate.{index}", ServerKind.GATE, "global", tcp_port, instance)


def _started(node_id: str, kind: ServerKind, dc_id: str, tcp_port: int, instance: SimpleNamespace) -> ServerHandle:
    return ServerHandle(
        node_id=node_id,
        kind=kind,
        dc_id=dc_id,
        host=HOST,
        tcp_port=tcp_port,
        udp_port=tcp_port + 1,
        instance=instance,
        started=True,
    )
