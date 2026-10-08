"""
How WorkerConfig derives its values: the executor core count, Env
defaults and the orphan grace.
"""

from hyperscale.distributed.env import Env
from hyperscale.distributed.runtime import (
    RealSystemResources,
    SystemResources,
)

# Machine-telemetry seam: swap_defaults rebinds under SIM.
_DEFAULT_SYSTEM_RESOURCES: SystemResources = RealSystemResources()


def _get_os_cpus() -> int:
    """Get OS CPU count via the machine-telemetry seam (constant under
    SIM, live in REAL mode)."""
    return _DEFAULT_SYSTEM_RESOURCES.cpu_count(logical=False)


def _resolve_total_cores(env: Env, explicit_total_cores: int | None) -> int:
    """Resolve the worker's executor core count.

    Precedence: an explicit constructor value, then ``WORKER_MAX_CORES``
    (unset or ``0`` means "auto"), then the physical core count. Values
    below one are rejected instead of being coerced — a zero-core worker
    would register but could never accept dispatch.
    """
    if explicit_total_cores is not None:
        return _require_positive_core_count(explicit_total_cores, "total_cores")

    env_total_cores = env.WORKER_MAX_CORES
    if env_total_cores:
        return _require_positive_core_count(env_total_cores, "WORKER_MAX_CORES")

    return _get_os_cpus()


def _require_positive_core_count(core_count: int, source_name: str) -> int:
    """Return ``core_count`` or raise when it cannot host an executor."""
    if core_count < 1:
        raise ValueError(f"{source_name} must be at least 1, got {core_count}")

    return core_count


def _default_env_value(name: str):
    """Return the canonical distributed Env default for ``name``."""
    return getattr(Env(), name)



def derive_orphan_grace_seconds(env: Env) -> float:
    """How long a workflow whose job leader died waits for its new leader
    before the worker cancels it: the time the cluster needs to replace the
    leader and say so -- the surviving managers agree the leader is dead
    (one suspicion window, the no-witness one at worst: a worker cannot see
    whether its managers have witnesses), elect a datacenter leader if the
    dead one led (pre-vote, election timeout and its jitter), and deliver
    the transfer (one standard request). Observed rescues taking longer
    raise it (WorkerState.longest_orphan_rescue_seconds); AD-26 extensions
    lengthen it while managers keep heartbeating the worker."""
    return (
        max(env.SWIM_SUSPICION_MAX_TIMEOUT, env.SWIM_NO_WITNESS_SUSPICION_TIMEOUT)
        + env.LEADER_PRE_VOTE_TIMEOUT
        + env.LEADER_ELECTION_TIMEOUT_BASE
        + env.LEADER_ELECTION_TIMEOUT_JITTER
        + env.MANAGER_TCP_TIMEOUT_STANDARD
    )
