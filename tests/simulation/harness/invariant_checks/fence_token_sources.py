"""
Where each node kind keeps its per-job fence tokens (AD-10). Every map
named here only ever moves a job's token forward in correct operation:

* manager ``lease``: ``ManagerState._job_fencing_tokens``, the job
  leadership fence (``apply_job_leadership`` accepts only a newer token
  or an idempotent re-claim);
* manager ``dispatch``: ``JobManager._job_fence_tokens``, the
  ``(term << 32) | counter`` token each dispatch carries;
* worker ``accepted``: ``WorkerState._job_fence_tokens``, the newest
  token the worker accepted from a job leader;
* gate ``gate``: ``GateJobManager._job_fence_tokens``, the newest token
  the gate saw for the job.
"""

from collections.abc import Callable

from tests.simulation.harness.server_handle import ServerHandle, ServerKind

FenceTokenMap = dict[str, int]


def _manager_lease_tokens(handle: ServerHandle) -> FenceTokenMap:
    return handle.instance._manager_state._job_fencing_tokens


def _manager_dispatch_tokens(handle: ServerHandle) -> FenceTokenMap:
    return handle.instance._job_manager._job_fence_tokens


def _worker_accepted_tokens(handle: ServerHandle) -> FenceTokenMap:
    return handle.instance._worker_state._job_fence_tokens


def _gate_tokens(handle: ServerHandle) -> FenceTokenMap:
    return handle.instance._job_manager._job_fence_tokens


FENCE_TOKEN_SOURCES: dict[ServerKind, tuple[tuple[str, Callable[[ServerHandle], FenceTokenMap]], ...]] = {
    ServerKind.MANAGER: (
        ("lease", _manager_lease_tokens),
        ("dispatch", _manager_dispatch_tokens),
    ),
    ServerKind.WORKER: (("accepted", _worker_accepted_tokens),),
    ServerKind.GATE: (("gate", _gate_tokens),),
}
