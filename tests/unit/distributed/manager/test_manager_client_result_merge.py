"""
A client that submitted directly gets each test workflow's merged stats.

Workers report one WorkflowStats per core; the manager concatenated them
and pushed the list to a client that submitted without a gate, which
reads the first -- one core's stats. A client-bound push of a test
workflow now carries the cores' stats merged into one, as a gate's does;
a gate-bound push keeps the per-core list the gate aggregates across
datacenters. A workflow that drives no load is never merged as load-test
stats: the push says which it is (``is_test`` was left at its default,
True, for every workflow), and its elapsed time is its longest core's.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import (
    TrackingToken,
    WorkflowFinalResult,
    WorkflowResultPush,
)
from hyperscale.distributed.nodes.manager import server as manager_server_module
from hyperscale.distributed.nodes.manager.server import ManagerServer

JOB_ID = "job-1"
WORKFLOW_ID = "wf-1"
CLIENT_CALLBACK = ("10.0.0.20", 8500)
ORIGIN_GATE = ("10.0.0.9", 8431)
PER_CORE_STATS = [
    {"core": "core-a", "elapsed": 4.0},
    {"core": "core-b", "elapsed": 6.5},
    {"core": "core-c", "elapsed": 5.0},
]
SUB_WORKFLOW_TOKEN = TrackingToken.for_workflow(
    "dc-1", "manager-0", JOB_ID, WORKFLOW_ID
).to_sub_workflow_token("worker-1")


class MergingResults:
    def merge_results(self, workflow_stats: list[dict]) -> dict:
        return {
            "core": "merged:" + "+".join(stats["core"] for stats in workflow_stats),
            "elapsed": max(stats["elapsed"] for stats in workflow_stats),
        }


def make_manager(
    origin_gate: tuple[str, int] | None,
    is_test: bool,
    sent: list[WorkflowResultPush],
) -> ManagerServer:
    manager = object.__new__(ManagerServer)
    parent_token = str(SUB_WORKFLOW_TOKEN.to_parent_workflow_token())
    manager._job_manager = SimpleNamespace(
        get_job_by_id=lambda job_id: SimpleNamespace(
            workflows={parent_token: SimpleNamespace(is_test=is_test)}
        )
    )
    manager._manager_state = SimpleNamespace(
        get_job_callback=lambda job_id: CLIENT_CALLBACK,
        get_client_callback=lambda job_id: None,
        get_job_origin_gate=lambda job_id: origin_gate,
    )
    manager._node_id = SimpleNamespace(datacenter="dc-1", short="manager-0")
    manager._leases = SimpleNamespace(get_fence_token=lambda job_id: 1)
    manager._clock = SimpleNamespace(time=lambda: 100.0)
    manager._config = SimpleNamespace(tcp_timeout_standard_seconds=5.0)
    manager._get_job_target_dcs_for_push = lambda job_id: ["dc-1"]
    manager._get_job_target_dc_count_for_push = lambda job_id, target_dcs: len(target_dcs)
    manager._data_plane_push_fields = lambda job_id, workflow_id, datacenter: {}

    async def send_job_update_to_origin(job_id, callback_addr, gate_method, client_method, payload, timeout):
        sent.append(WorkflowResultPush.load(payload))
        return (client_method, callback_addr)

    manager._send_job_update_to_origin = send_job_update_to_origin
    return manager


def final_result() -> WorkflowFinalResult:
    return WorkflowFinalResult(
        job_id=JOB_ID,
        workflow_id=WORKFLOW_ID,
        workflow_name="Workflow",
        status="completed",
        results=list(PER_CORE_STATS),
        context_updates=b"",
        error=None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("origin_gate", "is_test", "expected_results"),
    [
        (None, True, [{"core": "merged:core-a+core-b+core-c", "elapsed": 6.5}]),
        (ORIGIN_GATE, True, PER_CORE_STATS),
        (None, False, PER_CORE_STATS),
        (ORIGIN_GATE, False, PER_CORE_STATS),
    ],
)
async def test_a_client_bound_push_carries_merged_test_stats(
    monkeypatch: pytest.MonkeyPatch,
    origin_gate: tuple[str, int] | None,
    is_test: bool,
    expected_results: list[dict],
) -> None:
    monkeypatch.setattr(manager_server_module, "Results", MergingResults)
    sent: list[WorkflowResultPush] = []
    manager = make_manager(origin_gate, is_test, sent)

    await manager._push_workflow_result_to_client(final_result(), SUB_WORKFLOW_TOKEN)

    (push,) = sent
    assert push.results == expected_results
    assert push.is_client_ready is (origin_gate is None)
    assert push.is_test is is_test
    assert push.elapsed_seconds == 6.5
