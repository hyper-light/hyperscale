"""
A ClusterHarness scenario writes nothing to the working directory.

Nodes the harness built had no WAL directory, so a manager's idempotency
ledger fell back to ``Env.MERCURY_SYNC_LOGS_DIRECTORY`` -- the working
directory -- and running scenarios from the repository root left
``manager-idempotency-*.wal`` files there (some were committed). Every
node now keeps its WAL and logs in a per-run directory the harness
removes on exit. Run from an empty working directory with every node kind
(gate, manager, worker) and a workload, the directory stays empty and the
run directory is gone afterwards. ``Env``'s logs-directory default is
the working directory at import (the repository root, not the empty
directory the test enters), so that directory must gain nothing either.
"""

import os
import pathlib

import pytest

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.testing.workflows import SimpleWorkflow
from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    ExpectAllWorkflowsComplete,
    HarnessTimeouts,
    Submission,
    SubmissionPattern,
    WorkloadSpec,
)

WORKLOAD_TIMEOUT_SECONDS = 60.0


def _every_tier_spec() -> ClusterSpec:
    return ClusterSpec(
        gates=1,
        datacenters={"main": DCSpec(managers=1, workers=1, cores_per_worker=1)},
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        timeouts=HarnessTimeouts(stabilization_default=75.0),
    )


def _simple_workload() -> WorkloadSpec:
    return WorkloadSpec(
        submissions=[
            Submission(
                workflows=[([], SimpleWorkflow)],
                dc_count=1,
                timeout_seconds=WORKLOAD_TIMEOUT_SECONDS,
                vus=1,
            ),
        ],
        pattern=SubmissionPattern.SINGLE,
        expectations=[ExpectAllWorkflowsComplete(expected_workflow_names=[SimpleWorkflow.__name__])],
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_a_scenario_writes_nothing_to_the_working_directory(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    working_directory = tmp_path / "working-directory"
    working_directory.mkdir()
    monkeypatch.chdir(working_directory)
    default_logs_directory = pathlib.Path(Env().MERCURY_SYNC_LOGS_DIRECTORY)
    default_logs_directory_entries = set(os.listdir(default_logs_directory))

    async with ClusterHarness(
        _every_tier_spec(),
        mode=ExecutionMode.REAL,
        scenario_name="working_directory_untouched",
    ) as cluster:
        node_data_root = cluster.node_data_directory("any").parent
        async with cluster.workload(_simple_workload()) as driver:
            await driver.submit_and_wait()
        manager_id = cluster.managers("main")[0].node_id
        assert any(cluster.node_data_directory(manager_id).iterdir())

    assert sorted(os.listdir(working_directory)) == []
    assert set(os.listdir(default_logs_directory)) - default_logs_directory_entries == set()
    assert not node_data_root.exists()
