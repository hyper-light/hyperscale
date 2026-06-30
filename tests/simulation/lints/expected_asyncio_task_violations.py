"""
Phase 6b ratcheted snapshot — the set of production files under
``hyperscale/distributed/`` that currently contain a direct call to
``asyncio.create_task`` or ``asyncio.ensure_future``.

Why this lint
-------------

Phase 6's deterministic ``SimulationLoop`` guarantees ordering only
for tasks that route through ``loop.create_task`` (which the SIM
harness *controls*) or through a ``TaskRunner.run`` (which uses the
same loop's ``create_task`` under the hood). Direct calls to
``asyncio.create_task`` resolve through ``asyncio.get_running_loop()``
which, while it returns the right loop in well-behaved code, makes
every callsite implicitly dependent on the running-loop context.
That coupling is fine for REAL — but for SIM, a stray
``asyncio.create_task`` from a code path the harness doesn't
explicitly drive can land on the *wrong* loop (e.g., a default
``ProactorEventLoop`` on Windows, a real ``SelectorEventLoop`` on
unix) and silently bypass the SimulationLoop's determinism
guarantees.

The Phase 6b migration routes every direct call through one of:

1. ``self._task_runner.run(coro)`` — when the construction site has
   a ``TaskRunner`` injected.
2. ``loop.create_task(coro)`` — when the caller has the loop
   reference (rare; only loop-internal helpers).

Each migration commit shrinks the allowlist by one file. Final
state (after Phase 6b): only the three taskex internals remain —
``taskex/run.py``, ``taskex/task_runner.py``, ``taskex/task.py``.
These *are* the TaskRunner; their direct ``asyncio.ensure_future``
calls are how the runner submits work to the loop, so they stay on
the allowlist permanently.

Stored as Python (not a text snapshot) so the project's ``*.txt``
gitignore rule doesn't accidentally hide it from version control.
"""

EXPECTED_ASYNCIO_TASK_VIOLATIONS: frozenset[str] = frozenset(
    {
        # TaskRunner internals — permanent allowlist entries. These
        # files implement the runner itself and submit work to the
        # loop via ``asyncio.ensure_future`` by design.
        "hyperscale/distributed/taskex/run.py",
        "hyperscale/distributed/taskex/task_runner.py",
        "hyperscale/distributed/taskex/task.py",
        # Phase 6b migration backlog — each entry shrinks per
        # per-module commit. Ordered alphabetically by path.
        "hyperscale/distributed/health/probes.py",
        "hyperscale/distributed/jobs/gates/gate_job_timeout_tracker.py",
        "hyperscale/distributed/jobs/workflow_dispatcher.py",
        "hyperscale/distributed/leases/job_lease.py",
        "hyperscale/distributed/ledger/wal/wal_writer.py",
        "hyperscale/distributed/nodes/client/tracking.py",
        "hyperscale/distributed/nodes/gate/orphan_job_coordinator.py",
        "hyperscale/distributed/nodes/gate/server.py",
        "hyperscale/distributed/nodes/manager/discovery.py",
        "hyperscale/distributed/nodes/manager/rate_limiting.py",
        "hyperscale/distributed/server/events/lamport_runner.py",
        "hyperscale/distributed/server/server/mercury_sync_base_server.py",
        "hyperscale/distributed/swim/detection/hierarchical_failure_detector.py",
        "hyperscale/distributed/swim/detection/job_suspicion_manager.py",
        "hyperscale/distributed/swim/detection/suspicion_manager.py",
        "hyperscale/distributed/swim/health_aware_server.py",
        "hyperscale/distributed/swim/health/federated_health_monitor.py",
        "hyperscale/distributed/swim/health/health_monitor.py",
        "hyperscale/distributed/swim/health/out_of_band_health_channel.py",
        "hyperscale/distributed/swim/leadership/local_leader_election.py",
    }
)
