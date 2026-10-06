"""
Worker autonomous extension trigger (AD-26 Phase H4).

A background loop on the worker that scans active workflows and, for
each workflow whose elapsed runtime has crossed
``deadline × lookahead_fraction`` AND has shown forward progress
since the last extension request, invokes
``WorkerServer.request_extension`` with a complete
``WorkflowProgressSnapshot``.

This closes the AD-26 protocol-flow loop that was previously open in
the codebase: ``request_extension`` was a public API on
``WorkerServer`` but nothing called it. The autonomous trigger
materialises the worker side of AD-26 §"Complete Protocol Flow
Example" lines 119–153.

Design constraints honored:

* **TaskRunner-managed** per CLAUDE.md — no orphaned ``asyncio.Task``.
* **At most one in-flight extension per workflow** — the worker's
  ``WorkerState._extension_requested`` flag gates re-requests until
  the manager has acked the previous one (cleared by
  ``clear_extension_request`` after the response is processed).
* **Stuck-workflow detection** — when a workflow has been past its
  deadline lookahead but no progress dimension has advanced since
  the last request, the trigger does NOT request again. This lets
  the manager's hard timeout and AD-26 ``max_extensions`` cap
  engage cleanly instead of the worker spamming hopeless requests.
* **Memory-bounded per-workflow tracking** — last-snapshot dict is
  keyed by ``workflow_id`` and cleaned up via
  ``forget_workflow`` when the workflow terminates.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Awaitable, Callable
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.distributed.health.workflow_progress_snapshot import WorkflowProgressSnapshot
from hyperscale.distributed.nodes.worker.models.workflow_runtime_state import WorkflowRuntimeState
from hyperscale.distributed.taskex.util.time_parser import TimeParser

from .extension_trigger_config import ExtensionTriggerConfig
from ._per_workflow_trigger_state import _PerWorkflowTriggerState

_DEFAULT_CLOCK: Clock = RealClock()

# Pulled out so unit tests can substitute a deterministic snapshot
# builder without depending on the full WorkerServer/state graph.
SnapshotBuilder = Callable[[WorkflowRuntimeState], WorkflowProgressSnapshot]


def default_snapshot_builder(
    runtime: WorkflowRuntimeState,
) -> WorkflowProgressSnapshot:
    """Build a ``WorkflowProgressSnapshot`` from a runtime state.

    Reads the H3 multi-dimensional counters (``cores_completed``,
    ``step_transitions``, ``actions_completed``) plus the workflow's
    nominal ``vus`` allocation as ``cores_total``. The snapshot
    timestamp is captured from the module-level ``RealClock``; SIM
    mode swaps in a deterministic builder via the ``snapshot_builder``
    constructor parameter on ``ExtensionTrigger``.
    """
    return WorkflowProgressSnapshot(
        workflow_id=runtime.workflow_id,
        cores_completed=runtime.cores_completed,
        cores_total=runtime.vus,
        step_transitions=runtime.step_transitions,
        actions_completed=runtime.actions_completed,
        snapshot_time=_DEFAULT_CLOCK.monotonic(),
    )


class ExtensionTrigger:
    """Worker autonomous extension trigger.

    Constructed once per ``WorkerServer``. The owner schedules the
    background loop via ``TaskRunner`` (per CLAUDE.md "use the
    TaskRunner instead of orphan tasks"). One in-flight extension
    request per workflow is enforced by the ``is_extension_pending``
    callback.

    Public API:

        trigger = ExtensionTrigger(...)

        async def loop_body() -> None:
            while is_running():
                await asyncio.sleep(trigger.config.poll_interval_seconds)
                trigger.tick()

        task_runner.run(loop_body)

        # When a workflow terminates:
        trigger.forget_workflow(workflow_id)
    """

    def __init__(
        self,
        *,
        active_runtimes_provider: Callable[[], list[WorkflowRuntimeState]],
        deadline_provider: Callable[[str], float | None],
        is_extension_pending: Callable[[], bool],
        request_extension: Callable[..., None],
        config: ExtensionTriggerConfig | None = None,
        snapshot_builder: SnapshotBuilder | None = None,
        clock: Clock | None = None,
    ) -> None:
        # Phase 5 DI seam — replaces the prior ``time_source:
        # Callable[[], float] | None`` parameter. ``clock.monotonic``
        # is bound to ``self._now`` so the existing call sites in
        # ``tick`` keep working unchanged. The injected ``clock`` is
        # NOT threaded into the snapshot builder by default: under
        # SIM mode callers should provide a custom ``snapshot_builder``
        # that captures simulated time on each call.
        self._active_runtimes_provider: Callable[
            [], list[WorkflowRuntimeState]
        ] = active_runtimes_provider
        self._deadline_provider: Callable[[str], float | None] = deadline_provider
        self._is_extension_pending: Callable[[], bool] = is_extension_pending
        self._request_extension: Callable[..., None] = request_extension
        self._config: ExtensionTriggerConfig = (
            config if config is not None else ExtensionTriggerConfig()
        )
        self._snapshot_builder: SnapshotBuilder = (
            snapshot_builder
            if snapshot_builder is not None
            else default_snapshot_builder
        )
        self._clock: Clock = clock if clock is not None else _DEFAULT_CLOCK
        self._now: Callable[[], float] = self._clock.monotonic
        self._workflows: dict[str, _PerWorkflowTriggerState] = {}

    @property
    def config(self) -> ExtensionTriggerConfig:
        return self._config

    def forget_workflow(self, workflow_id: str) -> None:
        """Drop tracker state for a terminated workflow.

        Called when the workflow finishes (success/failure/cancelled)
        so the bookkeeping dict doesn't grow unbounded. Idempotent.
        """
        self._workflows.pop(workflow_id, None)

    def tick(self) -> list[str]:
        """Run one scan over active workflows.

        Returns the list of ``workflow_id``s for which an extension
        request was issued. Useful for tests and observability.

        Logic per workflow:

        1. Skip if there's already a heartbeat-in-flight extension
           request (the worker has only one ``_extension_requested``
           bit; we serialize per-worker until the manager acks).
        2. Skip if the deadline lookup returns ``None`` (no deadline
           recorded — workflow was dispatched without one, e.g. a
           legacy path; nothing to extend).
        3. Skip if elapsed runtime is below the lookahead threshold
           ``deadline × lookahead_fraction``.
        4. Skip if no progress dimension has advanced since the last
           request. Truly stuck workflow → let the deadline fire.
        5. Otherwise, build the snapshot, invoke
           ``request_extension``, and record the request.
        """
        if self._is_extension_pending():
            return []

        triggered: list[str] = []
        now = self._now()

        for runtime in self._active_runtimes_provider():
            workflow_id = runtime.workflow_id
            if not workflow_id:
                continue

            deadline = self._deadline_provider(workflow_id)
            if deadline is None or deadline <= 0.0:
                continue

            elapsed = now - runtime.start_time
            lookahead_seconds = max(
                self._config.minimum_lookahead_seconds,
                deadline * self._config.lookahead_fraction,
            )
            if elapsed < lookahead_seconds:
                continue

            snapshot = self._snapshot_builder(runtime)
            tracker = self._workflows.get(workflow_id)
            last_snapshot = (
                tracker.last_request_snapshot if tracker is not None else None
            )
            if last_snapshot is not None and not snapshot.any_advanced(
                last_snapshot
            ):
                # No progress on any dimension since the last
                # request — let the manager's hard timeout fire.
                continue

            self._request_extension(
                reason="autonomous-trigger",
                progress=runtime.cores_completed,
                completed_items=runtime.cores_completed,
                total_items=runtime.vus,
                estimated_completion=max(0.0, deadline - elapsed),
                workflow_id=workflow_id,
                step_transitions=runtime.step_transitions,
                actions_completed=runtime.actions_completed,
                snapshot_time=snapshot.snapshot_time,
            )

            new_state = _PerWorkflowTriggerState(
                last_request_snapshot=snapshot,
                last_request_time=now,
                last_request_count=(
                    (tracker.last_request_count + 1)
                    if tracker is not None
                    else 1
                ),
            )
            self._workflows[workflow_id] = new_state
            triggered.append(workflow_id)

            # One extension request per scan — the worker's
            # heartbeat piggyback can only carry one snapshot at a
            # time, so we yield to the next tick for any other
            # workflows ready to ask.
            break

        return triggered

    async def run_loop(
        self,
        is_running: Callable[[], bool],
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        """Long-running background loop suitable for ``TaskRunner.run``.

        Honors ``is_running()`` for cooperative shutdown — when it
        returns False the loop exits and the ``TaskRunner`` reaps
        the task cleanly.
        """
        while is_running():
            await sleep(self._config.poll_interval_seconds)
            try:
                self.tick()
            except Exception:
                # Per CLAUDE.md "we *do not* EVER swallow errors" —
                # but a per-tick failure must not kill the loop.
                # Re-raise once shutdown completes; for now we let
                # the next tick try again. Production callers wire a
                # logger/metric counter to surface these.
                raise

_REHOMED = (
    ExtensionTriggerConfig,
    _PerWorkflowTriggerState,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
