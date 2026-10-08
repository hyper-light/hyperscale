"""
AD-41 uncertainty-aware, graduated enforcement of per-workflow resource budgets.
"""

from collections.abc import Awaitable, Callable

from hyperscale.distributed.runtime import Clock

from .enforcement_action import EnforcementAction
from .resource_budget import ResourceBudget
from .resource_violation_type import ResourceViolationType
from .violation_state import ViolationState

# The kill gate's confidence: a workflow is killed only when its estimate
# minus two standard deviations still exceeds the limit (~97.7% one-sided).
KILL_CONFIDENCE_SIGMAS = 2.0

# AD-41 failure modes: a kill request that does not take effect is
# "retried on next heartbeat" -- once -- and then escalates to evicting
# the worker that ignores it.
MAX_KILL_ATTEMPTS = 2


class ResourceEnforcer:
    """Judges each workflow's resource estimates against its job's budget.

    Graduated response per resource (CPU, memory):

    * above ``warning_threshold`` of the limit for the warning grace: WARN,
      once per violation;
    * above ``throttle_threshold`` once warned, for a warning grace:
      THROTTLE_WORKFLOW -- the workflow's concurrency is scaled by
      ``throttle limit / estimate`` (load scales with concurrency, so this
      aims the estimate at the throttle line), again every warning grace
      while it stays above; the throttle is released when the workflow is
      back under ``warning_threshold`` on every resource;
    * certainly above ``kill_threshold`` -- the estimate minus two standard
      deviations exceeds it -- for the kill grace, once warned: KILL_WORKFLOW;
    * still violating a kill grace after each kill request (lost, or
      ignored): the request is retried once, then EVICT_WORKER.

    Graces stretch with relative uncertainty (``1 + sigma / value``): a
    noisy estimate is watched longer before anything acts on it. Dropping
    back under the warning threshold ends the violation.

    State is per (workflow, resource); a finished workflow is released,
    and a budget is held only while its job is running.
    """

    __slots__ = (
        "_clock",
        "_default_budget",
        "_on_warn",
        "_on_throttle_workflow",
        "_on_release_throttle",
        "_on_kill_workflow",
        "_on_evict_worker",
        "_violations",
        "_throttled_resources",
        "_job_budgets",
    )

    def __init__(
        self,
        clock: Clock,
        default_budget: ResourceBudget,
        on_warn: Callable[[str, str, ResourceViolationType, float, float], Awaitable[None]],
        on_throttle_workflow: Callable[[str, str, str, float], Awaitable[bool]],
        on_release_throttle: Callable[[str, str, str], Awaitable[bool]],
        on_kill_workflow: Callable[[str, str, str, ResourceViolationType], Awaitable[bool]],
        on_evict_worker: Callable[[str, ResourceViolationType], Awaitable[bool]],
    ) -> None:
        self._clock = clock
        self._default_budget = default_budget
        self._on_warn = on_warn
        self._on_throttle_workflow = on_throttle_workflow
        self._on_release_throttle = on_release_throttle
        self._on_kill_workflow = on_kill_workflow
        self._on_evict_worker = on_evict_worker
        self._violations: dict[tuple[str, ResourceViolationType], ViolationState] = {}
        # Per throttled workflow, the resources whose violation throttled it:
        # the workflow has one concurrency cap, released when none remain.
        self._throttled_resources: dict[str, set[ResourceViolationType]] = {}
        self._job_budgets: dict[str, ResourceBudget] = {}

    @property
    def tracked_violation_count(self) -> int:
        return len(self._violations)

    def assign_budget(self, job_id: str, budget: ResourceBudget) -> None:
        """Enforce ``job_id``'s workflows against ``budget``."""
        self._job_budgets[job_id] = budget

    def release_job(self, job_id: str) -> None:
        self._job_budgets.pop(job_id, None)

    @property
    def throttled_workflow_count(self) -> int:
        return len(self._throttled_resources)

    def release_workflow(self, workflow_id: str) -> None:
        """Forget a finished workflow's violations (and its throttle, which
        ended with it)."""
        for violation_type in ResourceViolationType:
            self._violations.pop((workflow_id, violation_type), None)
        self._throttled_resources.pop(workflow_id, None)

    async def check_workflow(
        self,
        workflow_id: str,
        worker_id: str,
        job_id: str,
        cpu_percent: float,
        cpu_uncertainty: float,
        memory_bytes: float,
        memory_uncertainty: float,
    ) -> EnforcementAction:
        """Judge one workflow's latest estimates; CPU first, then memory."""
        budget = self._job_budgets.get(job_id, self._default_budget)
        cpu_action = await self._check_resource(
            workflow_id,
            worker_id,
            job_id,
            ResourceViolationType.CPU_EXCEEDED,
            cpu_percent,
            cpu_uncertainty,
            budget.max_cpu_percent,
            budget,
        )
        if cpu_action is not EnforcementAction.NONE:
            return cpu_action
        return await self._check_resource(
            workflow_id,
            worker_id,
            job_id,
            ResourceViolationType.MEMORY_EXCEEDED,
            memory_bytes,
            memory_uncertainty,
            float(budget.max_memory_bytes),
            budget,
        )

    async def _check_resource(
        self,
        workflow_id: str,
        worker_id: str,
        job_id: str,
        violation_type: ResourceViolationType,
        value: float,
        uncertainty: float,
        limit: float,
        budget: ResourceBudget,
    ) -> EnforcementAction:
        key = (workflow_id, violation_type)
        if value <= limit * budget.warning_threshold:
            self._violations.pop(key, None)
            await self._end_throttle(workflow_id, worker_id, job_id, violation_type)
            return EnforcementAction.NONE

        now = self._clock.monotonic()
        state = self._violations.get(key)
        if state is None:
            state = ViolationState(worker_id=worker_id, job_id=job_id, started_at=now)
            self._violations[key] = state

        uncertainty_factor = 1.0 + uncertainty / max(value, 1.0)
        kill_grace = budget.kill_grace_seconds * uncertainty_factor
        certain = value - KILL_CONFIDENCE_SIGMAS * uncertainty > limit * budget.kill_threshold
        state.certain_since = (state.certain_since or now) if certain else None

        if (
            not state.warning_sent
            and now - state.started_at >= budget.warning_grace_seconds * uncertainty_factor
        ):
            state.warning_sent = True
            state.warned_at = now
            await self._on_warn(workflow_id, worker_id, violation_type, value, limit)
            return EnforcementAction.WARN

        # As in the AD-41 reference enforcer, nothing is killed before it
        # has been warned about: the warning grace bounds how fast a
        # transient spike can cost a workflow its run.
        if not state.warning_sent:
            return EnforcementAction.NONE
        if self._kill_due(state, now, kill_grace):
            return await self._escalate(workflow_id, worker_id, violation_type, state, now)
        throttle_interval = budget.warning_grace_seconds * uncertainty_factor
        if value > limit * budget.throttle_threshold and self._throttle_due(state, now, throttle_interval):
            return await self._throttle(
                workflow_id, worker_id, violation_type, state, now, (limit * budget.throttle_threshold) / value
            )
        return EnforcementAction.NONE

    @staticmethod
    def _kill_due(state: ViolationState, now: float, kill_grace: float) -> bool:
        if state.certain_since is None or now - state.certain_since < kill_grace:
            return False
        return state.kill_requested_at is None or now - state.kill_requested_at >= kill_grace

    @staticmethod
    def _throttle_due(state: ViolationState, now: float, throttle_interval: float) -> bool:
        """A sustained interval since the warning, or since the last throttle
        (whose effect shows in the estimates by then)."""
        since = state.throttled_at if state.throttled_at is not None else state.warned_at
        return since is not None and now - since >= throttle_interval

    async def _throttle(
        self,
        workflow_id: str,
        worker_id: str,
        violation_type: ResourceViolationType,
        state: ViolationState,
        now: float,
        scale: float,
    ) -> EnforcementAction:
        state.throttled_at = now
        if not await self._on_throttle_workflow(workflow_id, worker_id, state.job_id, scale):
            return EnforcementAction.NONE
        self._throttled_resources.setdefault(workflow_id, set()).add(violation_type)
        return EnforcementAction.THROTTLE_WORKFLOW

    async def _end_throttle(
        self,
        workflow_id: str,
        worker_id: str,
        job_id: str,
        violation_type: ResourceViolationType,
    ) -> None:
        """Release the workflow's throttle once no resource still holds it.

        A release that does not reach the worker leaves the throttle
        recorded, so the next in-bound check retries it -- a lost release
        must not leave a workflow throttled for the rest of its run.
        """
        throttled = self._throttled_resources.get(workflow_id)
        if throttled is None or violation_type not in throttled:
            return
        if len(throttled) > 1:
            throttled.discard(violation_type)
            return
        if await self._on_release_throttle(workflow_id, worker_id, job_id):
            del self._throttled_resources[workflow_id]

    async def _escalate(
        self,
        workflow_id: str,
        worker_id: str,
        violation_type: ResourceViolationType,
        state: ViolationState,
        now: float,
    ) -> EnforcementAction:
        """Kill (first request or its retry), then evict a worker that ignores it."""
        if state.kill_attempts >= MAX_KILL_ATTEMPTS:
            return await self._evict_unresponsive_worker(workflow_id, worker_id, violation_type)

        state.kill_attempts += 1
        state.kill_requested_at = now
        if await self._on_kill_workflow(workflow_id, worker_id, state.job_id, violation_type):
            return EnforcementAction.KILL_WORKFLOW
        return EnforcementAction.NONE

    async def _evict_unresponsive_worker(
        self,
        workflow_id: str,
        worker_id: str,
        violation_type: ResourceViolationType,
    ) -> EnforcementAction:
        """Evict a worker that ignored both kill requests (AD-41 failure
        modes), forgetting the workflow once the eviction took."""
        if await self._on_evict_worker(worker_id, violation_type):
            self.release_workflow(workflow_id)
            return EnforcementAction.EVICT_WORKER
        return EnforcementAction.NONE
