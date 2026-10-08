"""
Gate job failover coordination (AD-36 Part 13: mid-flight failover).

A job's leader gate moves the unfinished work of a datacenter the job lost
while it ran there to a healthy datacenter not already running it --
at-least-once execution, the re-run marked in the job's results -- and
tells the lost datacenter to stop running the job once it answers.
"""

import asyncio
import dataclasses
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from hyperscale.distributed.jobs.job_status_order import JobStatusOrder
from hyperscale.distributed.models import (
    CancelJob,
    DatacenterHealth,
    DatacenterSubstitution,
    GlobalJobStatus,
    JobCancelResponse,
    JobProgress,
    JobSubmission,
)
from hyperscale.distributed.runtime import Clock
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ServerError,
    ServerInfo,
    ServerWarning,
)

if TYPE_CHECKING:
    from hyperscale.distributed.jobs import JobLeadershipTracker
    from hyperscale.distributed.jobs.gates import GateJobManager, GateJobTimeoutTracker
    from hyperscale.distributed.nodes.gate.dispatch_coordinator import GateDispatchCoordinator
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
    from hyperscale.distributed.taskex import TaskRunner


class GateJobFailoverCoordinator:
    """
    Moves a job off a datacenter it lost mid-run (AD-36 Part 13).

    Every check interval -- the manager heartbeat interval, the fastest a
    datacenter's health classification changes -- for each job this gate
    leads that is not terminal, is not best-effort (AD-44: a best-effort
    job chose to complete without the datacenters that do not report) and
    whose submission it holds, a datacenter the job runs in that the
    routing view classifies UNHEALTHY is lost. Its unfinished workflows --
    no result of theirs from it reached the gate and none was aggregated
    -- re-run in the best eligible datacenter not already holding the
    job, with the job's remaining budget; their ancestors re-run with them
    for the context the unfinished ones read. The placement change commits
    to a quorum of gates, and to the job's durable record, before the
    re-run is dispatched: a gate taking the job over, or recovering it
    from its ledger, aggregates by the same result slots.

    A re-run no manager of the replacement took is a loss of the
    replacement too: the share moves on. A datacenter the job moved off
    is told to stop until a manager of it confirms the job is stopped,
    ended, or held nowhere there.

    State kept here is this leader's own, per job, and goes with the job
    (``forget_job``): which replacements took their re-run, which released
    datacenters confirmed, and the dispatches and cancels in flight. A
    gate taking a job over re-sends each re-run -- a manager answers a
    resubmitted job from the job it holds.
    """

    __slots__ = (
        "_state",
        "_logger",
        "_task_runner",
        "_job_manager",
        "_job_leadership_tracker",
        "_job_timeout_tracker",
        "_dispatch_coordinator",
        "_datacenter_managers",
        "_clock",
        "_send_tcp",
        "_get_node_addr",
        "_get_node_id_short",
        "_is_running",
        "_classify_datacenter_health",
        "_route_replacement",
        "_delivered_workflow_ids",
        "_release_workflow_timeouts",
        "_replicate_placement",
        "_record_reassignment",
        "_check_interval_seconds",
        "_cancel_timeout_seconds",
        "_failover_locks",
        "_accepted_reruns",
        "_confirmed_releases",
        "_unplaceable_reported",
        "_reruns_in_flight",
        "_release_cancels_in_flight",
    )

    def __init__(
        self,
        state: "GateRuntimeState",
        logger: Logger,
        task_runner: "TaskRunner",
        job_manager: "GateJobManager",
        job_leadership_tracker: "JobLeadershipTracker",
        job_timeout_tracker: "GateJobTimeoutTracker",
        dispatch_coordinator: "GateDispatchCoordinator",
        datacenter_managers: dict[str, list[tuple[str, int]]],
        clock: Clock,
        *,
        send_tcp: Callable[
            [tuple[str, int], str, bytes, float],
            Awaitable[tuple[bytes | Exception | None, float]],
        ],
        get_node_addr: Callable[[], tuple[str, int]],
        get_node_id_short: Callable[[], str],
        is_running: Callable[[], bool],
        classify_datacenter_health: Callable[[str], str],
        route_replacement: Callable[[str, set[str] | None, frozenset[str], float], str | None],
        delivered_workflow_ids: Callable[[str, str], Awaitable[set[str]]],
        release_workflow_timeouts: Callable[[str, set[str]], Awaitable[None]],
        replicate_placement: Callable[
            [str, list[str], list[DatacenterSubstitution], list[str]],
            Awaitable[bool],
        ],
        record_reassignment: Callable[[str, DatacenterSubstitution], Awaitable[None]],
        check_interval_seconds: float,
        cancel_timeout_seconds: float,
    ) -> None:
        """
        Args:
            state: The gate's runtime state (submissions, workflow ids, the
                managers that took each job)
            logger: Async logger
            task_runner: Runs re-run dispatches and cancels off the loop
            job_manager: The gate's jobs and their placement
            job_leadership_tracker: Which jobs this gate leads
            job_timeout_tracker: AD-34 tracking, moved to the replacement
            dispatch_coordinator: Dispatches a re-run to one datacenter
            datacenter_managers: Each datacenter's managers
            clock: The runtime clock
            send_tcp: Sends a message to a manager
            get_node_addr: This gate's TCP address (managers report to it)
            get_node_id_short: This gate's short id, for logs
            is_running: Whether the gate runs (the loop's condition)
            classify_datacenter_health: A datacenter's health, as routing
                classifies it
            route_replacement: The best eligible datacenter for a job,
                within its placement constraint, outside the occupied
                datacenters and under its dispatch latency budget
                (None when there is none)
            delivered_workflow_ids: The job's workflows a datacenter
                delivered a result for, or whose results were aggregated
            release_workflow_timeouts: Stops the job's per-workflow result
                timeouts for workflows whose result slots moved
            replicate_placement: Commits a job's placement (datacenters,
                substitutions, released datacenters) to a quorum of gates
            record_reassignment: Records a substitution in the job's
                durable record (AD-38), for a gate recovering the job
            check_interval_seconds: Seconds between checks
            cancel_timeout_seconds: Seconds a cancel waits on a manager
        """
        self._state = state
        self._logger = logger
        self._task_runner = task_runner
        self._job_manager = job_manager
        self._job_leadership_tracker = job_leadership_tracker
        self._job_timeout_tracker = job_timeout_tracker
        self._dispatch_coordinator = dispatch_coordinator
        self._datacenter_managers = datacenter_managers
        self._clock = clock
        self._send_tcp = send_tcp
        self._get_node_addr = get_node_addr
        self._get_node_id_short = get_node_id_short
        self._is_running = is_running
        self._classify_datacenter_health = classify_datacenter_health
        self._route_replacement = route_replacement
        self._delivered_workflow_ids = delivered_workflow_ids
        self._release_workflow_timeouts = release_workflow_timeouts
        self._replicate_placement = replicate_placement
        self._record_reassignment = record_reassignment
        self._check_interval_seconds = check_interval_seconds
        self._cancel_timeout_seconds = cancel_timeout_seconds
        # One failover decision per job at a time: each is built on the
        # placement the one before it committed.
        self._failover_locks: dict[str, asyncio.Lock] = {}
        self._accepted_reruns: dict[str, set[str]] = {}
        self._confirmed_releases: dict[str, set[str]] = {}
        # Lost datacenters of a job no datacenter could take over yet,
        # reported once until one can.
        self._unplaceable_reported: dict[str, set[str]] = {}
        self._reruns_in_flight: set[tuple[str, str]] = set()
        self._release_cancels_in_flight: set[tuple[str, str]] = set()

    async def run(self) -> None:
        """The failover loop, for as long as the gate runs."""
        while self._is_running():
            await self._clock.sleep(self._check_interval_seconds)
            await self.check_jobs()

    async def check_jobs(self) -> None:
        """One pass over the jobs this gate leads."""
        for job_id in self._job_manager.get_all_job_ids():
            await self._check_led_job(job_id)

    async def _check_led_job(self, job_id: str) -> None:
        """Check one job, if this gate leads it; a failed check is logged
        and the job checked again next pass."""
        if not self._job_leadership_tracker.is_leader(job_id):
            return
        try:
            await self._check_job(job_id)
        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=(
                        f"Failover check of job {job_id[:8]}... failed: "
                        f"{type(error).__name__}: {error}"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id_short(),
                )
            )

    def forget_job(self, job_id: str) -> None:
        """Drop the job's failover state (job cleanup)."""
        self._failover_locks.pop(job_id, None)
        self._accepted_reruns.pop(job_id, None)
        self._confirmed_releases.pop(job_id, None)
        self._unplaceable_reported.pop(job_id, None)

    async def _check_job(self, job_id: str) -> None:
        # A datacenter the job moved off is told to stop even once the
        # job ended: it may run the job still.
        self._start_release_cancels(job_id)

        job = self._job_manager.get_job(job_id)
        submission = self._state._job_submissions.get(job_id)
        if not self._is_failover_eligible(job, submission):
            return

        target_dcs = self._job_manager.get_target_dcs(job_id)
        self._resume_pending_reruns(job_id, target_dcs, submission)
        await self._fail_over_unhealthy_datacenters(job_id, target_dcs, submission)

    @staticmethod
    def _is_live_job(job: GlobalJobStatus | None) -> bool:
        """Whether the gate holds the job and it is not terminal."""
        return job is not None and not JobStatusOrder().is_terminal(job.status)

    def _is_failover_eligible(self, job: GlobalJobStatus | None, submission: JobSubmission | None) -> bool:
        """Whether the job runs, its submission is held here, and it is not
        best-effort (AD-44)."""
        return self._is_live_job(job) and submission is not None and not submission.best_effort

    def _resume_pending_reruns(self, job_id: str, target_dcs: set[str], submission: JobSubmission) -> None:
        """Re-send each re-run its replacement has not taken or answered."""
        accepted_reruns = self._accepted_reruns.get(job_id, set())
        for substitution in self._job_manager.get_datacenter_substitutions(job_id):
            if self._rerun_pending(job_id, substitution.replacement_datacenter, target_dcs, accepted_reruns):
                self._start_rerun(job_id, substitution.replacement_datacenter, submission)

    def _rerun_pending(
        self,
        job_id: str,
        replacement: str,
        target_dcs: set[str],
        accepted_reruns: set[str],
    ) -> bool:
        """Whether a replacement the job still runs in has neither taken its
        re-run nor answered for it."""
        return bool(replacement) and replacement in target_dcs and self._rerun_unanswered(
            job_id, replacement, accepted_reruns
        )

    def _rerun_unanswered(self, job_id: str, replacement: str, accepted_reruns: set[str]) -> bool:
        """Whether the replacement took no re-run and sent no result."""
        return replacement not in accepted_reruns and self._job_manager.get_dc_result(job_id, replacement) is None

    async def _fail_over_unhealthy_datacenters(
        self,
        job_id: str,
        target_dcs: set[str],
        submission: JobSubmission,
    ) -> None:
        """Move the job off each datacenter it runs in that routing
        classifies UNHEALTHY."""
        for datacenter in sorted(target_dcs):
            if self._classify_datacenter_health(datacenter) == DatacenterHealth.UNHEALTHY.value:
                await self.fail_over(job_id, datacenter, submission)

    @staticmethod
    def _delivered_along_chain(
        substitutions: list[DatacenterSubstitution],
        datacenter: str,
    ) -> set[str]:
        """The workflows the datacenters before ``datacenter`` along its
        chain of losses delivered: theirs is not its to finish."""
        substitution_by_replacement = GateJobFailoverCoordinator._substitutions_by_replacement(substitutions)
        delivered_along_chain: set[str] = set()
        chain_datacenter = datacenter
        for _ in range(len(substitution_by_replacement)):
            if (chain_substitution := substitution_by_replacement.get(chain_datacenter)) is None:
                break
            delivered_along_chain.update(chain_substitution.completed_workflow_ids)
            chain_datacenter = chain_substitution.lost_datacenter
        return delivered_along_chain

    @staticmethod
    def _substitutions_by_replacement(
        substitutions: list[DatacenterSubstitution],
    ) -> dict[str, DatacenterSubstitution]:
        """Each substitution by the datacenter that replaced the lost one."""
        return {
            substitution.replacement_datacenter: substitution
            for substitution in substitutions
            if substitution.replacement_datacenter
        }

    async def fail_over(
        self,
        job_id: str,
        lost_datacenter: str,
        submission: JobSubmission,
    ) -> None:
        """Move the job off ``lost_datacenter``: commit the substitution to
        a quorum of gates, then dispatch the re-run."""
        async with self._failover_locks.setdefault(job_id, asyncio.Lock()):
            replacement = await self._fail_over_locked(job_id, lost_datacenter, submission)

        if replacement is None:
            return
        if replacement:
            self._start_rerun(job_id, replacement, submission)
        self._start_release_cancels(job_id)

    async def _fail_over_locked(
        self,
        job_id: str,
        lost_datacenter: str,
        submission: JobSubmission,
    ) -> str | None:
        """Under the job's failover lock: commit the job's move off the lost
        datacenter; returns the replacement ("" when nothing re-runs), or
        None when the job was not moved."""
        job = self._job_manager.get_job(job_id)
        target_dcs = self._job_manager.get_target_dcs(job_id)
        if not self._may_fail_over(job, lost_datacenter, target_dcs, submission):
            return None

        return await self._fail_over_unfinished_share(job_id, job, lost_datacenter, target_dcs, submission)

    def _may_fail_over(
        self,
        job: GlobalJobStatus | None,
        lost_datacenter: str,
        target_dcs: set[str],
        submission: JobSubmission,
    ) -> bool:
        """Whether the job still runs in the lost datacenter with budget left."""
        if job is None or lost_datacenter not in target_dcs:
            return False
        # Out of budget, the job is AD-34's to end.
        return not (submission.timeout_seconds - (self._clock.monotonic() - job.timestamp) <= 0.0)

    async def _fail_over_unfinished_share(
        self,
        job_id: str,
        job: GlobalJobStatus,
        lost_datacenter: str,
        target_dcs: set[str],
        submission: JobSubmission,
    ) -> str | None:
        """Find the lost datacenter's unfinished share and the datacenter
        to re-run it in, then commit the move."""
        if not (workflow_ids := self._state._job_workflow_ids.get(job_id)):
            await self._log_unplaceable(
                job_id,
                lost_datacenter,
                "its workflow ids are unknown here, so its unfinished share is too",
            )
            return None

        delivered_workflow_ids = await self._delivered_workflow_ids(job_id, lost_datacenter)
        released_datacenters = self._job_manager.get_released_datacenters(job_id)
        substitutions = self._job_manager.get_datacenter_substitutions(job_id)
        # A lost replacement's share is what the datacenters before it
        # along its chain of losses left: theirs is not its to finish.
        delivered_along_chain = self._delivered_along_chain(substitutions, lost_datacenter)
        unfinished_workflow_ids = (
            workflow_ids - delivered_workflow_ids - delivered_along_chain
        )

        replacement = await self._route_unfinished_share(
            job_id,
            lost_datacenter,
            submission,
            unfinished_workflow_ids,
            target_dcs,
            released_datacenters,
            substitutions,
        )
        if replacement is None:
            return None

        return await self._commit_failover(
            job_id,
            job,
            lost_datacenter,
            replacement,
            workflow_ids=workflow_ids,
            delivered_workflow_ids=delivered_workflow_ids,
            unfinished_workflow_ids=unfinished_workflow_ids,
            target_dcs=target_dcs,
            released_datacenters=released_datacenters,
            substitutions=substitutions,
        )

    async def _route_unfinished_share(
        self,
        job_id: str,
        lost_datacenter: str,
        submission: JobSubmission,
        unfinished_workflow_ids: set[str],
        target_dcs: set[str],
        released_datacenters: set[str],
        substitutions: list[DatacenterSubstitution],
    ) -> str | None:
        """The datacenter to re-run the unfinished share in ("" when there
        is none to re-run); None, reported, when none can take it."""
        if not unfinished_workflow_ids:
            return ""

        replacement = self._route_replacement(
            job_id,
            self._placement_constraint(submission),
            self._occupied_datacenters(target_dcs, released_datacenters, substitutions, lost_datacenter),
            submission.dispatch_latency_budget_ms,
        )
        if replacement is None:
            await self._log_unplaceable(
                job_id,
                lost_datacenter,
                "no eligible datacenter can take its unfinished share; "
                "it is asked again every check",
            )
        return replacement

    @staticmethod
    def _occupied_datacenters(
        target_dcs: set[str],
        released_datacenters: set[str],
        substitutions: list[DatacenterSubstitution],
        lost_datacenter: str,
    ) -> frozenset[str]:
        """The datacenters a replacement may not be: those the job runs in,
        was released from, or lost."""
        return frozenset(
            target_dcs
            | released_datacenters
            | {substitution.lost_datacenter for substitution in substitutions}
            | {lost_datacenter}
        )

    @staticmethod
    def _placement_constraint(submission: JobSubmission) -> set[str] | None:
        """The datacenters the job may run in, or None when unconstrained."""
        return set(submission.datacenters) or None

    async def _commit_failover(
        self,
        job_id: str,
        job: GlobalJobStatus,
        lost_datacenter: str,
        replacement: str,
        *,
        workflow_ids: set[str],
        delivered_workflow_ids: set[str],
        unfinished_workflow_ids: set[str],
        target_dcs: set[str],
        released_datacenters: set[str],
        substitutions: list[DatacenterSubstitution],
    ) -> str | None:
        """Commit the substitution to a quorum of gates, then record it;
        None, reported, when no quorum committed it."""
        substitution = self._build_substitution(
            lost_datacenter,
            replacement,
            delivered_workflow_ids & workflow_ids,
            self._datacenter_progress(job, lost_datacenter),
        )
        if not await self._replicate_placement(
            job_id,
            sorted((target_dcs - {lost_datacenter}) | ({replacement} - {""})),
            [*substitutions, substitution],
            sorted(released_datacenters | {lost_datacenter}),
        ):
            await self._log_unplaceable(
                job_id,
                lost_datacenter,
                "its new placement reached no quorum of gates; it is tried again every check",
            )
            return None

        await self._record_failover(
            job_id, lost_datacenter, replacement, substitution, unfinished_workflow_ids, workflow_ids
        )
        return replacement

    @staticmethod
    def _datacenter_progress(job: GlobalJobStatus, datacenter: str) -> JobProgress | None:
        """The last progress the datacenter reported for the job."""
        return next(
            (
                progress
                for progress in job.datacenters
                if progress.datacenter == datacenter
            ),
            None,
        )

    @staticmethod
    def _build_substitution(
        lost_datacenter: str,
        replacement: str,
        completed_workflow_ids: set[str],
        lost_progress: JobProgress | None,
    ) -> DatacenterSubstitution:
        """The substitution of the replacement for the lost datacenter, with
        the work the lost one counted as it stood (AD-36)."""
        return DatacenterSubstitution(
            lost_datacenter=lost_datacenter,
            replacement_datacenter=replacement,
            completed_workflow_ids=sorted(completed_workflow_ids),
            total_completed=lost_progress.total_completed if lost_progress else 0,
            total_failed=lost_progress.total_failed if lost_progress else 0,
        )

    async def _record_failover(
        self,
        job_id: str,
        lost_datacenter: str,
        replacement: str,
        substitution: DatacenterSubstitution,
        unfinished_workflow_ids: set[str],
        workflow_ids: set[str],
    ) -> None:
        """Record a committed failover: the job's durable record (AD-38),
        its timeout tracking (AD-34), and its results' timeouts."""
        self._unplaceable_reported.get(job_id, set()).discard(lost_datacenter)
        await self._record_reassignment(job_id, substitution)
        await self._job_timeout_tracker.replace_target_datacenter(
            job_id, lost_datacenter, replacement
        )
        # The re-run's results take as long as the re-run: the job's
        # budget bounds them, not a wait that began with the other
        # datacenters' results.
        await self._release_workflow_timeouts(job_id, unfinished_workflow_ids)
        await self._logger.log(
            ServerInfo(
                message=(
                    f"Job {job_id[:8]}... lost datacenter {lost_datacenter} mid-run: "
                    + (
                        f"its {len(unfinished_workflow_ids)} unfinished of "
                        f"{len(workflow_ids)} workflows re-run in {replacement}"
                        if replacement
                        else "it had delivered every workflow's result; nothing re-runs"
                    )
                ),
                node_host=self._get_node_addr()[0],
                node_port=self._get_node_addr()[1],
                node_id=self._get_node_id_short(),
            )
        )

    def _start_rerun(self, job_id: str, replacement: str, submission: JobSubmission) -> None:
        if (job_id, replacement) in self._reruns_in_flight:
            return
        self._reruns_in_flight.add((job_id, replacement))
        self._task_runner.run(self._dispatch_rerun, job_id, replacement, submission)

    async def _dispatch_rerun(
        self,
        job_id: str,
        replacement: str,
        submission: JobSubmission,
    ) -> None:
        """Send the replacement its share: the job's workflows no
        datacenter before it along its chain of losses delivered, with
        what is left of the job's budget."""
        try:
            await self._dispatch_rerun_with_budget(job_id, replacement, submission)
        finally:
            self._reruns_in_flight.discard((job_id, replacement))

    async def _dispatch_rerun_with_budget(
        self,
        job_id: str,
        replacement: str,
        submission: JobSubmission,
    ) -> None:
        """Send the re-run while the job runs and has budget left."""
        job = self._job_manager.get_job(job_id)
        if not self._is_live_job(job):
            return
        remaining_timeout = submission.timeout_seconds - (
            self._clock.monotonic() - job.timestamp
        )
        if remaining_timeout <= 0.0:
            return

        await self._dispatch_rerun_share(job_id, replacement, submission, remaining_timeout)

    async def _dispatch_rerun_share(
        self,
        job_id: str,
        replacement: str,
        submission: JobSubmission,
        remaining_timeout: float,
    ) -> None:
        """Send the replacement the workflows not delivered before it along
        its chain of losses, when any are left."""
        delivered_along_chain = self._delivered_along_chain(
            self._job_manager.get_datacenter_substitutions(job_id), replacement
        )
        if not (
            rerun_workflow_ids := self._state._job_workflow_ids.get(job_id, set())
            - delivered_along_chain
        ):
            return

        await self._send_rerun(job_id, replacement, submission, rerun_workflow_ids, remaining_timeout)

    async def _send_rerun(
        self,
        job_id: str,
        replacement: str,
        submission: JobSubmission,
        rerun_workflow_ids: set[str],
        remaining_timeout: float,
    ) -> None:
        """Dispatch the re-run; a replacement no manager of which took it is
        lost too, and the job moves off it."""
        if await self._dispatch_coordinator.dispatch_to_datacenter(
            job_id,
            replacement,
            dataclasses.replace(
                submission,
                rerun_workflow_ids=sorted(rerun_workflow_ids),
                timeout_seconds=remaining_timeout,
                origin_gate_addr=self._get_node_addr(),
            ),
        ):
            self._accepted_reruns.setdefault(job_id, set()).add(replacement)
            await self._logger.log(
                ServerInfo(
                    message=(
                        f"Datacenter {replacement} took the re-run of job {job_id[:8]}... "
                        f"({len(rerun_workflow_ids)} workflows, {remaining_timeout:.1f}s "
                        "of budget left)"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id_short(),
                )
            )
            return

        await self._logger.log(
            ServerWarning(
                message=(
                    f"No manager of datacenter {replacement} took the re-run of job "
                    f"{job_id[:8]}...: the job is moved off it too"
                ),
                node_host=self._get_node_addr()[0],
                node_port=self._get_node_addr()[1],
                node_id=self._get_node_id_short(),
            )
        )
        if self._job_leadership_tracker.is_leader(job_id):
            await self.fail_over(job_id, replacement, submission)

    def _start_release_cancels(self, job_id: str) -> None:
        confirmed = self._confirmed_releases.get(job_id, set())
        for datacenter in self._job_manager.get_released_datacenters(job_id):
            if self._release_cancel_needed(job_id, datacenter, confirmed):
                self._release_cancels_in_flight.add((job_id, datacenter))
                self._task_runner.run(self._cancel_released_datacenter, job_id, datacenter)

    def _release_cancel_needed(self, job_id: str, datacenter: str, confirmed: set[str]) -> bool:
        """Whether a released datacenter is neither confirmed stopped nor
        being told to stop."""
        return datacenter not in confirmed and (job_id, datacenter) not in self._release_cancels_in_flight

    async def _cancel_released_datacenter(self, job_id: str, datacenter: str) -> None:
        """Tell a datacenter the job moved off to stop running it: the
        manager that took the job first, then the datacenter's others,
        following a redirect to the job's leader there. Confirmed once a
        manager answers the job stopped or ended, or every manager of the
        datacenter answers it holds no record of the job."""
        try:
            await self._tell_datacenter_to_stop(job_id, datacenter)
        except Exception as error:
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Telling datacenter {datacenter} to stop job {job_id[:8]}... "
                        f"failed: {type(error).__name__}: {error}; it is told again "
                        "every check"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id_short(),
                )
            )
        finally:
            self._release_cancels_in_flight.discard((job_id, datacenter))

    async def _tell_datacenter_to_stop(self, job_id: str, datacenter: str) -> None:
        """Ask the datacenter's managers in turn to stop the job, confirming
        the release once one does or all hold no record of it."""
        cancel_payload = CancelJob(
            job_id=job_id,
            reason=f"the job moved off datacenter {datacenter}",
            fence_token=0,
        ).dump()
        configured_managers = self._datacenter_managers.get(datacenter, [])
        manager_queue = self._release_manager_queue(job_id, datacenter, configured_managers)
        managers_without_job: set[tuple[str, int]] = set()
        if await self._ask_managers_to_stop(
            job_id, datacenter, cancel_payload, manager_queue, managers_without_job
        ):
            return

        if self._every_manager_lacks_job(configured_managers, managers_without_job):
            self._confirmed_releases.setdefault(job_id, set()).add(datacenter)

    def _release_manager_queue(
        self,
        job_id: str,
        datacenter: str,
        configured_managers: list[tuple[str, int]],
    ) -> list[tuple[str, int]]:
        """The managers to ask: the one that took the job first, then the
        datacenter's configured ones."""
        known_manager = self._state.get_job_dc_managers(job_id).get(datacenter)
        return (
            [known_manager, *configured_managers] if known_manager else list(configured_managers)
        )

    async def _ask_managers_to_stop(
        self,
        job_id: str,
        datacenter: str,
        cancel_payload: bytes,
        manager_queue: list[tuple[str, int]],
        managers_without_job: set[tuple[str, int]],
    ) -> bool:
        """Ask each queued manager once, following redirects; True once one
        confirms the job stopped or ended."""
        asked_managers: set[tuple[str, int]] = set()
        while manager_queue:
            if await self._ask_manager_to_stop(
                job_id,
                datacenter,
                cancel_payload,
                tuple(manager_queue.pop(0)),
                asked_managers,
                manager_queue,
                managers_without_job,
            ):
                return True
        return False

    async def _ask_manager_to_stop(
        self,
        job_id: str,
        datacenter: str,
        cancel_payload: bytes,
        manager_addr: tuple[str, int],
        asked_managers: set[tuple[str, int]],
        manager_queue: list[tuple[str, int]],
        managers_without_job: set[tuple[str, int]],
    ) -> bool:
        """Ask one manager not asked yet to stop the job; True once it
        confirms the job stopped or ended."""
        if manager_addr in asked_managers:
            return False
        asked_managers.add(manager_addr)
        response, _ = await self._send_tcp(
            manager_addr,
            "cancel_job",
            cancel_payload,
            self._cancel_timeout_seconds,
        )
        if (answer := self._cancel_answer(response)) is None:
            return False
        return self._settle_cancel_answer(
            job_id, datacenter, manager_addr, answer, manager_queue, managers_without_job
        )

    def _cancel_answer(self, response: bytes | Exception | None) -> JobCancelResponse | None:
        """A manager's cancel answer; None for a failed or empty one."""
        if isinstance(response, Exception) or not response:
            return None
        return self._load_cancel_answer(response)

    @staticmethod
    def _load_cancel_answer(response: bytes) -> JobCancelResponse | None:
        """Load a cancel answer; None when it is something else."""
        # Anything else -- a rate limit, say -- confirms nothing.
        answer = JobCancelResponse.load(response)
        return answer if isinstance(answer, JobCancelResponse) else None

    def _settle_cancel_answer(
        self,
        job_id: str,
        datacenter: str,
        manager_addr: tuple[str, int],
        answer: JobCancelResponse,
        manager_queue: list[tuple[str, int]],
        managers_without_job: set[tuple[str, int]],
    ) -> bool:
        """Confirm the release on a stopped or ended job; else note a manager
        without it, or queue the leader it redirects to first."""
        if self._answer_confirms_stop(answer):
            self._confirmed_releases.setdefault(job_id, set()).add(datacenter)
            return True
        self._follow_cancel_answer(manager_addr, answer, manager_queue, managers_without_job)
        return False

    @staticmethod
    def _answer_confirms_stop(answer: JobCancelResponse) -> bool:
        """Whether the manager stopped the job, or it had ended."""
        return answer.success or answer.already_cancelled or answer.already_completed

    @staticmethod
    def _follow_cancel_answer(
        manager_addr: tuple[str, int],
        answer: JobCancelResponse,
        manager_queue: list[tuple[str, int]],
        managers_without_job: set[tuple[str, int]],
    ) -> None:
        """Note a manager holding no record of the job, or queue the job's
        leader it redirects to first."""
        if answer.job_not_found:
            managers_without_job.add(manager_addr)
        elif answer.leader_addr is not None:
            manager_queue.insert(0, tuple(answer.leader_addr))

    @staticmethod
    def _every_manager_lacks_job(
        configured_managers: list[tuple[str, int]],
        managers_without_job: set[tuple[str, int]],
    ) -> bool:
        """Whether the datacenter has managers and each holds no record of
        the job."""
        return bool(configured_managers) and managers_without_job >= {
            tuple(manager_addr) for manager_addr in configured_managers
        }

    async def _log_unplaceable(self, job_id: str, lost_datacenter: str, reason: str) -> None:
        """Report, once until it changes, a lost datacenter of the job
        that could not be moved off yet."""
        reported = self._unplaceable_reported.setdefault(job_id, set())
        if lost_datacenter in reported:
            return
        reported.add(lost_datacenter)
        await self._logger.log(
            ServerWarning(
                message=(
                    f"Job {job_id[:8]}... lost datacenter {lost_datacenter} mid-run, "
                    f"but cannot move off it yet: {reason}"
                ),
                node_host=self._get_node_addr()[0],
                node_port=self._get_node_addr()[1],
                node_id=self._get_node_id_short(),
            )
        )


__all__ = ["GateJobFailoverCoordinator"]
