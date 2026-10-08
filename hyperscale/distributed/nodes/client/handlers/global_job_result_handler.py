"""``GlobalJobResultHandler`` -- pickled under the namespace
``hyperscale.distributed.nodes.client.handlers.tcp_job_result`` (see that module)."""

from hyperscale.distributed.models import (
    ClientJobResult,
    DatacenterSubstitution,
    GlobalJobResult,
    JobFinalResult,
    JobStatus,
    WorkflowDCResult,
    WorkflowResult,
    WorkflowResultPush,
)
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerWarning
from hyperscale.reporting.results import Results

from .tcp_workflow_result import WorkflowResultPushHandler


class GlobalJobResultHandler:
    """
    Handle global job result from gate.

    This is the aggregated result across all datacenters.
    Sent when multi-DC job completes.

    It carries every datacenter's workflow results, so a workflow whose
    aggregated push has not arrived (a gate that failed over, a
    partition) is recorded from it -- merged across datacenters, as the
    gate merges a pushed one -- and the job's results are complete.
    """

    def __init__(
        self,
        state: ClientState,
        logger: Logger,
        workflow_results: WorkflowResultPushHandler,
    ) -> None:
        self._state = state
        self._logger = logger
        self._workflow_results = workflow_results

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Process global job result.

        Args:
            addr: Source gate address
            data: Serialized GlobalJobResult message
            clock_time: Logical clock time

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            result = GlobalJobResult.load(data)
            job = self._state._jobs.get(result.job_id)
            if not job:
                return b"ok"  # Job not tracked, ignore

            await self._record_unrecorded_workflow_results(result, job)
            self._apply_global_result(job, result)

            return b"ok"

        except Exception as error:
            await self._logger.log(
                ServerWarning(
                    message=f"Global job result handling failed: {error}",
                    node_host="client",
                    node_port=0,
                    node_id="client",
                )
            )
            return b"error"

    async def _record_unrecorded_workflow_results(self, result: GlobalJobResult, job: ClientJobResult) -> None:
        """Record each workflow the job holds no result for from the global result's datacenter results."""
        datacenter_results_by_workflow = self._unrecorded_results_by_workflow(result, job)

        # Each is recorded as the gate aggregates a pushed one: every
        # datacenter's stats merged, failed if any datacenter failed
        # it, and each datacenter's own result.
        for workflow_id, datacenter_results in datacenter_results_by_workflow.items():
            await self._workflow_results.apply(
                self._aggregated_workflow_push(result.job_id, workflow_id, datacenter_results)
            )

    def _unrecorded_results_by_workflow(
        self,
        result: GlobalJobResult,
        job: ClientJobResult,
    ) -> dict[str, list[tuple[str, WorkflowResult, str]]]:
        """Group by workflow every datacenter's result for each workflow the job holds no result for."""
        # Every datacenter's result for each workflow the job holds no
        # result for. A datacenter that replaced one the job lost
        # mid-run (AD-36) re-ran that share -- its result is marked so
        # -- and re-ran the workflows the lost one had delivered only
        # for the context their dependents read: those are not its to
        # report.
        substitution_by_replacement = {
            substitution.replacement_datacenter: substitution
            for substitution in result.datacenter_substitutions
        }
        datacenter_results_by_workflow: dict[str, list[tuple[str, WorkflowResult, str]]] = {}
        for datacenter_result in result.per_datacenter_results:
            self._collect_datacenter_results(
                datacenter_result,
                substitution_by_replacement,
                job,
                datacenter_results_by_workflow,
            )
        return datacenter_results_by_workflow

    def _collect_datacenter_results(
        self,
        datacenter_result: JobFinalResult,
        substitution_by_replacement: dict[str, DatacenterSubstitution],
        job: ClientJobResult,
        datacenter_results_by_workflow: dict[str, list[tuple[str, WorkflowResult, str]]],
    ) -> None:
        """Add one datacenter's unrecorded workflow results, skipping those it re-ran only for context (AD-36)."""
        chain = self._substitution_chain(substitution_by_replacement, datacenter_result.datacenter)
        delivered_before = self._delivered_before(chain)
        rerun_of = self._rerun_of(chain)
        for workflow_result in datacenter_result.workflow_results:
            if self._is_unrecorded(workflow_result, job, delivered_before):
                datacenter_results_by_workflow.setdefault(workflow_result.workflow_id, []).append(
                    (datacenter_result.datacenter, workflow_result, rerun_of)
                )

    @staticmethod
    def _substitution_chain(
        substitution_by_replacement: dict[str, DatacenterSubstitution],
        datacenter: str,
    ) -> list[DatacenterSubstitution]:
        """The substitutions a datacenter's share descends through, newest first (AD-36)."""
        chain: list[DatacenterSubstitution] = []
        chain_datacenter = datacenter
        for _ in range(len(substitution_by_replacement)):
            if (substitution := substitution_by_replacement.get(chain_datacenter)) is None:
                break
            chain.append(substitution)
            chain_datacenter = substitution.lost_datacenter
        return chain

    @staticmethod
    def _delivered_before(chain: list[DatacenterSubstitution]) -> set[str]:
        """The workflows a lost datacenter in the chain had already delivered."""
        return {
            workflow_id
            for substitution in chain
            for workflow_id in substitution.completed_workflow_ids
        }

    @staticmethod
    def _rerun_of(chain: list[DatacenterSubstitution]) -> str:
        """The first lost datacenter whose share the chain re-ran, or "" for an original share."""
        return chain[-1].lost_datacenter if chain else ""

    @staticmethod
    def _is_unrecorded(
        workflow_result: WorkflowResult,
        job: ClientJobResult,
        delivered_before: set[str],
    ) -> bool:
        """Whether the workflow has no result on the job and was not delivered before a substitution."""
        return (
            workflow_result.workflow_id not in job.workflow_results
            and workflow_result.workflow_id not in delivered_before
        )

    def _aggregated_workflow_push(
        self,
        job_id: str,
        workflow_id: str,
        datacenter_results: list[tuple[str, WorkflowResult, str]],
    ) -> WorkflowResultPush:
        """The aggregated push for one workflow, merged across its datacenters as the gate merges one."""
        workflow_stats = self._merged_workflow_stats(datacenter_results)
        failed_results = self._failed_datacenter_results(datacenter_results)
        return WorkflowResultPush(
            job_id=job_id,
            workflow_id=workflow_id,
            workflow_name=datacenter_results[0][1].workflow_name,
            datacenter="aggregated",
            status=JobStatus.FAILED.value if failed_results else JobStatus.COMPLETED.value,
            results=self._merged_or_single_stats(workflow_stats),
            error="; ".join(self._failure_messages(failed_results)) or None,
            per_dc_results=self._per_datacenter_results(datacenter_results),
            is_client_ready=True,
        )

    @staticmethod
    def _merged_workflow_stats(datacenter_results: list[tuple[str, WorkflowResult, str]]) -> list:
        """Every datacenter's stats for the workflow, in datacenter order."""
        return [
            stats for _, workflow_result, _ in datacenter_results for stats in workflow_result.results
        ]

    @staticmethod
    def _failed_datacenter_results(
        datacenter_results: list[tuple[str, WorkflowResult, str]],
    ) -> list[tuple[str, WorkflowResult]]:
        """The datacenters whose result for the workflow did not complete."""
        return [
            (datacenter, workflow_result)
            for datacenter, workflow_result, _ in datacenter_results
            if workflow_result.status.upper() != "COMPLETED"
        ]

    @staticmethod
    def _merged_or_single_stats(workflow_stats: list) -> list:
        """The stats merged into one when there are several, else as they are."""
        return (
            [Results().merge_results(workflow_stats)]
            if len(workflow_stats) > 1
            else workflow_stats
        )

    @staticmethod
    def _failure_messages(failed_results: list[tuple[str, WorkflowResult]]) -> list[str]:
        """Each failed datacenter's error, prefixed with its datacenter."""
        return [
            f"{datacenter}: {workflow_result.error}"
            for datacenter, workflow_result in failed_results
            if workflow_result.error
        ]

    def _per_datacenter_results(
        self,
        datacenter_results: list[tuple[str, WorkflowResult, str]],
    ) -> list[WorkflowDCResult]:
        """Each datacenter's own result for the workflow."""
        return [
            WorkflowDCResult(
                datacenter=datacenter,
                status=workflow_result.status,
                stats=self._datacenter_stats(workflow_result),
                error=workflow_result.error,
                rerun_of=rerun_of,
            )
            for datacenter, workflow_result, rerun_of in datacenter_results
        ]

    @staticmethod
    def _datacenter_stats(workflow_result: WorkflowResult):
        """One datacenter's stats for the workflow, merged when it reported several."""
        return (
            Results().merge_results(workflow_result.results)
            if len(workflow_result.results) > 1
            else next(iter(workflow_result.results), None)
        )

    def _apply_global_result(self, job: ClientJobResult, result: GlobalJobResult) -> None:
        """Update the job with the aggregated result, then signal its results
        and completion. A provisional result older than the one held is
        skipped (AD-44 late-result ``update`` policy: pushes may cross)."""
        if self._is_stale_provisional(job, result):
            return
        # Update job with aggregated result
        job.status = result.status
        job.total_completed = result.total_completed
        job.total_failed = result.total_failed
        job.elapsed_seconds = result.elapsed_seconds
        if result.errors:
            job.error = "; ".join(result.errors)

        # Multi-DC specific fields
        job.per_datacenter_results = result.per_datacenter_results
        job.per_datacenter_statuses = result.per_datacenter_statuses
        job.aggregated = result.aggregated
        job.completion_reason = result.completion_reason
        job.unreported_datacenters = list(result.unreported_datacenters)
        job.is_final = result.is_final

        # Signal results complete and job completion
        self._signal_job_complete(result.job_id)

    def _is_stale_provisional(self, job: ClientJobResult, result: GlobalJobResult) -> bool:
        """A provisional result after the final one, or holding fewer
        datacenters' results than the one held."""
        if result.is_final:
            return False
        return self._holds_final_result(job) or len(result.per_datacenter_results) < len(job.per_datacenter_results)

    @staticmethod
    def _holds_final_result(job: ClientJobResult) -> bool:
        """The job holds a final global result (only global results carry per-datacenter results)."""
        return job.is_final and bool(job.per_datacenter_results)

    def _signal_job_complete(self, job_id: str) -> None:
        """Set the job's results event, then its completion event."""
        results_event = self._state._job_results_events.get(job_id)
        if results_event:
            results_event.set()

        event = self._state._job_events.get(job_id)
        if event:
            event.set()
