"""``GlobalJobResultHandler`` -- pickled under the namespace
``hyperscale.distributed.nodes.client.handlers.tcp_job_result`` (see that module)."""

from hyperscale.distributed.models import (
    DatacenterSubstitution,
    GlobalJobResult,
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
                chain: list[DatacenterSubstitution] = []
                chain_datacenter = datacenter_result.datacenter
                for _ in range(len(substitution_by_replacement)):
                    if (substitution := substitution_by_replacement.get(chain_datacenter)) is None:
                        break
                    chain.append(substitution)
                    chain_datacenter = substitution.lost_datacenter
                delivered_before = {
                    workflow_id
                    for substitution in chain
                    for workflow_id in substitution.completed_workflow_ids
                }
                rerun_of = chain[-1].lost_datacenter if chain else ""
                for workflow_result in datacenter_result.workflow_results:
                    if (
                        workflow_result.workflow_id not in job.workflow_results
                        and workflow_result.workflow_id not in delivered_before
                    ):
                        datacenter_results_by_workflow.setdefault(workflow_result.workflow_id, []).append(
                            (datacenter_result.datacenter, workflow_result, rerun_of)
                        )

            # Each is recorded as the gate aggregates a pushed one: every
            # datacenter's stats merged, failed if any datacenter failed
            # it, and each datacenter's own result.
            for workflow_id, datacenter_results in datacenter_results_by_workflow.items():
                workflow_stats = [
                    stats for _, workflow_result, _ in datacenter_results for stats in workflow_result.results
                ]
                failed_results = [
                    (datacenter, workflow_result)
                    for datacenter, workflow_result, _ in datacenter_results
                    if workflow_result.status.upper() != "COMPLETED"
                ]
                await self._workflow_results.apply(
                    WorkflowResultPush(
                        job_id=result.job_id,
                        workflow_id=workflow_id,
                        workflow_name=datacenter_results[0][1].workflow_name,
                        datacenter="aggregated",
                        status=JobStatus.FAILED.value if failed_results else JobStatus.COMPLETED.value,
                        results=(
                            [Results().merge_results(workflow_stats)]
                            if len(workflow_stats) > 1
                            else workflow_stats
                        ),
                        error="; ".join(
                            f"{datacenter}: {workflow_result.error}"
                            for datacenter, workflow_result in failed_results
                            if workflow_result.error
                        )
                        or None,
                        per_dc_results=[
                            WorkflowDCResult(
                                datacenter=datacenter,
                                status=workflow_result.status,
                                stats=(
                                    Results().merge_results(workflow_result.results)
                                    if len(workflow_result.results) > 1
                                    else next(iter(workflow_result.results), None)
                                ),
                                error=workflow_result.error,
                                rerun_of=rerun_of,
                            )
                            for datacenter, workflow_result, rerun_of in datacenter_results
                        ],
                        is_client_ready=True,
                    )
                )

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

            # Signal results complete and job completion
            results_event = self._state._job_results_events.get(result.job_id)
            if results_event:
                results_event.set()

            event = self._state._job_events.get(result.job_id)
            if event:
                event.set()

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
