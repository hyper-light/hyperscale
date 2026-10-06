"""``JobFinalResultHandler`` -- pickled under the namespace
``hyperscale.distributed.nodes.client.handlers.tcp_job_result`` (see that module)."""

from hyperscale.distributed.models import JobFinalResult, WorkflowResultPush
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerWarning
from hyperscale.reporting.results import Results

from .tcp_workflow_result import WorkflowResultPushHandler


class JobFinalResultHandler:
    """
    Handle final job result from manager (when no gates).

    This is a per-datacenter result with all workflow results.
    Sent when job completes in a single-DC scenario.

    Workflow results travel separately and unordered with it, so any
    workflow whose result has not arrived (or never will) is recorded from
    this result exactly as a pushed one is; once applied, the job's
    results are complete.
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
        Process final job result.

        Args:
            addr: Source manager address
            data: Serialized JobFinalResult message
            clock_time: Logical clock time

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            result = JobFinalResult.load(data)
            job = self._state._jobs.get(result.job_id)
            if not job:
                return b"ok"  # Job not tracked, ignore

            # Each missing workflow is recorded as the client-ready push
            # for it would be: its per-core stats merged into one.
            for workflow_result in result.workflow_results:
                if workflow_result.workflow_id in job.workflow_results:
                    continue

                workflow_stats = workflow_result.results
                await self._workflow_results.apply(
                    WorkflowResultPush(
                        job_id=result.job_id,
                        workflow_id=workflow_result.workflow_id,
                        workflow_name=workflow_result.workflow_name,
                        datacenter=result.datacenter,
                        status=workflow_result.status,
                        fence_token=result.fence_token,
                        results=(
                            [Results().merge_results(workflow_stats)]
                            if len(workflow_stats) > 1
                            else list(workflow_stats)
                        ),
                        error=workflow_result.error,
                        is_client_ready=True,
                    )
                )

            # Update job with final result
            job.status = result.status
            job.total_completed = result.total_completed
            job.total_failed = result.total_failed
            job.elapsed_seconds = result.elapsed_seconds
            if result.errors:
                job.error = "; ".join(result.errors)

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
                    message=f"Final job result handling failed: {error}",
                    node_host="client",
                    node_port=0,
                    node_id="client",
                )
            )
            return b"error"
