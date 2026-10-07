"""
Result reporting for HyperscaleClient.

Handles submission to local file-based reporters (JSON/CSV/XML).
"""

from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.models.client_config import ClientConfig
from hyperscale.distributed.runtime import Clock
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerWarning
from hyperscale.reporting.reporter import Reporter
from hyperscale.reporting.json import JSONConfig


class ClientReportingManager:
    """
    Manages submission to local file-based reporters.

    Reporting flow:
    1. Get reporter configs for job from state
    2. Filter to local file-based types (JSON/CSV/XML)
    3. If no local configs, create default per-workflow JSON
    4. For each config: create Reporter, connect and submit within one
       ``reporter_submission_timeout_seconds`` deadline on ``clock``, then
       close within another, so a hung reporter delays the job's results
       (and the reporters after it) by a bounded time, never forever
    5. Best-effort submission (don't raise on reporter failures)
    """

    def __init__(
        self,
        state: ClientState,
        config: ClientConfig,
        logger: Logger,
        clock: Clock,
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._clock = clock

    async def submit_to_local_reporters(
        self,
        job_id: str,
        workflow_name: str,
        workflow_stats: dict,
    ) -> None:
        """
        Submit workflow results to local file-based reporters.

        Uses configured reporters if provided, otherwise defaults to per-workflow
        JSON files with naming pattern: <workflow_name>_workflow_results.json

        Args:
            job_id: Job identifier
            workflow_name: Name of the workflow
            workflow_stats: Workflow statistics dictionary

        Note:
            This is best-effort submission - failures are logged but not raised
        """
        local_configs = self._get_local_reporter_configs(job_id)

        # If no file-based configs provided, use default per-workflow JSON
        if not local_configs:
            local_configs = self._create_default_reporter_configs(workflow_name)

        for config in local_configs:
            await self._submit_single_reporter(config, workflow_stats)

    async def _submit_single_reporter(self, config, workflow_stats: dict) -> None:
        """
        Submit results to a single local reporter.

        Creates Reporter instance, connects, submits workflow/step results,
        and closes connection.

        Args:
            config: Reporter configuration object (JSONConfig/CSVConfig/XMLConfig)
            workflow_stats: Workflow statistics dictionary

        Note:
            Failures are logged but not raised (best-effort submission)
        """
        reporter_type = getattr(config, "reporter_type", None)
        reporter_type_name = reporter_type.name if reporter_type else "unknown"

        deadline = self._clock.monotonic() + self._config.reporter_submission_timeout_seconds
        try:
            reporter = Reporter(config)
            try:
                # A connect that failed or timed out may have opened part of
                # its backend: it is closed all the same.
                await self._clock.wait_for(reporter.connect(), max(deadline - self._clock.monotonic(), 0.0))
                await self._clock.wait_for(
                    reporter.submit_workflow_results(workflow_stats),
                    max(deadline - self._clock.monotonic(), 0.0),
                )
                await self._clock.wait_for(
                    reporter.submit_step_results(workflow_stats),
                    max(deadline - self._clock.monotonic(), 0.0),
                )
            finally:
                # A deadline of its own: one spent by a hung submission
                # would cancel the close before it began.
                await self._clock.wait_for(reporter.close(), self._config.reporter_submission_timeout_seconds)

        except Exception as reporter_error:
            workflow_name = workflow_stats.get("workflow_name", "unknown")
            await self._logger.log(
                ServerWarning(
                    message=f"Reporter submission failed: {type(reporter_error).__name__}: {reporter_error}, "
                    f"reporter_type={reporter_type_name}, "
                    f"workflow={workflow_name}",
                    node_host="client",
                    node_port=0,
                    node_id="client",
                )
            )

    def _get_local_reporter_configs(self, job_id: str) -> list:
        """
        Get local file-based reporter configs for a job.

        Filters job's reporter configs to only include local file types
        (JSON/CSV/XML) based on config.local_reporter_types.

        Args:
            job_id: Job identifier

        Returns:
            List of local reporter config objects
        """
        configs = self._state._job_reporting_configs.get(job_id, [])

        # Filter to only file-based reporters
        local_configs = [
            config
            for config in configs
            if getattr(config, "reporter_type", None) in self._config.local_reporter_types
        ]

        return local_configs

    def _create_default_reporter_configs(self, workflow_name: str) -> list:
        """
        Create default JSON reporter configs for a workflow.

        Generates per-workflow JSON file configs with naming pattern:
        - <workflow_name>_workflow_results.json
        - <workflow_name>_step_results.json

        Args:
            workflow_name: Name of the workflow

        Returns:
            List containing single JSONConfig instance
        """
        workflow_name_lower = workflow_name.lower()
        return [
            JSONConfig(
                workflow_results_filepath=f"{workflow_name_lower}_workflow_results.json",
                step_results_filepath=f"{workflow_name_lower}_step_results.json",
            )
        ]
