import re
import uuid
from typing import Dict, List

from hyperscale.logging.hyperscale_logger import HyperscaleLogger
from hyperscale.reporting.experiment.experiments_collection import (
    ExperimentMetricsCollectionSet,
)
from hyperscale.reporting.metric import MetricsSet
from hyperscale.reporting.metric.stage_streams_set import StageStreamsSet
from hyperscale.reporting.processed_result.types.base_processed_result import (
    BaseProcessedResult,
)
from hyperscale.reporting.system.system_metrics_set import (
    SessionMetricsCollection,
    SystemMetricsCollection,
    SystemMetricsSet,
)

try:
    from aio_statsd import GraphiteClient

    from hyperscale.reporting.types.statsd import StatsD

    from .graphite_config import GraphiteConfig

    has_connector = True

except Exception:
    from hyperscale.reporting.types.empty import Empty as StatsD

    GraphiteClient = None
    GraphiteConfig = None
    has_connector = False


class Graphite(StatsD):
    def __init__(self, config: GraphiteConfig) -> None:
        super().__init__(config)

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.connection = GraphiteClient(host=self.host, port=self.port)

        self.statsd_type = "Graphite"

    async def submit_session_system_metrics(
        self, system_metrics_sets: List[SystemMetricsSet]
    ):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Session System Metrics to {self.statsd_type}"
        )

        metrics_sets: List[SessionMetricsCollection] = []
        for metrics_set in system_metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Preparing Session System Metrics Set - {metrics_set.system_metrics_set_id}"
            )

            for monitor_metrics in metrics_set.session_cpu_metrics.values():
                metrics_sets.append(monitor_metrics.record)

            for monitor_metrics in metrics_set.session_memory_metrics.values():
                metrics_sets.append(monitor_metrics.record)

        for metrics_set in metrics_sets:
            for metric_field, metric_value in metrics_set.record.items():
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Session System Metric Set - {metrics_set.name}:{metrics_set.group}:{metric_field}"
                )

                self.connection.send_graphite(
                    f"{metrics_set.group}_{metrics_set.name}_{metric_field}",
                    metric_value,
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Session System Metrics to {self.statsd_type}"
        )

    async def submit_stage_system_metrics(
        self, system_metrics_sets: List[SystemMetricsSet]
    ):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Stage System Metrics to {self.statsd_type}"
        )

        metrics_sets: List[SystemMetricsCollection] = []
        for metrics_set in system_metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Preparing Stage System Metrics Set - {metrics_set.system_metrics_set_id}"
            )

            cpu_metrics = metrics_set.cpu
            memory_metrics = metrics_set.memory

            for stage_name, stage_cpu_metrics in cpu_metrics.metrics.items():
                for monitor_metrics in stage_cpu_metrics.values():
                    metrics_sets.append(monitor_metrics.record)

                stage_memory_metrics = memory_metrics.metrics.get(stage_name)
                for monitor_metrics in stage_memory_metrics.values():
                    metrics_sets.append(monitor_metrics.record)

                stage_mb_per_vu_metrics = metrics_set.mb_per_vu.get(stage_name)

                if stage_mb_per_vu_metrics:
                    metrics_sets.append(stage_mb_per_vu_metrics.record)

        for metrics_set in metrics_sets:
            for metric_field, metric_value in metrics_set.record.items():
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Stage System Metric Set - {metrics_set.name}:{metrics_set.group}:{metric_field}"
                )

                self.connection.send_graphite(
                    f"{metrics_set.group}_{metrics_set.group}_{metrics_set.name}_{metric_field}",
                    metric_value,
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Stage System Metrics to {self.statsd_type}"
        )

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Streams to {self.statsd_type}"
        )

        for stage_name, stream in stream_metrics.items():
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Stream - {stage_name}:{stream.stream_set_id}"
            )

            for group_name, group in stream.grouped.items():
                for metric_field, metric_value in group.items():
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Submitting Stream Metric - {stage_name}:{group_name}:{metric_field}"
                    )

                    self.connection.send_graphite(
                        f"{stage_name}_stream_{group_name}_{metric_field}", metric_value
                    )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Streams to {self.statsd_type}"
        )

    async def submit_experiments(
        self, experiment_metrics: ExperimentMetricsCollectionSet
    ):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Experiments to {self.statsd_type}"
        )

        for experiment in experiment_metrics.experiment_summaries:
            experiment_id = uuid.uuid4()

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Experiment - {experiment.experiment_name}:{experiment_id}"
            )

            for field, value in experiment.stats:
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Experiment field - {experiment.experiment_name}:{field}"
                )

                self.connection.send_graphite(
                    f"{experiment.experiment_name}_{field}", value
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Experiments to {self.statsd_type}"
        )

    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Variants to {self.statsd_type}"
        )

        for variant in experiment_metrics.variant_summaries:
            variant_id = uuid.uuid4()

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Variant - {variant.variant_name}:{variant_id}"
            )

            for field, value in variant.stats:
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Variants field - {variant.variant_name}:{field}"
                )

                self.connection.send_graphite(f"{variant.variant_name}_{field}", value)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Variants to {self.statsd_type}"
        )

    async def submit_mutations(
        self, experiment_metrics: ExperimentMetricsCollectionSet
    ):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Mutations to {self.statsd_type}"
        )

        for mutation in experiment_metrics.mutation_summaries:
            mutation_id = uuid.uuid4()

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Mutation - {mutation.mutation_name}:{mutation_id}"
            )

            for field, value in mutation.stats:
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Mutatio field - {mutation.mutation_name}:{field}"
                )

                self.connection.send_graphite(
                    f"{mutation.mutation_name}_{field}", value
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Mutations to {self.statsd_type}"
        )

    async def submit_events(self, events: List[BaseProcessedResult]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Events to {self.statsd_type}"
        )

        for event in events:
            self.connection.send_graphite(f"{event.name}_time", event.time)

            if event.success:
                self.connection.send_graphite(f"{event.name}_success", 1)

            else:
                self.connection.send_graphite(f"{event.name}_failed", 1)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Events to {self.statsd_type}"
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Shared Metrics to {self.statsd_type}"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for field, value in metrics_set.common_stats.items():
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:common:{field}"
                )

                self.connection.send_graphite(
                    f"{metrics_set.name}_common_{field}", value
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Shared Metrics to {self.statsd_type}"
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Metrics to {self.statsd_type}"
        )

        for metrics_set in metrics:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for group_name, group in metrics_set.groups.items():
                for metric_field, metric_value in group.stats.items():
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Submitting Metric - {metrics_set.name}:{group_name}:{metric_field}"
                    )

                    self.connection.send_graphite(
                        f"{metrics_set.name}_{group_name}_{metric_field}", metric_value
                    )

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Submitted Metric - {metrics_set.name}:{group_name}:{metric_field}"
                    )

                for metric_field, metric_value in group.custom.items():
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Submitting Metric - {metrics_set.name}:{group_name}:{metric_field}"
                    )

                    self.connection.send_graphite(
                        f"{metrics_set.name}_{group_name}_{metric_field}", metric_value
                    )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Metrics to {self.statsd_type}"
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Custom Metrics to {self.statsd_type}"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for custom_metric_name, custom_metric in metrics_set.custom_metrics.items():
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Metric - {metrics_set.name}:custom:{custom_metric_name}"
                )

                self.connection.send_graphite(
                    f"{metrics_set.name}_custom_{custom_metric_name}",
                    custom_metric.metric_value,
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Custom Metrics to {self.statsd_type}"
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Error Metrics to {self.statsd_type}"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for error in metrics_set.errors:
                error_message = re.sub(
                    "[^0-9a-zA-Z]+", "_", error.get("message").lower()
                )

                self.connection.send_graphite(
                    f"{metrics_set.name}_error_{error_message}", error.get("count")
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Error Metrics to {self.statsd_type}"
        )
