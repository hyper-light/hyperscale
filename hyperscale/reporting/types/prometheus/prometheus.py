import asyncio
import functools
import uuid
from typing import Dict, List


from hyperscale.reporting.metric import MetricsSet, MetricType

try:
    from prometheus_client import (
        CollectorRegistry,
        push_to_gateway,
    )
    from prometheus_client.core import REGISTRY
    from prometheus_client.exposition import basic_auth_handler

    from .prometheus_config import PrometheusConfig
    from .prometheus_metric import PrometheusMetric

    has_connector = True

except Exception:
    has_connector = False

    class REGISTRY:
        pass

    class PrometheusConfig:
        pass
    

    class CollectorRegistry:
        pass

    def basic_auth_handler(*args, **kwargs):
        pass

    def push_to_gateway(*args, **kwargs):
        pass


class Prometheus:
    def __init__(self, config: PrometheusConfig) -> None:
        self.pushgateway_address = config.pushgateway_address
        self.auth_request_method = config.auth_request_method
        self.auth_request_timeout = config.auth_request_timeout
        self.auth_request_data = config.auth_request_data
        self.username = config.username
        self.password = config.password
        self.namespace = config.namespace
        self.job_name = config.job_name

        self.registry = None
        self._auth_handler = None
        self._has_auth = False
        self._loop = asyncio.get_event_loop()

        self.types_map = {
            "total": "count",
            "succeeded": "count",
            "failed": "count",
            "actions_per_second": "gauge",
            "median": "gauge",
            "mean": "gauge",
            "variance": "gauge",
            "stdev": "gauge",
            "minimum": "gauge",
            "maximum": "gauge",
        }

        self._events: Dict[str, PrometheusMetric] = {}
        self._metrics: Dict[str, Dict[str, Dict[str, PrometheusMetric]]] = {}
        self._streams: Dict[str, Dict[str, PrometheusMetric]] = {}
        self._session_system_metrics: Dict[str, Dict[str, PrometheusMetric]] = {}
        self._stage_system_metrics: Dict[str, Dict[str, PrometheusMetric]] = {}

        self._experiments: Dict[str, Dict[str, PrometheusMetric]] = {}
        self._variants: Dict[str, Dict[str, PrometheusMetric]] = {}
        self._mutations: Dict[str, Dict[str, PrometheusMetric]] = {}

        self._shared_metrics: Dict[str, Dict[str, PrometheusMetric]] = {}
        self._custom_metrics: Dict[str, PrometheusMetric] = {}
        self._errors: Dict[str, PrometheusMetric] = {}

        self.metric_types_map = {
            MetricType.COUNT: "count",
            MetricType.RATE: "gauge",
            MetricType.DISTRIBUTION: "histogram",
            MetricType.SAMPLE: "gauge",
        }

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

    async def connect(self) -> None:
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connecting to Prometheus Pushgateway at: {self.pushgateway_address}"
        )

        self.registry = CollectorRegistry()
        REGISTRY.register(self.registry)

        if self.username and self.password:
            self._has_auth = True

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connected to Prometheus Pushgateway at: {self.pushgateway_address}"
        )

    def _generate_auth(self) -> basic_auth_handler:
        return basic_auth_handler(
            self.pushgateway_address,
            self.auth_request_method,
            self.auth_request_timeout,
            {"Content-Type": "application/json"},
            self.auth_request_data,
            username=self.username,
            password=self.password,
        )

    async def _submit_to_pushgateway(self):
        if self._has_auth:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Pushing to secure Prometheus Pushgateway via HTTPS"
            )
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    push_to_gateway,
                    self.pushgateway_address,
                    job=self.job_name,
                    registry=self.registry,
                    handler=self._generate_auth,
                ),
            )

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Pushed to secure Prometheus Pushgateway via HTTPS"
            )

        else:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Pushing to Prometheus Pushgateway via HTTP"
            )
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    push_to_gateway,
                    self.pushgateway_address,
                    job=self.job_name,
                    registry=self.registry,
                ),
            )

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Pushed to Prometheus Pushgateway via HTTP"
            )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Shared Metrics to Prometheus - Namespace: {self.namespace}"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            tags = [f"{tag.name}:{tag.value}" for tag in metrics_set.tags]

            shared_metrics = self._shared_metrics.get(metrics_set.name)
            if shared_metrics is None:
                shared_metrics = {}

                for field in metrics_set.common_stats.keys():
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:common:{field}"
                    )

                    metric_name = f"{metrics_set.name}_{field}".replace(".", "_")
                    metric_type = self.types_map.get(field)

                    prometheus_metric = PrometheusMetric(
                        metric_name,
                        metric_type,
                        metric_description=f"{metrics_set.name} {field}",
                        metric_labels=[
                            *tags,
                            f"stage:{metrics_set.stage}",
                            "group:common",
                        ],
                        metric_namespace=self.namespace,
                        registry=self.registry,
                    )
                    prometheus_metric.create_metric()

                    shared_metrics[field] = prometheus_metric

            self._shared_metrics[metrics_set.name] = shared_metrics
            for field, value in metrics_set.common_stats.items():
                metric = shared_metrics.get(field)
                metric.update(value)

        await self._submit_to_pushgateway()
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Shared Metrics to Prometheus - Namespace: {self.namespace}"
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Metrics to Prometheus - Namespace: {self.namespace}"
        )

        for metrics_set in metrics:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            tags = [f"{tag.name}:{tag.value}" for tag in metrics_set.tags]

            stage_metrics = self._metrics.get(metrics_set.name)
            if stage_metrics is None:
                stage_metrics = {}

                for group_name, group in metrics_set.groups.items():
                    group_metrics = stage_metrics.get(group_name)

                    if group_metrics is None:
                        group_metrics = {}

                        for quantile_name in metrics_set.quantiles:
                            self.types_map[quantile_name] = "gauge"

                        fields = {}

                        for metric_field in metrics_set.stats_fields:
                            await self.logger.filesystem.aio[
                                "hyperscale.reporting"
                            ].debug(
                                f"{self.metadata_string} - Submitting Metric - {metrics_set.name}:{group_name}:{metric_field}"
                            )
                            metric_type = self.types_map.get(metric_field)
                            metric_name = f"{metrics_set.name}_{group_name}_{metric_field}".replace(
                                ".", "_"
                            )

                            prometheus_metric = PrometheusMetric(
                                metric_name,
                                metric_type,
                                metric_description=f"{metrics_set.name} {metric_field}",
                                metric_labels=[
                                    *tags,
                                    f"stage:{metrics_set.stage}",
                                    f"group:{group_name}",
                                ],
                                metric_namespace=self.namespace,
                                registry=self.registry,
                            )
                            prometheus_metric.create_metric()

                            fields[metric_field] = prometheus_metric

                        group_metrics[group_name] = fields

                stage_metrics[metrics_set.name] = group_metrics

            self._metrics[metrics_set.name] = stage_metrics
            for group_name, group in metrics_set.groups.items():
                group_metrics = stage_metrics.get(group_name)
                record = group.record

                for field in group_metrics[group_name]:
                    metric_value = record.get(field)
                    field_metric = group_metrics.get(field)
                    field_metric.update(metric_value)

        await self._submit_to_pushgateway()
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Metrics to Prometheus - Namespace: {self.namespace}"
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Custom Metrics to Prometheus - Namespace: {self.namespace}"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            custom_metrics = self._custom_metrics.get(metrics_set.name)
            if custom_metrics is None:
                custom_metrics = {}

                for (
                    custom_metric_name,
                    custom_metric,
                ) in metrics_set.custom_metrics.items():
                    # group_metrics = custom_metrics.get(custom_group_name)

                    metric_type = self.metric_types_map.get(
                        custom_metric.metric_type, "gauge"
                    )

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Submitting Metric - {metrics_set.name}:custom:{custom_metric_name}"
                    )

                    metric_name = f"{metrics_set.name}_{custom_metric_name}".replace(
                        ".", "_"
                    )

                    tags = [f"{tag.name}:{tag.value}" for tag in metrics_set.tags]

                    prometheus_metric = PrometheusMetric(
                        metric_name,
                        metric_type,
                        metric_description=f"{metrics_set.name} {custom_metric_name}",
                        metric_labels=[
                            *tags,
                            f"stage:{metrics_set.stage}",
                            "group:custom",
                            f"type:{custom_metric.metric_type}",
                        ],
                        metric_namespace=self.namespace,
                        registry=self.registry,
                    )
                    prometheus_metric.create_metric()

                    custom_metrics[custom_metric_name] = prometheus_metric

            self._custom_metrics[metrics_set.name] = custom_metrics
            for custom_metric_name, custom_metric in metrics_set.custom_metrics.items():
                custom_metric_prometheus_value: PrometheusMetric = custom_metrics.get(
                    custom_metric_name
                )
                custom_metric_prometheus_value.update(custom_metric.metric_value)

        await self._submit_to_pushgateway()
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Custom Metrics to Prometheus - Namespace: {self.namespace}"
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Error Metrics to Prometheus - Namespace: {self.namespace}"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            if self._errors.get(metrics_set.name) is None:
                errors_metric = PrometheusMetric(
                    f"{metrics_set.name}_errors",
                    "count",
                    metric_description=f"Errors for action - {metrics_set.name}.",
                    metric_labels=[],
                    metric_namespace=self.namespace,
                    registry=self.registry,
                )

                errors_metric.create_metric()
                self._errors[metrics_set.name] = errors_metric

            for error in metrics_set.errors:
                errors_metric.update(
                    error.get("count"), labels={"message": error.get("message")}
                )

        await self._submit_to_pushgateway()
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Error Metrics to Prometheus - Namespace: {self.namespace}"
        )

    async def close(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Closing session - {self.session_uuid}"
        )
