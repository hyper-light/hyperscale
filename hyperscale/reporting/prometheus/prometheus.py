import asyncio
import functools
import uuid
from typing import Dict, Literal
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)
from hyperscale.reporting.common.results_types import MetricType

from .prometheus_config import PrometheusConfig

try:
    from prometheus_client import (
        CollectorRegistry,
        push_to_gateway,
    )
    from prometheus_client.core import REGISTRY
    from prometheus_client.exposition import basic_auth_handler

    from .prometheus_metric import PrometheusMetric

    has_connector = True

except Exception:
    has_connector = False

    class REGISTRY:
        pass

    class CollectorRegistry:
        pass

    def basic_auth_handler(*args, **kwargs):
        pass

    def push_to_gateway(*args, **kwargs):
        pass


PrometheusMetricType = Literal[
    "count",
    "gauge",
    "histogram",
    "info",
    "enum",
    "summary",
]


class Prometheus:
    def __init__(self, config: PrometheusConfig) -> None:
        self.pushgateway_host = config.pushgateway_host
        self.pushgateway_port = config.pushgateway_port
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

        self.types_map: Dict[
            MetricType,
            PrometheusMetricType,
        ] = {
            "COUNT": "count",
            "DISTRIBUTION": "gauge",
            "SAMPLE": "gauge",
            "RATE": "gauge",
            "TIMING": "summary",
        }

        self._workflow_results_metrics: Dict[str, PrometheusMetric] = {}
        self._step_results_metrics: Dict[str, PrometheusMetric] = {}

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.Prometheus
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self) -> None:
        self.registry = CollectorRegistry()
        REGISTRY.register(self.registry)

        if self.username and self.password:
            self._has_auth = True

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

        else:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    push_to_gateway,
                    self.pushgateway_address,
                    job=self.job_name,
                    registry=self.registry,
                ),
            )

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        for result in workflow_results:
            metric_workflow = result.get("metric_workflow")
            metric_name = result.get("metric_name")

            prometheus_metric_name = f"{metric_workflow}_{metric_name}"

            workflow_metric = self._workflow_results_metrics.get(prometheus_metric_name)
            if workflow_metric is None:
                metric_type: MetricType = self.types_map.get(result.get("metric_type"))

                metric_group = result.get("metric_group")

                metric_description = f"{metric_name} {metric_type.lower()} for Workflow {metric_workflow}"

                workflow_metric = prometheus_metric = PrometheusMetric(
                    metric_name,
                    metric_type,
                    metric_description=metric_description,
                    metric_labels=[
                        f"metric_name:{metric_name}",
                        f"metric_type:{metric_type}",
                        f"metric_workflow:{metric_workflow}",
                        f"metric_group:{metric_group}",
                    ],
                    metric_namespace=self.namespace,
                    registry=self.registry,
                )

                prometheus_metric.create_metric()

            metric_value = result.get("metric_value")
            workflow_metric.update(metric_value)
            self._workflow_results_metrics[prometheus_metric_name] = workflow_metric

        await self._submit_to_pushgateway()

    async def submit_step_results(self, step_results: StepMetricSet):
        for result in step_results:
            metric_workflow = result.get("metric_workflow")
            metric_name = result.get("metric_name")
            metric_step = result.get("metric_step")

            prometheus_metric_name = f"{metric_workflow}_{metric_step}_{metric_name}"

            workflow_metric = self._workflow_results_metrics.get(prometheus_metric_name)
            if workflow_metric is None:
                metric_type: MetricType = self.types_map.get(result.get("metric_type"))

                metric_group = result.get("metric_group")

                metric_description = f"{metric_name} {metric_type.lower()} for Workflow {metric_workflow} step {metric_step}"

                workflow_metric = prometheus_metric = PrometheusMetric(
                    metric_name,
                    metric_type,
                    metric_description=metric_description,
                    metric_labels=[
                        f"metric_step:{metric_step},metric_name:{metric_name}",
                        f"metric_type:{metric_type}",
                        f"metric_workflow:{metric_workflow}",
                        f"metric_group:{metric_group}",
                    ],
                    metric_namespace=self.namespace,
                    registry=self.registry,
                )

                prometheus_metric.create_metric()

            metric_value = result.get("metric_value")
            workflow_metric.update(metric_value)
            self._workflow_results_metrics[prometheus_metric_name] = workflow_metric

        await self._submit_to_pushgateway()

    async def close(self):
        pass
