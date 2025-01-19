import json
import uuid
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)
from typing import Any, Dict

from .kafka_config import KafkaConfig

try:
    from aiokafka import AIOKafkaProducer

    has_connector = True

except Exception:
    has_connector = False

    class AIOKafkaProducer:
        pass


class Kafka:
    def __init__(self, config: KafkaConfig) -> None:
        self.host = config.host
        self.port = config.port
        self.client_id = config.client_id

        self._workflow_results_topic_name = config.workflow_results_topic_name
        self._step_results_topic_name = config.step_results_topic_name
        self._workflow_results_partition = config.workflow_results_partition
        self._step_results_partition = config.step_results_partition

        self.compression_type = config.compression_type
        self.timeout = config.timeout
        self.enable_idempotence = config.idempotent or True
        self.options: Dict[str, Any] = config.options or {}
        self._producer = None

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.Kafka
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=f"{self.host}:{self.port}",
            client_id=self.client_id,
            compression_type=self.compression_type,
            request_timeout_ms=self.timeout,
            enable_idempotence=self.enable_idempotence,
            **self.options,
        )

        await self._producer.start()

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        batch = self._producer.create_batch()
        for result in workflow_results:
            metric_workflow = result.get("metric_workflow")
            metric_name = result.get("metric_name")

            result_key = f"{metric_workflow}_{metric_name}"

            batch.append(
                value=json.dumps(result).encode("utf-8"),
                timestamp=None,
                key=result_key.encode("utf-8"),
            )

        await self._producer.send_batch(
            batch,
            self._workflow_results_topic_name,
            partition=self._workflow_results_partition,
        )

    async def submit_step_results(self, step_results: StepMetricSet):
        batch = self._producer.create_batch()
        for result in step_results:
            metric_workflow = result.get("metric_workflow")
            metric_step = result.get("metric_step")
            metric_name = result.get("metric_name")

            result_key = f"{metric_workflow}_{metric_step}_{metric_name}"

            batch.append(
                value=json.dumps(result).encode("utf-8"),
                timestamp=None,
                key=result_key.encode("utf-8"),
            )

        await self._producer.send_batch(
            batch,
            self._workflow_results_topic_name,
            partition=self._workflow_results_partition,
        )

    async def close(self):
        await self._producer.stop()
