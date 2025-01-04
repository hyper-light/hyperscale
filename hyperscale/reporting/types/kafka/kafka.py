import json
import uuid
from typing import Any, Dict, List

from hyperscale.logging.hyperscale_logger import HyperscaleLogger
from hyperscale.reporting.metric import MetricsSet

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
        self.client_id = config.client_id

        self.events_topic = config.events_topic
        self.metrics_topic = config.metrics_topic
        self.streams_topic = config.streams_topic

        self.custom_metrics_topic = f"{config.metrics_topic}_custom"
        self.shared_metrics_topic = f"{config.metrics_topic}_shared"
        self.errors_topic = f"{config.metrics_topic}_errors"

        self.experiments_topic = config.experiments_topic
        self.variants_topic = f"{config.experiments_topic}_variants"
        self.mutations_topic = f"{config.experiments_topic}_metrics"

        self.session_system_metrics_topic = f"{config.system_metrics_topic}_session"
        self.stage_system_metrics_topic = f"{config.system_metrics_topic}_stage"

        self.events_partition = config.events_partition
        self.metrics_partition = config.metrics_partition
        self.experiments_partition = config.experiments_partition
        self.streams_partition = config.streams_partition

        self.system_metrics_partition = config.system_metrics_partition

        self.shared_metrics_partition = config.metrics_partition
        self.errors_partition = config.metrics_partition
        self.custom_metrics_partition = config.metrics_partition

        self.compression_type = config.compression_type
        self.timeout = config.timeout
        self.enable_idempotence = config.idempotent or True
        self.options: Dict[str, Any] = config.options or {}
        self._producer = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

    async def connect(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connecting to Kafka at - {self.host}"
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Using Kafka Options - Compression Type: {self.compression_type}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Using Kafka Options - Connection Timeout: {self.timeout}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Using Kafka Options - Idempotent: {self.enable_idempotence}"
        )

        for option_name, option in self.options.items():
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Using Kafka Options - {option_name.capitalize()}: {option}"
            )

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.host,
            client_id=self.client_id,
            compression_type=self.compression_type,
            request_timeout_ms=self.timeout,
            enable_idempotence=self.enable_idempotence,
            **self.options,
        )

        await self._producer.start()

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connected to Kafka at - {self.host}"
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Shared Metrics to Topic - {self.shared_metrics_topic} - Partition - {self.shared_metrics_partition}"
        )

        batch = self._producer.create_batch()
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            batch.append(
                value=json.dumps(
                    {
                        "name": metrics_set.name,
                        "stage": metrics_set.stage,
                        "group": "common",
                        **metrics_set.common_stats,
                    }
                ).encode("utf-8"),
                timestamp=None,
                key=bytes(metrics_set.name, "utf"),
            )

        await self._producer.send_batch(
            batch, self.shared_metrics_topic, partition=self.shared_metrics_partition
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Shared Metrics to Topic - {self.shared_metrics_topic} - Partition - {self.shared_metrics_partition}"
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Metrics to Topic - {self.metrics_topic} - Partition - {self.metrics_partition}"
        )

        batch = self._producer.create_batch()
        for metrics_set in metrics:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for group_name, group in metrics_set.groups.items():
                batch.append(
                    value=json.dumps(
                        {
                            **group.record,
                            "name": metrics_set.name,
                            "stage": metrics_set.stage,
                            "group": group_name,
                        }
                    ).encode("utf-8"),
                    timestamp=None,
                    key=bytes(f"{metrics_set.name}_{group_name}", "utf"),
                )

        await self._producer.send_batch(
            batch, self.metrics_topic, partition=self.metrics_partition
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Metrics to Topic - {self.metrics_topic} - Partition - {self.metrics_partition}"
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        batch = self._producer.create_batch()

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Customm Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for custom_metic_name, custom_metric in metrics_set.custom_metrics.items():
                batch.append(
                    value=json.dumps(
                        {
                            "name": metrics_set.name,
                            "stage": metrics_set.stage,
                            "group": "custom",
                            custom_metic_name: custom_metric.metric_value,
                        }
                    ).encode("utf-8"),
                    timestamp=None,
                    key=bytes(f"{metrics_set.name}_{custom_metic_name}", "utf"),
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Customm Metrics to Topic - {self.custom_metrics_topic} - Partition - {self.custom_metrics_partition}"
        )
        await self._producer.send_batch(
            batch, self.custom_metrics_topic, partition=self.custom_metrics_partition
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Customm Metrics to Topic - {self.custom_metrics_topic} - Partition - {self.custom_metrics_partition}"
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Error Metrics to Topic - {self.metrics_topic} - Partition - {self.metrics_partition}"
        )

        batch = self._producer.create_batch()
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for error in metrics_set.errors:
                batch.append(
                    value=json.dumps(
                        {
                            "metric_name": metrics_set.name,
                            "metric_stage": metrics_set.stage,
                            "error_message": error.get("message"),
                            "error_count": error.get("count"),
                        }
                    ).encode("utf-8"),
                    timestamp=None,
                    key=bytes(metrics_set.name, "utf"),
                )

        await self._producer.send_batch(
            batch, self.errors_topic, partition=self.errors_partition
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Error Metrics to Topic - {self.metrics_topic} - Partition - {self.metrics_partition}"
        )

    async def close(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Closing session - {self.session_uuid}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closing connection to Kafka at - {self.host}"
        )

        await self._producer.stop()

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closed connection to Kafka at - {self.host}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Session Closed - {self.session_uuid}"
        )
