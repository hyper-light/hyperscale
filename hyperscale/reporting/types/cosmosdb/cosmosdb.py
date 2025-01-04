import uuid
from typing import Dict, List

from hyperscale.logging.hyperscale_logger import HyperscaleLogger
from hyperscale.reporting.metric import MetricsSet

from .cosmosdb_config import CosmosDBConfig

try:
    from azure.cosmos import PartitionKey
    from azure.cosmos.aio import CosmosClient

    has_connector = True
except Exception:
    has_connector = False

    class CosmosClient:
        pass

    class PartitionKey:
        pass


class CosmosDB:
    def __init__(self, config: CosmosDBConfig) -> None:
        self.account_uri = config.account_uri
        self.account_key = config.account_key

        self.database_name = config.database
        self.events_container_name = config.events_container
        self.metrics_container_name = config.metrics_container
        self.streams_container_name = config.streams_container

        self.experiments_container_name = config.experiments_container
        self.variants_container_name = f"{config.experiments_container}_variants"
        self.mutations_container_name = f"{config.experiments_container}_mutations"

        self.shared_metrics_container_name = f"{self.metrics_container_name}_metrics"
        self.custom_metrics_container_name = f"{self.metrics_container_name}_custom"
        self.errors_container_name = f"{self.metrics_container}_errors"

        self.session_system_metrics_container_name = (
            f"{config.system_metrics_container}_session"
        )
        self.stage_system_metrics_container_name = (
            f"{config.system_metrics_container}_stage"
        )

        self.events_partition_key = config.events_partition_key
        self.metrics_partition_key = config.metrics_partition_key
        self.streams_partition_key = config.streams_partition_key
        self.system_metrics_partition = config.system_metrics_partition

        self.experiments_partition_key = config.experiments_partition_key
        self.variants_partition_key = config.variants_partition_key
        self.mutations_partition_key = config.mutations_partition_key

        self.analytics_ttl = config.analytics_ttl

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.events_container = None
        self.metrics_container = None
        self.streams_container = None

        self.session_system_metrics_container = None
        self.stage_system_metrics_container = None

        self.experiments_container = None
        self.variants_container = None
        self.mutations_container = None

        self.shared_metrics_container = None
        self.custom_metrics_container = None
        self.errors_container = None

        self.client = None
        self.database = None

    async def connect(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connecting to CosmosDB"
        )

        self.client = CosmosClient(self.account_uri, credential=self.account_key)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connected to CosmosDB"
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Creating Database - {self.database_name} - if not exists"
        )
        self.database = await self.client.create_database_if_not_exists(
            self.database_name
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Created or set Database - {self.database_name}"
        )
        
    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Creating Shared Metrics container - {self.shared_metrics_container_name} with Partition Key /{self.metrics_partition_key} if not exists"
        )
        self.shared_metrics_container = (
            await self.database.create_container_if_not_exists(
                self.shared_metrics_container_name,
                PartitionKey(f"/{self.metrics_partition_key}"),
            )
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Created or set Shared Metrics container - {self.shared_metrics_container_name} with Partition Key /{self.metrics_partition_key}"
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Shared Metrics to container - {self.shared_metrics_container_name} with Partition Key /{self.metrics_partition_key}"
        )
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            await self.shared_metrics_container.upsert_item(
                {
                    "id": str(uuid.uuid4()),
                    "name": metrics_set.name,
                    "stage": metrics_set.stage,
                    "group": "common",
                    **metrics_set.common_stats,
                }
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Shared Metrics to container - {self.shared_metrics_container_name} with Partition Key /{self.metrics_partition_key}"
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Creating Metrics container - {self.metrics_container_name} with Partition Key /{self.metrics_partition_key} if not exists"
        )
        self.metrics_container = await self.database.create_container_if_not_exists(
            self.metrics_container_name, PartitionKey(f"/{self.metrics_partition_key}")
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Created or set Metrics container - {self.metrics_container_name} with Partition Key /{self.metrics_partition_key}"
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Metrics to container - {self.metrics_container_name} with Partition Key /{self.metrics_partition_key}"
        )
        for metrics_set in metrics:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for group_name, group in metrics_set.groups.items():
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Metrics Group - {group_name}:{group.metrics_group_id}"
                )

                await self.metrics_container.upsert_item(
                    {"id": str(uuid.uuid4()), "group": group_name, **group.record}
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Metrics to container - {self.metrics_container_name} with Partition Key /{self.metrics_partition_key}"
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Custom Metrics Group - Xuarom"
            )

            custom_metrics_container_name = (
                f"{self.custom_metrics_container_name}_metrics"
            )
            await self.logger.filesystem.aio["hyperscale.reporting"].info(
                f"{self.metadata_string} - Creating Custom Metrics container - {custom_metrics_container_name} with Partition Key /{self.metrics_partition_key} if not exists"
            )

            custom_container = await self.database.create_container_if_not_exists(
                custom_metrics_container_name,
                PartitionKey(f"/{self.metrics_partition_key}"),
            )

            self.custom_metrics_container = custom_container

            await self.logger.filesystem.aio["hyperscale.reporting"].info(
                f"{self.metadata_string} - Created or set Custom Metrics container - {custom_metrics_container_name} with Partition Key /{self.metrics_partition_key}"
            )

            await custom_container.upsert_item(
                {
                    "id": str(uuid.uuid4()),
                    "name": metrics_set.name,
                    "stage": metrics_set.stage,
                    "group": "custom",
                    **{
                        custom_metric_name: custom_metric.metric_value
                        for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                    },
                }
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Metrics to container - {self.metrics_container_name} with Partition Key /{self.metrics_partition_key}"
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Creating Error Metrics container - {self.errors_container_name} with Partition Key /{self.metrics_partition_key} if not exists"
        )
        self.errors_container = await self.database.create_container_if_not_exists(
            self.errors_container_name, PartitionKey(f"/{self.metrics_partition_key}")
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Created or set Error Metrics container - {self.errors_container_name} with Partition Key /{self.metrics_partition_key}"
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Error Metrics to container - {self.errors_container_name} with Partition Key /{self.metrics_partition_key}"
        )
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for error in metrics_set.errors:
                await self.metrics_container.upsert_item(
                    {
                        "id": str(uuid.uuid4()),
                        "name": metrics_set.name,
                        "stage": metrics_set.stage,
                        "error_message": error.get("message"),
                        "error_count": error.get("count"),
                    }
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Error Metrics to container - {self.errors_container_name} with Partition Key /{self.metrics_partition_key}"
        )

    async def close(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Closing session - {self.session_uuid}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closing connection to CosmosDB"
        )

        await self.client.close()

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closed connection to CosmosDB"
        )
