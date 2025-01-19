import asyncio
import uuid
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

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
        self._workflow_results_container_name = config.workflow_results_container_name
        self._step_results_container_name = config.step_results_container_name

        self._workflow_results_partition_key = config.workflow_results_partition_key
        self._step_results_partition_key = config.step_results_partition_key

        self.analytics_ttl = config.analytics_ttl

        self.session_uuid = str(uuid.uuid4())

        self._workflow_results_container = None
        self._step_results_container = None

        self.client = None
        self.database = None

        self.reporter_type = ReporterTypes.CosmosDB
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        self.client = CosmosClient(self.account_uri, credential=self.account_key)

        self.database = await self.client.create_database_if_not_exists(
            self.database_name
        )

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        self._workflow_results_container = (
            await self.database.create_container_if_not_exists(
                self._workflow_results_container_name,
                PartitionKey(f"/{self._workflow_results_partition_key}"),
            )
        )

        await asyncio.gather(
            *[
                self._workflow_results_container.upsert_item(
                    {"id": str(uuid.uuid4()), **result}
                )
                for result in workflow_results
            ]
        )

    async def submit_step_results(self, step_results: StepMetricSet):
        self._step_results_container = (
            await self.database.create_container_if_not_exists(
                self._step_results_container_name,
                PartitionKey(f"/{self._step_results_partition_key}"),
            )
        )

        await asyncio.gather(
            *[
                self._step_results_container.upsert_item(
                    {"id": str(uuid.uuid4()), **result}
                )
                for result in step_results
            ]
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
