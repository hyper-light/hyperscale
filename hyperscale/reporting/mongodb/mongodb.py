import uuid
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

from .mongodb_config import MongoDBConfig

try:
    from motor.motor_asyncio import AsyncIOMotorClient

    has_connector = True

except Exception:
    has_connector = False

    class AsyncIOMotorClient:
        pass


class MongoDB:
    def __init__(self, config: MongoDBConfig) -> None:
        self.host = config.host
        self.port = config.port
        self.username = config.username
        self.password = config.password
        self.database_name = config.database

        self._workflow_results_collection_name = config.workflow_results_collection_name
        self._step_results_collection_name = config.step_results_collection_name

        self.connection: AsyncIOMotorClient = None
        self.database = None

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.MongoDB
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        if self.username and self.password:
            connection_string = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}"

        else:
            connection_string = (
                f"mongodb://{self.host}:{self.port}/{self.database_name}"
            )

        self.connection = AsyncIOMotorClient(connection_string)
        self.database = self.connection[self.database_name]

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        await self.database[self._workflow_results_collection_name].insert_many(
            workflow_results
        )

    async def submit_step_results(self, step_results: StepMetricSet):
        await self.database[self._step_results_collection_name].insert_many(
            step_results
        )

    async def close(self):
        await self.connection.close()
