import asyncio
import json
import uuid
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

from .redis_config import RedisConfig, RedisChannelType

try:
    import aioredis

    has_connector = True

except Exception:
    aioredis = object
    has_connector = True


class Redis:
    def __init__(self, config: RedisConfig) -> None:
        self.host = config.host
        self.port = config.port
        self.base = "rediss" if config.secure else "redis"
        self.username = config.username
        self.password = config.password
        self.database = config.database

        self._workflow_results_channel_name = config.workflow_results_channel_name
        self._step_results_channel_name = config.step_results_channel_name

        self.channel_type: RedisChannelType = config.channel_type
        self.connection = None

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.Redis
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        self.connection = await aioredis.from_url(
            f"{self.base}://{self.host}:{self.port}",
            username=self.username,
            password=self.password,
            db=self.database,
        )

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        if self.channel_type == "channel":
            await asyncio.gather(
                *[
                    self.connection.publish(
                        self._workflow_results_channel_name,
                        json.dumps(result),
                    )
                    for result in workflow_results
                ]
            )

        elif self.channel_type == "pipeline":
            await asyncio.gather(
                *[
                    self.connection.sadd(
                        self._workflow_results_channel_name,
                        json.dumps(result),
                    )
                    for result in workflow_results
                ]
            )

    async def submit_step_results(self, step_results: StepMetricSet):
        if self.channel_type == "channel":
            await asyncio.gather(
                *[
                    self.connection.publish(
                        self._step_results_channel_name,
                        json.dumps(result),
                    )
                    for result in step_results
                ]
            )

        elif self.channel_type == "pipeline":
            await asyncio.gather(
                *[
                    self.connection.sadd(
                        self._step_results_channel_name,
                        json.dumps(result),
                    )
                    for result in step_results
                ]
            )

    async def close(self):
        await self.connection.close()
