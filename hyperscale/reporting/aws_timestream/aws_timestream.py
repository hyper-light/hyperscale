import asyncio
import functools
import uuid

from hyperscale.reporting.common import (
    ReporterTypes,
    StepMetricSet,
    WorkflowMetricSet,
)

from .aws_timestream_config import AWSTimestreamConfig
from .aws_timestream_record import AWSTimestreamRecord

try:
    import boto3

    has_connector = True
except Exception:
    has_connector = False


class AWSTimestream:
    def __init__(self, config: AWSTimestreamConfig) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name

        self.database_name = config.database_name
        self.workflow_results_table_name = config.workflow_results_table_name
        self.step_results_table_name = config.step_results_table_name

        self.retention_options = config.retention_options
        self.session_uuid = str(uuid.uuid4())

        self.client = None
        self._loop = asyncio.get_event_loop()

        self.reporter_type = ReporterTypes.AWSTimestream
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        self.client = await self._loop.run_in_executor(
            None,
            functools.partial(
                boto3.client,
                "timestream-write",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            ),
        )

        try:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.create_database, DatabaseName=self.database_name
                ),
            )

        except Exception:
            pass

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        try:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=self.workflow_results_table_name,
                    RetentionProperties=self.retention_options,
                ),
            )

        except Exception:
            pass

        records = []

        for result in workflow_results:
            timestream_record = AWSTimestreamRecord(
                metric_group=result.get("metric_group"),
                metric_name=result.get("metric_name"),
                metric_type=result.get("metric_type"),
                metric_value=result.get("metric_value"),
                session_uuid=self.session_uuid,
                metric_workflow=result.get("metric_workflow"),
            )

            records.append(timestream_record.to_dict())

        await self._loop.run_in_executor(
            None,
            functools.partial(
                self.client.write_records,
                DatabaseName=self.database_name,
                TableName=self.workflow_results_table_name,
                Records=records,
                CommonAttributes={},
            ),
        )

    async def submit_step_results(self, step_results: StepMetricSet):
        try:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=self.step_results_table_name,
                    RetentionProperties=self.retention_options,
                ),
            )

        except Exception:
            pass

        records = []

        for result in step_results:
            timestream_record = AWSTimestreamRecord(
                metric_group=result.get("metric_group"),
                metric_name=result.get("metric_name"),
                metric_type=result.get("metric_type"),
                metric_value=result.get("metric_value"),
                session_uuid=self.session_uuid,
                metric_step=result.get("metric_step"),
                metric_workflow=result.get("metric_workflow"),
            )

            records.append(timestream_record.to_dict())

        await self._loop.run_in_executor(
            None,
            functools.partial(
                self.client.write_records,
                DatabaseName=self.database_name,
                TableName=self.step_results_table_name,
                Records=records,
                CommonAttributes={},
            ),
        )

    async def close(self):
        pass
