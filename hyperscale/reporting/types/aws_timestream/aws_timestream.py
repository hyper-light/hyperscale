import asyncio
import functools
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import psutil

from hyperscale.reporting.metric import MetricsSet

from .aws_timestream_config import AWSTimestreamConfig
from .aws_timestream_error_record import AWSTimestreamErrorRecord
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
        self.events_table_name = config.events_table
        self.streams_table_name = config.streams_table

        self.metrics_table_name = config.metrics_table
        self.stage_metrics_table_name = f"{config.metrics_table}_stage"
        self.errors_table_name = f"{config.metrics_table}_errors"

        self.experiments_table_name = config.experiments_table
        self.variants_table_name = f"{config.experiments_table}_variants"
        self.mutations_table_name = f"{config.experiments_table}_mutations"
        self.session_system_metrics_table_name = (
            f"{config.system_metrics_table}_session"
        )
        self.stage_system_metrics_table_name = f"{config.system_metrics_table}_stage"

        self.retention_options = config.retention_options
        self.session_uuid = str(uuid.uuid4())

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.client = None
        self._loop = asyncio.get_event_loop()
        self.metadata_string: str = None

    async def connect(self):
        self.client = await self._loop.run_in_executor(
            self._executor,
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
                self._executor,
                functools.partial(
                    self.client.create_database, DatabaseName=self.database_name
                ),
            )

        except Exception:
            pass

    async def submit_common(self, metrics: List[MetricsSet]):
        try:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=self.stage_metrics_table_name,
                    RetentionProperties=self.retention_options,
                ),
            )

        except Exception:
            pass

        records = []

        for metrics_set in metrics:
            for field, value in metrics_set.common_stats.items():
                timestream_record = AWSTimestreamRecord(
                    record_type="stage_metrics",
                    record_name=metrics_set.name,
                    record_stage=metrics_set.stage,
                    group_name="common",
                    field_name=field,
                    value=value,
                    session_uuid=self.session_uuid,
                )

                records.append(timestream_record.to_dict())

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.write_records,
                DatabaseName=self.database_name,
                TableName=self.stage_metrics_table_name,
                Records=records,
                CommonAttributes={},
            ),
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        try:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=self.metrics_table_name,
                    RetentionProperties=self.retention_options,
                ),
            )

        except Exception:
            pass

        records = []

        for metrics_set in metrics:
            for group_name, group in metrics_set.groups.items():
                metric_result = {**group.stats, **group.custom}

                for field, value in metric_result.items():
                    timestream_record = AWSTimestreamRecord(
                        record_type="metric",
                        record_name=metrics_set.name,
                        record_stage=metrics_set.stage,
                        group_name=group_name,
                        field_name=field,
                        value=value,
                        session_uuid=self.session_uuid,
                    )

                    records.append(timestream_record.to_dict())

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.write_records,
                DatabaseName=self.database_name,
                TableName=self.metrics_table_name,
                Records=records,
                CommonAttributes={},
            ),
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        try:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=self.metrics_table_name,
                    RetentionProperties=self.retention_options,
                ),
            )

        except Exception:
            pass

        records = []

        for metrics_set in metrics_sets:
            for custom_metric in metrics_set.custom_metrics.values():
                timestream_record = AWSTimestreamRecord(
                    record_type="metric",
                    record_name=metrics_set.name,
                    record_stage=metrics_set.stage,
                    group_name="custom",
                    field_name=custom_metric.metric_name,
                    value=custom_metric.metric_value,
                    session_uuid=self.session_uuid,
                )

                records.append(timestream_record.to_dict())

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.write_records,
                DatabaseName=self.database_name,
                TableName=self.metrics_table_name,
                Records=records,
                CommonAttributes={},
            ),
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        try:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=self.errors_table_name,
                    RetentionProperties=self.retention_options,
                ),
            )

        except Exception:
            pass

        error_records = []

        for metrics_set in metrics_sets:
            for error in metrics_set.errors:
                timestream_record = AWSTimestreamErrorRecord(
                    record_name=metrics_set.name,
                    record_stage=metrics_set.stage,
                    error_message=error.get("message"),
                    count=error.get("count"),
                    session_uuid=self.session_uuid,
                )

                error_records.append(timestream_record.to_dict())

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.write_records,
                DatabaseName=self.database_name,
                TableName=f"{self.metrics_table_name}_errors",
                Records=error_records,
                CommonAttributes={},
            ),
        )

    async def close(self):
        pass
