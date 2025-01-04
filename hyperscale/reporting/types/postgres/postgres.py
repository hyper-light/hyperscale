# # This is an ugly patch for: https://github.com/aio-libs/aiopg/issues/837
# import selectors  # isort:skip # noqa: F401

# selectors._PollLikeSelector.modify = (  # type: ignore
#     selectors._BaseSelectorImpl.modify  # type: ignore
# )

import uuid
from typing import Dict, List


from hyperscale.reporting.metric import MetricsSet, MetricType

from .postgres_config import PostgresConfig

try:
    import sqlalchemy
    from sqlalchemy.dialects.postgresql import UUID
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.ext.asyncio.engine import (
        AsyncConnection,
        AsyncEngine,
        AsyncTransaction,
    )
    from sqlalchemy.schema import CreateTable

    has_connector = True

except Exception:
    has_connector = False

    class UUID:
        pass

    class sqlalchemy:
        pass

    def create_enging(*args, **kwargs):
        pass


class Postgres:
    def __init__(self, config: PostgresConfig) -> None:
        self.host = config.host
        self.database = config.database
        self.username = config.username
        self.password = config.password

        self.events_table_name = config.events_table
        self.metrics_table_name = config.metrics_table
        self.streams_table_name = config.streams_table

        self.experiments_table_name = config.experiments_table
        self.variants_table_name = f"{config.experiments_table}_variants"
        self.mutations_table_name = f"{config.experiments_table}_mutations"

        self.shared_metrics_table_name = f"{config.metrics_table}_shared"
        self.errors_table_name = f"{config.metrics_table}_errors"
        self.custom_metrics_table_name = f"{config.metrics_table}_custom"

        self.session_system_metrics_table_name = (
            f"{config.system_metrics_table}_session"
        )
        self.stage_system_metrics_table_name = f"{config.system_metrics_table}_stage"

        self._engine = None
        self.metadata = sqlalchemy.MetaData()

        self._events_table = None
        self._metrics_table = None
        self._streams_table = None

        self._experiments_table = None
        self._variants_table = None
        self._mutations_table = None

        self._shared_metrics_table = None
        self._errors_table = None
        self._custom_metrics_table = None

        self._session_system_metrics_table = None
        self._stage_system_metrics_table = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()
        self.sql_type = "Postgresql"

        self.metric_types_map = {
            MetricType.COUNT: lambda field_name: sqlalchemy.Column(
                field_name, sqlalchemy.BIGINT
            ),
            MetricType.DISTRIBUTION: lambda field_name: sqlalchemy.Column(
                field_name, sqlalchemy.FLOAT
            ),
            MetricType.SAMPLE: lambda field_name: sqlalchemy.Column(
                field_name, sqlalchemy.FLOAT
            ),
            MetricType.RATE: lambda field_name: sqlalchemy.Column(
                field_name, sqlalchemy.FLOAT
            ),
        }

    async def connect(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connecting to {self.sql_type} instance at - {self.host} - Database: {self.database}"
        )

        connection_uri = "postgresql+asyncpg://"

        if self.username and self.password:
            connection_uri = f"{connection_uri}{self.username}:{self.password}@"

        self._engine: AsyncEngine = await create_async_engine(
            f"{connection_uri}{self.host}/{self.database}", echo=False
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connected to {self.sql_type} instance at - {self.host} - Database: {self.database}"
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name}"
        )

        async with self._engine.connect() as connection:
            connection: AsyncConnection = connection

            async with connection.begin() as transaction:
                transaction: AsyncTransaction = transaction

                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name} - Initiating transaction"
                )

                if self._shared_metrics_table is None:
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Creating Shared Metrics table - {self.shared_metrics_table_name} - if not exists"
                    )

                    stage_metrics_table = sqlalchemy.Table(
                        self.shared_metrics_table_name,
                        self.metadata,
                        sqlalchemy.Column(
                            "id",
                            UUID(as_uuid=True),
                            primary_key=True,
                            default=uuid.uuid4,
                        ),
                        sqlalchemy.Column("name", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("stage", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("group", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("total", sqlalchemy.BIGINT),
                        sqlalchemy.Column("succeeded", sqlalchemy.BIGINT),
                        sqlalchemy.Column("failed", sqlalchemy.BIGINT),
                        sqlalchemy.Column("actions_per_second", sqlalchemy.FLOAT),
                    )

                    await connection.execute(
                        CreateTable(stage_metrics_table, if_not_exists=True)
                    )
                    self._shared_metrics_table = stage_metrics_table

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Created or set Shared Metrics table - {self.shared_metrics_table_name}"
                    )

                for metrics_set in metrics_sets:
                    await connection.execute(
                        self._metrics_table.insert(
                            values={
                                "name": metrics_set.name,
                                "stage": metrics_set.stage,
                                "group": "common",
                                **metrics_set.common_stats,
                            }
                        )
                    )

                await transaction.commit()
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name} - Transaction committed"
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Shared Metrics to Table - {self.shared_metrics_table_name}"
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name}"
        )

        async with self._engine.connect() as connection:
            connection: AsyncConnection = connection

            async with connection.begin() as transaction:
                transaction: AsyncTransaction = transaction

                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name} - Initiating transaction"
                )

                for metrics_set in metrics:
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
                    )

                    if self._metrics_table is None:
                        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                            f"{self.metadata_string} - Creating Metrics table - {self.metrics_table_name} - if not exists"
                        )

                        metrics_table = sqlalchemy.Table(
                            self.metrics_table_name,
                            self.metadata,
                            sqlalchemy.Column(
                                "id",
                                UUID(as_uuid=True),
                                primary_key=True,
                                default=uuid.uuid4,
                            ),
                            sqlalchemy.Column("name", sqlalchemy.VARCHAR(255)),
                            sqlalchemy.Column("stage", sqlalchemy.VARCHAR(255)),
                            sqlalchemy.Column("group", sqlalchemy.VARCHAR(255)),
                            sqlalchemy.Column("median", sqlalchemy.FLOAT),
                            sqlalchemy.Column("mean", sqlalchemy.FLOAT),
                            sqlalchemy.Column("variance", sqlalchemy.FLOAT),
                            sqlalchemy.Column("stdev", sqlalchemy.FLOAT),
                            sqlalchemy.Column("minimum", sqlalchemy.FLOAT),
                            sqlalchemy.Column("maximum", sqlalchemy.FLOAT),
                        )

                        for quantile in metrics_set.quantiles:
                            metrics_table.append_column(
                                sqlalchemy.Column(f"{quantile}", sqlalchemy.FLOAT)
                            )

                        for (
                            custom_field_name,
                            sql_alchemy_type,
                        ) in metrics_set.custom_schemas:
                            metrics_table.append_column(
                                custom_field_name, sql_alchemy_type
                            )

                        await connection.execute(
                            CreateTable(metrics_table, if_not_exists=True)
                        )
                        self._metrics_table = metrics_table

                        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                            f"{self.metadata_string} - Created or set Metrics table - {self.metrics_table_name}"
                        )

                    for group_name, group in metrics_set.groups.items():
                        await connection.execute(
                            self._metrics_table.insert(
                                values={**group.record, "group": group_name}
                            )
                        )

                await transaction.commit()
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name} - Transaction committed"
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Metrics to Table - {self.metrics_table_name}"
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        async with self._engine.connect() as connection:
            connection: AsyncConnection = connection

            async with connection.begin() as transaction:
                transaction: AsyncTransaction = transaction

                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Custom Metrics - Initiating transaction"
                )

                await self.logger.filesystem.aio["hyperscale.reporting"].info(
                    f"{self.metadata_string} - Submitting Custom Metrics to table - {self.custom_metrics_table_name}"
                )

                if self._custom_metrics_table is None:
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Creating Custom Metrics table - {self.custom_metrics_table_name} - if not exists"
                    )

                    custom_metrics_table = sqlalchemy.Table(
                        self.custom_metrics_table_name,
                        self.metadata,
                        sqlalchemy.Column(
                            "id",
                            UUID(as_uuid=True),
                            primary_key=True,
                            default=uuid.uuid4,
                        ),
                        sqlalchemy.Column("name", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("stage", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("group", sqlalchemy.VARCHAR(255)),
                    )

                    for metrics_set in metrics_sets:
                        for (
                            custom_metric_name,
                            custom_metric,
                        ) in metrics_set.custom_metrics.items():
                            custom_metrics_table.append_column(
                                self.metric_types_map.get(
                                    custom_metric.metric_type,
                                    lambda field_name: sqlalchemy.Column(
                                        field_name, sqlalchemy.FLOAT
                                    ),
                                )(custom_metric_name)
                            )

                    await connection.execute(
                        CreateTable(custom_metrics_table, if_not_exists=True)
                    )

                    self._custom_metrics_table = custom_metrics_table

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Created or set Custom Metrics table - {self.custom_metrics_table_name}"
                    )

                for metrics_set in metrics_sets:
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
                    )

                    await connection.execute(
                        self._custom_metrics_table.insert(
                            values={
                                "name": metrics_set.name,
                                "stage": metrics_set.stage,
                                "group": "custom",
                                **{
                                    custom_metric_name: custom_metric.metric_value
                                    for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                                },
                            }
                        )
                    )

                    await self.logger.filesystem.aio["hyperscale.reporting"].info(
                        f"{self.metadata_string} - Submitted Custom Metrics to table - {self.custom_metrics_table_name}"
                    )

                await transaction.commit()
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Custom Metrics - Transaction committed"
                )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name}"
        )

        async with self._engine.connect() as connection:
            connection: AsyncConnection = connection

            async with connection.begin() as transaction:
                transaction: AsyncTransaction = transaction

                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name} - Initiating transaction"
                )

                for metrics_set in metrics_sets:
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
                    )

                    if self._errors_table is None:
                        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                            f"{self.metadata_string} - Creating Error Metrics table - {self.errors_table_name} - if not exists"
                        )

                        errors_table = sqlalchemy.Table(
                            self.errors_table_name,
                            self.metadata,
                            sqlalchemy.Column(
                                "id",
                                UUID(as_uuid=True),
                                primary_key=True,
                                default=uuid.uuid4,
                            ),
                            sqlalchemy.Column("name", sqlalchemy.VARCHAR(255)),
                            sqlalchemy.Column("stage", sqlalchemy.VARCHAR(255)),
                            sqlalchemy.Column("error_message", sqlalchemy.TEXT),
                            sqlalchemy.Column("error_count", sqlalchemy.Integer),
                        )

                        await connection.execute(
                            CreateTable(errors_table, if_not_exists=True)
                        )
                        self._errors_table = errors_table

                        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                            f"{self.metadata_string} - Created or set Error Metrics table - {self.errors_table_name}"
                        )

                    for error in metrics_set.errors:
                        await connection.execute(
                            self._errors_table.insert(
                                values={
                                    "name": metrics_set.name,
                                    "stage": metrics_set.stage,
                                    "error_message": error.get("message"),
                                    "error_count": error.get("count"),
                                }
                            )
                        )

                await transaction.commit()
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name} - Transaction committed"
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Error Metrics to Table - {self.errors_table_name}"
        )

    async def close(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Session Closed - {self.session_uuid}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closed connectiion to {self.sql_type} at - {self.host}"
        )
