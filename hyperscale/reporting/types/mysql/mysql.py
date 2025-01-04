import uuid
import warnings
from typing import Dict, List


from hyperscale.reporting.metric import MetricsSet, MetricType

from .mysql_config import MySQLConfig

try:
    # Aiomysql will raise warnings if a table exists despite us
    # explicitly passing "IF NOT EXISTS", so we're going to
    # ignore them.
    import aiomysql
    import sqlalchemy as sa

    warnings.filterwarnings("ignore", category=aiomysql.Warning)

    from aiomysql.sa import SAConnection, create_engine
    from sqlalchemy.schema import CreateTable

    has_connector = True

except Exception:
    has_connector = False

    class sqlalchemy:
        pass

    class sa:
        pass

    class CreateTable:
        pass

    class SAConnection:
        pass

    class OperationalError:
        pass

    def create_engine(*args, **kwargs):
        pass


class MySQL:
    def __init__(self, config: MySQLConfig) -> None:
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

        self._events_table = None
        self._metrics_table = None
        self._streams_table = None

        self._experiments_table = None
        self._variants_table = None
        self._mutations_table = None

        self._shared_metrics_table = None
        self._custom_metrics_table = None
        self._errors_table = None

        self._session_system_metrics_table = None
        self._stage_system_metrics_table = None

        self.metadata = sa.MetaData()
        self._engine = None
        self._connection = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.metric_types_map = {
            MetricType.COUNT: lambda field_name: sa.Column(field_name, sa.Integer),
            MetricType.DISTRIBUTION: lambda field_name: sa.Column(field_name, sa.Float),
            MetricType.SAMPLE: lambda field_name: sa.Column(field_name, sa.Float),
            MetricType.RATE: lambda field_name: sa.Column(field_name, sa.Float),
        }

    async def connect(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connecting to MySQL instance at - {self.host} - Database: {self.database}"
        )
        self._engine = await create_engine(
            db=self.database, host=self.host, user=self.username, password=self.password
        )

        self._connection: SAConnection = await self._engine.acquire()

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connected to MySQL instance at - {self.host} - Database: {self.database}"
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name}"
        )

        if self._shared_metrics_table is None:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Creating Shared Metrics table - {self.shared_metrics_table_name} - if not exists"
            )

            stage_metrics_table = sa.Table(
                self.shared_metrics_table_name,
                self.metadata,
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("name", sa.VARCHAR(255)),
                sa.Column("stage", sa.VARCHAR(255)),
                sa.Column("group", sa.VARCHAR(255)),
                sa.Column("total", sa.BIGINT),
                sa.Column("succeeded", sa.BIGINT),
                sa.Column("failed", sa.BIGINT),
                sa.Column("actions_per_second", sa.FLOAT),
            )

            await self._connection.execute(
                CreateTable(stage_metrics_table, if_not_exists=True)
            )
            self._shared_metrics_table = stage_metrics_table

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Created or set Shared Metrics table - {self.shared_metrics_table_name}"
            )

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name} - Initiating transaction"
            )

            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
                )

                await self._connection.execute(
                    self._shared_metrics_table.insert(
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

        async with self._connection.begin() as transaction:
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

                    metrics_table = sa.Table(
                        self.metrics_table_name,
                        self.metadata,
                        sa.Column("id", sa.Integer, primary_key=True),
                        sa.Column("name", sa.VARCHAR(255)),
                        sa.Column("stage", sa.VARCHAR(255)),
                        sa.Column("group", sa.TEXT()),
                        sa.Column("median", sa.FLOAT),
                        sa.Column("mean", sa.FLOAT),
                        sa.Column("variance", sa.FLOAT),
                        sa.Column("stdev", sa.FLOAT),
                        sa.Column("minimum", sa.FLOAT),
                        sa.Column("maximum", sa.FLOAT),
                    )

                    for quantile in metrics_set.quantiles:
                        metrics_table.append_column(sa.Column(f"{quantile}", sa.FLOAT))

                    for (
                        custom_field_name,
                        sql_alchemy_type,
                    ) in metrics_set.custom_schemas:
                        metrics_table.append_column(custom_field_name, sql_alchemy_type)

                    await self._connection.execute(
                        CreateTable(metrics_table, if_not_exists=True)
                    )

                    self._metrics_table = metrics_table

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Created or set Metrics table - {self.metrics_table_name}"
                    )

                for group_name, group in metrics_set.groups.items():
                    await self._connection.execute(
                        self._metrics_table.insert(
                            values={"group": group_name, **group.record}
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
        if self._custom_metrics_table is None:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Creating Custom Metrics table - {self.custom_metrics_table_name} - if not exists"
            )

            custom_metrics_table = sa.Table(
                self.custom_metrics_table_name,
                self.metadata,
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("name", sa.VARCHAR(255)),
                sa.Column("stage", sa.VARCHAR(255)),
                sa.Column("group", sa.VARCHAR(255)),
            )

            for metrics_set in metrics_sets:
                for (
                    custom_metric_name,
                    custom_metric,
                ) in metrics_set.custom_metrics.items():
                    custom_metrics_table.append_column(
                        self.metric_types_map.get(
                            custom_metric.metric_type,
                            lambda field_name: sa.Column(field_name, sa.Float),
                        )(custom_metric_name)
                    )

            await self._connection.execute(
                CreateTable(custom_metrics_table, if_not_exists=True)
            )

            self._custom_metrics_table = custom_metrics_table

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Created or set Custom Metrics table - {self.custom_metrics_table_name}"
            )

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Custom Metrics - Initiating transaction"
            )

            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
                )

                await self.logger.filesystem.aio["hyperscale.reporting"].info(
                    f"{self.metadata_string} - Submitting Custom Metrics to table - {self.custom_metrics_table_name}"
                )

                await self._connection.execute(
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

        async with self._connection.begin() as transaction:
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

                    errors_table = sa.Table(
                        self.errors_table_name,
                        self.metadata,
                        sa.Column("id", sa.Integer, primary_key=True),
                        sa.Column("name", sa.VARCHAR(255)),
                        sa.Column("stage", sa.VARCHAR(255)),
                        sa.Column("error_message", sa.TEXT),
                        sa.Column("error_count", sa.Integer),
                    )

                    await self._connection.execute(
                        CreateTable(errors_table, if_not_exists=True)
                    )

                    self._errors_table = errors_table

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Created or set Error Metrics table - {self.errors_table_name}"
                    )

                for error in metrics_set.errors:
                    await self._connection.execute(
                        self._errors_table.insert(
                            values={
                                "name": metrics_set.name,
                                "stage": metrics_set.stage,
                                "error_messages": error.get("message"),
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
            f"{self.metadata_string} - Closing session - {self.session_uuid}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closing connectiion to MySQL at - {self.host}"
        )

        await self._connection.close()

        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Session Closed - {self.session_uuid}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closed connectiion to MySQL at - {self.host}"
        )
