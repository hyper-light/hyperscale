import uuid
from datetime import datetime
from typing import Dict, List

from hyperscale.reporting.experiment.experiments_collection import (
    ExperimentMetricsCollectionSet,
)
from hyperscale.reporting.metric import MetricsSet
from hyperscale.reporting.metric.stage_streams_set import StageStreamsSet
from hyperscale.reporting.processed_result.types.base_processed_result import (
    BaseProcessedResult,
)
from hyperscale.reporting.system.system_metrics_set import (
    SessionMetricsCollection,
    SystemMetricsCollection,
    SystemMetricsSet,
)

try:
    import sqlalchemy
    from sqlalchemy.dialects.postgresql import UUID
    from sqlalchemy.schema import CreateTable

    from hyperscale.reporting.types.postgres.postgres import Postgres

    from .timescaledb_config import TimescaleDBConfig

    has_connector = True

except Exception:
    sqlalchemy = None
    UUID = None
    from hyperscale.reporting.types.empty import Empty as Postgres

    CreateTable = None
    TimescaleDBConfig = None
    has_connector = False


class TimescaleDB(Postgres):
    def __init__(self, config: TimescaleDBConfig) -> None:
        super().__init__(config)

    async def submit_session_system_metrics(
        self, system_metrics_sets: List[SystemMetricsSet]
    ):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Session System Metrics to Table - {self.session_system_metrics_table_name}"
        )

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Session System Metrics to Table - {self.session_system_metrics_table_name} - Initiating transaction"
            )

            if self._session_system_metrics_table is None:
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Creating Session System Metrics table - {self.session_system_metrics_table_name} - if not exists"
                )

                session_system_metrics_table = sqlalchemy.Table(
                    self.session_system_metrics_table_name,
                    self.metadata,
                    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column("name", sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column("group", sqlalchemy.TEXT()),
                    sqlalchemy.Column("median", sqlalchemy.FLOAT),
                    sqlalchemy.Column("mean", sqlalchemy.FLOAT),
                    sqlalchemy.Column("variance", sqlalchemy.FLOAT),
                    sqlalchemy.Column("stdev", sqlalchemy.FLOAT),
                    sqlalchemy.Column("minimum", sqlalchemy.FLOAT),
                    sqlalchemy.Column("maximum", sqlalchemy.FLOAT),
                )

                for quantile in SystemMetricsSet.quantiles:
                    session_system_metrics_table.append_column(
                        sqlalchemy.Column(f"quantile_{quantile}th", sqlalchemy.FLOAT)
                    )

                await self._connection.execute(
                    CreateTable(session_system_metrics_table, if_not_exists=True)
                )

                await self._connection.execute(
                    f"SELECT create_hypertable('{self.session_system_metrics_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                )

                await self._connection.execute(
                    f"CREATE INDEX ON {self.session_system_metrics_table_name} (name, time DESC);"
                )

                self._session_system_metrics_table = session_system_metrics_table

                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Created or set Session System Metrics table - {self.session_system_metrics_table_name}"
                )

            rows: List[SessionMetricsCollection] = []

            for metrics_set in system_metrics_sets:
                for monitor_metrics in metrics_set.session_cpu_metrics.values():
                    rows.append(monitor_metrics)

                for monitor_metrics in metrics_set.session_memory_metrics.values():
                    rows.append(monitor_metrics)

            for metrics_set in rows:
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Session System Metrics - {metrics_set.name}:{metrics_set.group}"
                )

                await self._connection.execute(
                    self._streams_table.insert(values=metrics_set.record)
                )

            await transaction.commit()
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Session System Metrics to Table - {self.session_system_metrics_table_name} - Transaction committed"
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Session System Metrics to Table - {self.session_system_metrics_table_name}"
        )

    async def submit_stage_system_metrics(
        self, system_metrics_sets: List[SystemMetricsSet]
    ):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Stage System Metrics to Table - {self.stage_system_metrics_table_name}"
        )

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Stage System Metrics to Table - {self.stage_system_metrics_table_name} - Initiating transaction"
            )
            if self._stage_system_metrics_table is None:
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Creating Stage System Metrics table - {self.stage_system_metrics_table_name} - if not exists"
                )

                stage_system_metrics_table = sqlalchemy.Table(
                    self.stage_system_metrics_table_name,
                    self.metadata,
                    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column("name", sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column("stage", sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column("group", sqlalchemy.TEXT()),
                    sqlalchemy.Column("median", sqlalchemy.FLOAT),
                    sqlalchemy.Column("mean", sqlalchemy.FLOAT),
                    sqlalchemy.Column("variance", sqlalchemy.FLOAT),
                    sqlalchemy.Column("stdev", sqlalchemy.FLOAT),
                    sqlalchemy.Column("minimum", sqlalchemy.FLOAT),
                    sqlalchemy.Column("maximum", sqlalchemy.FLOAT),
                )

                for quantile in SystemMetricsSet.quantiles:
                    stage_system_metrics_table.append_column(
                        sqlalchemy.Column(f"quantile_{quantile}th", sqlalchemy.FLOAT)
                    )

                await self._connection.execute(
                    CreateTable(stage_system_metrics_table, if_not_exists=True)
                )

                await self._connection.execute(
                    f"SELECT create_hypertable('{self.stage_system_metrics_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                )

                await self._connection.execute(
                    f"CREATE INDEX ON {self.stage_system_metrics_table_name} (name, time DESC);"
                )

                self._stage_system_metrics_table = stage_system_metrics_table

                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Created or set Stage System Metrics table - {self.stage_system_metrics_table_name}"
                )

            rows: List[SystemMetricsCollection] = []

            for metrics_set in system_metrics_sets:
                cpu_metrics = metrics_set.cpu
                memory_metrics = metrics_set.memory

                for stage_name, stage_cpu_metrics in cpu_metrics.metrics.items():
                    for monitor_metrics in stage_cpu_metrics.values():
                        rows.append(monitor_metrics)

                    stage_memory_metrics = memory_metrics.metrics.get(stage_name)
                    for monitor_metrics in stage_memory_metrics.values():
                        rows.append(monitor_metrics)

                    stage_mb_per_vu_metrics = metrics_set.mb_per_vu.get(stage_name)

                    if stage_mb_per_vu_metrics:
                        rows.append(stage_mb_per_vu_metrics)

            for metrics_set in rows:
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Stage System Metrics - {metrics_set.name}:{metrics_set.group}"
                )

                await self._connection.execute(
                    self._streams_table.insert(values=metrics_set.record)
                )

            await transaction.commit()
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Stage System Metrics to Table - {self.stage_system_metrics_table_name} - Transaction committed"
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Stage System Metrics to Table - {self.stage_system_metrics_table_name}"
        )

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Streams to Table - {self.streams_table_name}"
        )

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Streams to Table - {self.streams_table_name} - Initiating transaction"
            )

            for stage_name, stream in stream_metrics.items():
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Streams - {stage_name}:{stream.stream_set_id}"
                )

                if self._streams_table is None:
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Creating Streams table - {self.streams_table_name} - if not exists"
                    )

                    stream_table = sqlalchemy.Table(
                        self.streams_table_name,
                        self.metadata,
                        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                        sqlalchemy.Column("name", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("stage", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("group", sqlalchemy.TEXT()),
                        sqlalchemy.Column("median", sqlalchemy.FLOAT),
                        sqlalchemy.Column("mean", sqlalchemy.FLOAT),
                        sqlalchemy.Column("variance", sqlalchemy.FLOAT),
                        sqlalchemy.Column("stdev", sqlalchemy.FLOAT),
                        sqlalchemy.Column("minimum", sqlalchemy.FLOAT),
                        sqlalchemy.Column("maximum", sqlalchemy.FLOAT),
                    )

                    for quantile in stream.quantiles:
                        stream_table.append_column(
                            sqlalchemy.Column(
                                f"quantile_{quantile}th", sqlalchemy.FLOAT
                            )
                        )

                    await self._connection.execute(
                        CreateTable(stream_table, if_not_exists=True)
                    )

                    await self._connection.execute(
                        f"SELECT create_hypertable('{self.streams_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                    )

                    await self._connection.execute(
                        f"CREATE INDEX ON {self.streams_table_name} (name, time DESC);"
                    )

                    self._streams_table = stream_table

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Created or set Streams table - {self.streams_table_name}"
                    )

                for group_name, group in stream.grouped.items():
                    await self._connection.execute(
                        self._streams_table.insert(
                            values={
                                "name": f"{stage_name}_streams",
                                "stage": stage_name,
                                "group": group_name,
                                **group,
                            }
                        )
                    )

            await transaction.commit()
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Streams to Table - {self.streams_table_name} - Transaction committed"
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Streams to Table - {self.streams_table_name}"
        )

    async def submit_experiments(
        self, experiments_metrics: ExperimentMetricsCollectionSet
    ):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Experiments to Table - {self.experiments_table_name}"
        )

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Experiments to Table - {self.experiments_table_name} - Initiating transaction"
            )

            for experiment in experiments_metrics.experiment_summaries:
                if self._experiments_table is None:
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Creating Experiments table - {self.experiments_table_name} - if not exists"
                    )

                    experiments_table = sqlalchemy.Table(
                        self.experiments_table_name,
                        self.metadata,
                        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                        sqlalchemy.Column("experiment_name", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("experiment_randomized", sqlalchemy.Boolean),
                        sqlalchemy.Column("experiment_completed", sqlalchemy.BIGINT),
                        sqlalchemy.Column("experiment_succeeded", sqlalchemy.BIGINT),
                        sqlalchemy.Column("experiment_failed", sqlalchemy.BIGINT),
                        sqlalchemy.Column("experiment_median_aps", sqlalchemy.FLOAT),
                    )

                    await self._connection.execute(
                        CreateTable(experiments_table, if_not_exists=True)
                    )

                    await self._connection.execute(
                        f"SELECT create_hypertable('{self.experiments_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                    )

                    await self._connection.execute(
                        f"CREATE INDEX ON {self.experiments_table_name} (name, time DESC);"
                    )

                    self._experiments_table = experiments_table

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Created or set Experiments table - {self.experiments_table_name}"
                    )

                await self._connection.execute(
                    self._experiments_table.insert().values(**experiment.record)
                )

            await transaction.commit()
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Experiments to Table - {self.experiments_table_name} - Transaction committed"
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Experiments to Table - {self.experiments_table_name}"
        )

    async def submit_variants(
        self, experiments_metrics: ExperimentMetricsCollectionSet
    ):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Variants to Table - {self.variants_table_name}"
        )

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Variants to Table - {self.variants_table_name} - Initiating transaction"
            )

            for variant in experiments_metrics.variant_summaries:
                if self._variants_table is None:
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Creating Variants table - {self.variants_table_name} - if not exists"
                    )

                    variants_table = sqlalchemy.Table(
                        self.variants_table_name,
                        self.metadata,
                        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                        sqlalchemy.Column("variant_name", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column(
                            "variant_experiment", sqlalchemy.VARCHAR(255)
                        ),
                        sqlalchemy.Column("variant_weight", sqlalchemy.FLOAT),
                        sqlalchemy.Column(
                            "variant_distribution", sqlalchemy.VARCHAR(255)
                        ),
                        sqlalchemy.Column(
                            "variant_distribution_interval", sqlalchemy.FLOAT
                        ),
                        sqlalchemy.Column("variant_completed", sqlalchemy.BIGINT),
                        sqlalchemy.Column("variant_succeeded", sqlalchemy.BIGINT),
                        sqlalchemy.Column("variant_failed", sqlalchemy.BIGINT),
                        sqlalchemy.Column(
                            "variant_actions_per_second", sqlalchemy.FLOAT
                        ),
                        sqlalchemy.Column("variant_ratio_completed", sqlalchemy.FLOAT),
                        sqlalchemy.Column("variant_ratio_succeeded", sqlalchemy.FLOAT),
                        sqlalchemy.Column("variant_ratio_failed", sqlalchemy.FLOAT),
                        sqlalchemy.Column("variant_ratio_aps", sqlalchemy.FLOAT),
                    )

                    await self._connection.execute(
                        CreateTable(variants_table, if_not_exists=True)
                    )

                    await self._connection.execute(
                        f"SELECT create_hypertable('{self.variants_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                    )

                    await self._connection.execute(
                        f"CREATE INDEX ON {self.variants_table_name} (name, time DESC);"
                    )

                    self._variants_table = variants_table

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Created or set Variants table - {self.variants_table_name}"
                    )

                await self._connection.execute(
                    self._variants_table.insert().values(**variant.record)
                )

            await transaction.commit()
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Variants to Table - {self.variants_table_name} - Transaction committed"
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Variants to Table - {self.variants_table_name}"
        )

    async def submit_mutations(
        self, experiments_metrics: ExperimentMetricsCollectionSet
    ):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Mutations to Table - {self.mutations_table_name}"
        )

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Mutations to Table - {self.mutations_table_name} - Initiating transaction"
            )

            for mutation in experiments_metrics.mutation_summaries:
                if self._mutations_table is None:
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Creating Mutations table - {self.mutations_table_name} - if not exists"
                    )

                    mutations_table = sqlalchemy.Table(
                        self.mutations_table_name,
                        self.metadata,
                        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                        sqlalchemy.Column("mutation_name", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column(
                            "mutation_experiment_name", sqlalchemy.VARCHAR(255)
                        ),
                        sqlalchemy.Column(
                            "mutation_variant_name", sqlalchemy.VARCHAR(255)
                        ),
                        sqlalchemy.Column("mutation_chance", sqlalchemy.FLOAT),
                        sqlalchemy.Column("mutation_targets", sqlalchemy.VARCHAR(8192)),
                        sqlalchemy.Column("mutation_type", sqlalchemy.VARCHAR(255)),
                    )

                    await self._connection.execute(
                        CreateTable(mutations_table, if_not_exists=True)
                    )

                    await self._connection.execute(
                        f"SELECT create_hypertable('{self.mutations_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                    )

                    await self._connection.execute(
                        f"CREATE INDEX ON {self.mutations_table_name} (name, time DESC);"
                    )

                    self._mutations_table = mutations_table

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Created or set Mutations table - {self.mutations_table_name}"
                    )

                await self._connection.execute(
                    self._mutations_table.insert().values(**mutation.record)
                )

            await transaction.commit()
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Mutations to Table - {self.mutations_table_name} - Transaction committed"
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Mutations to Table - {self.mutations_table_name}"
        )

    async def submit_events(self, events: List[BaseProcessedResult]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Events to Table - {self.events_table_name}"
        )

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Events to Table - {self.events_table_name} - Initiating transaction"
            )

            for event in events:
                if self._events_table is None:
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Creating Events table - {self.events_table_name} - if not exists"
                    )

                    events_table = sqlalchemy.Table(
                        self.events_table_name,
                        self.metadata,
                        sqlalchemy.Column(
                            "id",
                            UUID(as_uuid=True),
                            primary_key=True,
                            default=uuid.uuid4,
                        ),
                        sqlalchemy.Column("name", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("stage", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("request_time", sqlalchemy.Float),
                        sqlalchemy.Column("succeeded", sqlalchemy.Boolean),
                        sqlalchemy.Column(
                            "time",
                            sqlalchemy.TIMESTAMP(timezone=False),
                            nullable=False,
                            default=datetime.now(),
                        ),
                    )

                    await self._connection.execute(
                        CreateTable(events_table, if_not_exists=True)
                    )
                    await self._connection.execute(
                        f"SELECT create_hypertable('{self.events_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                    )

                    await self._connection.execute(
                        f"CREATE INDEX ON {self.events_table_name} (name, time DESC);"
                    )

                    self._events_table = events_table
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Created or set Events table - {self.events_table_name}"
                    )

                record = event.record
                record["request_time"] = record["time"]
                del record["time"]

                await self._connection.execute(
                    self._events_table.insert(
                        values={**record, "time": datetime.now().timestamp()}
                    )
                )

            await transaction.commit()
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Events to Table - {self.events_table_name} - Transaction committed"
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Events to Table - {self.events_table_name}"
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name}"
        )

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name} - Initiating transaction"
            )

            if self._stage_metrics_table is None:
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Creating Shared Metrics table - {self.shared_metrics_table_name} - if not exists"
                )

                stage_metrics_table = sqlalchemy.Table(
                    self.stage_metrics_table_name,
                    self.metadata,
                    sqlalchemy.Column("id", UUID(as_uuid=True), default=uuid.uuid4),
                    sqlalchemy.Column("name", sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column("stage", sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column("group", sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column("total", sqlalchemy.BIGINT),
                    sqlalchemy.Column("succeeded", sqlalchemy.BIGINT),
                    sqlalchemy.Column("failed", sqlalchemy.BIGINT),
                    sqlalchemy.Column("actions_per_second", sqlalchemy.FLOAT),
                    sqlalchemy.Column(
                        "time",
                        sqlalchemy.TIMESTAMP(timezone=False),
                        nullable=False,
                        default=datetime.now(),
                    ),
                )

                await self._connection.execute(
                    CreateTable(stage_metrics_table, if_not_exists=True)
                )
                await self._connection.execute(
                    f"SELECT create_hypertable('{self.stage_metrics_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                )

                await self._connection.execute(
                    f"CREATE INDEX ON {self.stage_metrics_table_name} (name, time DESC);"
                )

                self._stage_metrics_table = stage_metrics_table

                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Created or set Shared Metrics table - {self.shared_metrics_table_name}"
                )

            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
                )
                await self._connection.execute(
                    self._stage_metrics_table.insert(
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

                    metrics_table = sqlalchemy.Table(
                        self.metrics_table_name,
                        self.metadata,
                        sqlalchemy.Column("id", UUID(as_uuid=True), default=uuid.uuid4),
                        sqlalchemy.Column("name", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("stage", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("group", sqlalchemy.TEXT),
                        sqlalchemy.Column("median", sqlalchemy.FLOAT),
                        sqlalchemy.Column("mean", sqlalchemy.FLOAT),
                        sqlalchemy.Column("variance", sqlalchemy.FLOAT),
                        sqlalchemy.Column("stdev", sqlalchemy.FLOAT),
                        sqlalchemy.Column("minimum", sqlalchemy.FLOAT),
                        sqlalchemy.Column("maximum", sqlalchemy.FLOAT),
                        sqlalchemy.Column(
                            "time",
                            sqlalchemy.TIMESTAMP(timezone=False),
                            nullable=False,
                            default=datetime.now(),
                        ),
                    )

                    for quantile in metrics_set.quantiles:
                        metrics_table.append_column(
                            sqlalchemy.Column(f"{quantile}", sqlalchemy.FLOAT)
                        )

                    for (
                        custom_field_name,
                        sql_alchemy_type,
                    ) in metrics_set.custom_schemas:
                        metrics_table.append_column(custom_field_name, sql_alchemy_type)

                    await self._connection.execute(
                        CreateTable(metrics_table, if_not_exists=True)
                    )
                    await self._connection.execute(
                        f"SELECT create_hypertable('{self.metrics_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                    )

                    await self._connection.execute(
                        f"CREATE INDEX ON {self.metrics_table_name} (name, time DESC);"
                    )

                    self._metrics_table = metrics_table

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Created or set Metrics table - {self.metrics_table_name}"
                    )

                for group_name, group in metrics_set.groups.items():
                    await self._connection.execute(
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
        if self._custom_metrics_table is None:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Creating Custom Metrics table - {self.custom_metrics_table_name} - if not exists"
            )

            custom_metrics_table = sqlalchemy.Table(
                self.custom_metrics_table_name,
                self.metadata,
                sqlalchemy.Column("id", UUID(as_uuid=True), default=uuid.uuid4),
                sqlalchemy.Column("name", sqlalchemy.VARCHAR(255)),
                sqlalchemy.Column("stage", sqlalchemy.VARCHAR(255)),
                sqlalchemy.Column("group", sqlalchemy.TEXT),
                sqlalchemy.Column(
                    "time",
                    sqlalchemy.TIMESTAMP(timezone=False),
                    nullable=False,
                    default=datetime.now(),
                ),
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

            await self._connection.execute(
                CreateTable(custom_metrics_table, if_not_exists=True)
            )

            await self._connection.execute(
                f"SELECT create_hypertable('{self.custom_metrics_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
            )

            await self._connection.execute(
                f"CREATE INDEX ON {self.custom_metrics_table_name} (name, time DESC);"
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

                    errors_table = sqlalchemy.Table(
                        self.errors_table_name,
                        self.metadata,
                        sqlalchemy.Column("id", UUID(as_uuid=True), default=uuid.uuid4),
                        sqlalchemy.Column("metric_name", sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column("metrics_stage", sqlalchemy.TEXT),
                        sqlalchemy.Column("error_message", sqlalchemy.TEXT),
                        sqlalchemy.Column("error_count", sqlalchemy.BIGINT),
                        sqlalchemy.Column(
                            "time",
                            sqlalchemy.TIMESTAMP(timezone=False),
                            nullable=False,
                            default=datetime.now(),
                        ),
                    )

                    await self._connection.execute(
                        CreateTable(errors_table, if_not_exists=True)
                    )
                    await self._connection.execute(
                        f"SELECT create_hypertable('{self.errors_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                    )

                    await self._connection.execute(
                        f"CREATE INDEX ON {self.errors_table_name}_errors (name, time DESC);"
                    )

                    self._errors_table = errors_table

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Created or set Error Metrics table - {self.errors_table_name}"
                    )

                for error in metrics_set.errors:
                    await self._connection.execute(
                        self._metrics_table.insert(
                            values={
                                "metric_name": metrics_set.name,
                                "metrics_stage": metrics_set.stage,
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
            f"{self.metadata_string} - Closing session - {self.session_uuid}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closing connectiion to {self.sql_type} at - {self.host}"
        )

        await self._connection.close()
        self._engine.terminate()
        await self._engine.wait_closed()

        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Session Closed - {self.session_uuid}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closed connectiion to {self.sql_type} at - {self.host}"
        )
