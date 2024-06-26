from __future__ import annotations

import os
import threading
import uuid
from typing import Any, Dict, List, TypeVar, Union

from hyperscale.core.personas.streaming.stream_analytics import StreamAnalytics
from hyperscale.logging.hyperscale_logger import HyperscaleLogger
from hyperscale.plugins.types.reporter.reporter_config import ReporterConfig

from .experiment.experiment_metrics_set import ExperimentMetricsSet
from .experiment.experiment_metrics_set_types import MutationSummary, VariantSummary
from .experiment.experiments_collection import (
    ExperimentMetricsCollection,
    ExperimentMetricsCollectionSet,
)
from .types import (
    CSV,
    JSON,
    S3,
    XML,
    AWSLambda,
    AWSLambdaConfig,
    AWSTimestream,
    AWSTimestreamConfig,
    BigQuery,
    BigQueryConfig,
    BigTable,
    BigTableConfig,
    Cassandra,
    CassandraConfig,
    Cloudwatch,
    CloudwatchConfig,
    CosmosDB,
    CosmosDBConfig,
    CSVConfig,
    Datadog,
    DatadogConfig,
    DogStatsD,
    DogStatsDConfig,
    GoogleCloudStorage,
    GoogleCloudStorageConfig,
    Graphite,
    GraphiteConfig,
    Honeycomb,
    HoneycombConfig,
    InfluxDB,
    InfluxDBConfig,
    JSONConfig,
    Kafka,
    KafkaConfig,
    MongoDB,
    MongoDBConfig,
    MySQL,
    MySQLConfig,
    Netdata,
    NetdataConfig,
    NewRelic,
    NewRelicConfig,
    Postgres,
    PostgresConfig,
    Prometheus,
    PrometheusConfig,
    Redis,
    RedisConfig,
    ReporterTypes,
    S3Config,
    Snowflake,
    SnowflakeConfig,
    SQLite,
    SQLiteConfig,
    StatsD,
    StatsDConfig,
    Telegraf,
    TelegrafConfig,
    TelegrafStatsD,
    TelegrafStatsDConfig,
    TimescaleDB,
    TimescaleDBConfig,
    XMLConfig,
)

ReporterType = TypeVar(
    "ReporterType",
    AWSLambdaConfig,
    AWSTimestreamConfig,
    BigQueryConfig,
    BigTableConfig,
    CassandraConfig,
    CloudwatchConfig,
    CosmosDBConfig,
    CSVConfig,
    DatadogConfig,
    DogStatsDConfig,
    GoogleCloudStorageConfig,
    GraphiteConfig,
    HoneycombConfig,
    InfluxDBConfig,
    JSONConfig,
    KafkaConfig,
    MongoDBConfig,
    MySQLConfig,
    NetdataConfig,
    NewRelicConfig,
    PostgresConfig,
    PrometheusConfig,
    RedisConfig,
    S3Config,
    SnowflakeConfig,
    SQLiteConfig,
    StatsDConfig,
    TelegrafConfig,
    TelegrafStatsDConfig,
    TimescaleDBConfig,
    XMLConfig,
)


class Reporter:
    reporters = {
        ReporterTypes.AWSLambda: lambda config: AWSLambda(config),
        ReporterTypes.AWSTimestream: lambda config: AWSTimestream(config),
        ReporterTypes.BigQuery: lambda config: BigQuery(config),
        ReporterTypes.BigTable: lambda config: BigTable(config),
        ReporterTypes.Cassandra: lambda config: Cassandra(config),
        ReporterTypes.Cloudwatch: lambda config: Cloudwatch(config),
        ReporterTypes.CosmosDB: lambda config: CosmosDB(config),
        ReporterTypes.CSV: lambda config: CSV(config),
        ReporterTypes.Datadog: lambda config: Datadog(config),
        ReporterTypes.DogStatsD: lambda config: DogStatsD(config),
        ReporterTypes.GCS: lambda config: GoogleCloudStorage(config),
        ReporterTypes.Graphite: lambda config: Graphite(config),
        ReporterTypes.Honeycomb: lambda config: Honeycomb(config),
        ReporterTypes.InfluxDB: lambda config: InfluxDB(config),
        ReporterTypes.JSON: lambda config: JSON(config),
        ReporterTypes.Kafka: lambda config: Kafka(config),
        ReporterTypes.MongoDB: lambda config: MongoDB(config),
        ReporterTypes.MySQL: lambda config: MySQL(config),
        ReporterTypes.Netdata: lambda config: Netdata(config),
        ReporterTypes.NewRelic: lambda config: NewRelic(config),
        ReporterTypes.Postgres: lambda config: Postgres(config),
        ReporterTypes.Prometheus: lambda config: Prometheus(config),
        ReporterTypes.Redis: lambda config: Redis(config),
        ReporterTypes.S3: lambda config: S3(config),
        ReporterTypes.Snowflake: lambda config: Snowflake(config),
        ReporterTypes.SQLite: lambda config: SQLite(config),
        ReporterTypes.StatsD: lambda config: StatsD(config),
        ReporterTypes.Telegraf: lambda config: Telegraf(config),
        ReporterTypes.TelegrafStatsD: lambda config: TelegrafStatsD(config),
        ReporterTypes.TimescaleDB: lambda config: TimescaleDB(config),
        ReporterTypes.XML: lambda config: XML(config),
    }

    def __init__(self, reporter_config: Union[ReporterConfig, ReporterType]) -> None:
        self.reporter_id = str(uuid.uuid4())
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.graph_name: str = None
        self.graph_id: str = None
        self.stage_name: str = None
        self.stage_id: str = None
        self.metadata_string: str = None
        self.thread_id = threading.current_thread().ident
        self.process_id = os.getpid()

        if reporter_config is None:
            reporter_config = JSONConfig()

        self.reporter_type = reporter_config.reporter_type

        if isinstance(self.reporter_type, ReporterTypes):
            self.reporter_type_name = self.reporter_type.name.capitalize()

        elif isinstance(self.reporter_type, str):
            self.reporter_type_name = self.reporter_type.capitalize()

        self.reporter_config = reporter_config

        selected_reporter = self.reporters.get(self.reporter_type)
        if selected_reporter is None:
            self.selected_reporter = JSON(reporter_config)

        else:
            self.selected_reporter = selected_reporter(reporter_config)

    async def connect(self):
        self.metadata_string = f"Graph - {self.graph_name}:{self.graph_id} - thread:{self.thread_id} - process:{self.process_id} - Stage: {self.stage_name}:{self.stage_id} - Reporter: {self.reporter_type_name}:{self.reporter_id} - "
        self.selected_reporter.metadata_string = self.metadata_string

        await self.logger.filesystem.aio.create_logfile("hyperscale.reporting.log")
        self.logger.filesystem.create_filelogger("hyperscale.reporting.log")

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connecting"
        )
        await self.selected_reporter.connect()

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connected"
        )

    async def submit_experiments(
        self, experiment_metrics_sets: List[ExperimentMetricsSet]
    ):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting {len(experiment_metrics_sets)} experiments"
        )

        experiment_metrics: List[ExperimentMetricsCollection] = [
            experiment.split_experiments_metrics()
            for experiment in experiment_metrics_sets
        ]

        experiments: List[Dict[str, str]] = [
            metrics_collection.experiment for metrics_collection in experiment_metrics
        ]

        variants: List[Dict[str, str]] = []
        variants_summaries: List[VariantSummary] = []
        for metrics_collection in experiment_metrics:
            variants.extend(metrics_collection.variants)
            variants_summaries.extend(metrics_collection.variant_summaries)

        mutations: List[MutationSummary] = []
        mutations_summaries: List[MutationSummary] = []
        for metrics_collection in experiment_metrics:
            mutations.extend(metrics_collection.mutations)
            mutations_summaries.extend(metrics_collection.mutation_summaries)

        experiment_metrics_collection_set = ExperimentMetricsCollectionSet(
            experiments_metrics_fields=ExperimentMetricsSet.experiments_fields(),
            variants_metrics_fields=ExperimentMetricsSet.variants_fields(),
            mutations_metrics_fields=ExperimentMetricsSet.mutations_fields(),
            experiments=experiments,
            variants=variants,
            mutations=mutations,
            experiment_summaries=[
                experiment.experiments_summary for experiment in experiment_metrics_sets
            ],
            variant_summaries=variants_summaries,
            mutation_summaries=mutations_summaries,
        )

        await self.selected_reporter.submit_experiments(
            experiment_metrics_collection_set
        )

        await self.selected_reporter.submit_variants(experiment_metrics_collection_set)

        if len(mutations) > 0:
            await self.selected_reporter.submit_mutations(
                experiment_metrics_collection_set
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted {len(experiments)} experiments"
        )

    async def submit_streams(self, stream_metrics: Dict[str, List[StreamAnalytics]]):
        streams_count = len(stream_metrics)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting {streams_count} streams"
        )
        await self.selected_reporter.submit_streams(stream_metrics)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted {streams_count} streams"
        )

    async def submit_common(self, metrics: List[Any]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting {len(metrics)} shared metrics"
        )
        await self.selected_reporter.submit_common(metrics)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted {len(metrics)} shared metrics"
        )

    async def submit_events(self, events: List[Any]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting {len(events)} events"
        )
        await self.selected_reporter.submit_events(events)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted {len(events)} events"
        )

    async def submit_metrics(self, metrics: List[Any]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting {len(metrics)} metrics"
        )
        await self.selected_reporter.submit_metrics(metrics)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted {len(metrics)} metrics"
        )

    async def submit_custom(self, metrics: List[Any]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting {len(metrics)} custom metrics"
        )
        await self.selected_reporter.submit_custom(metrics)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted {len(metrics)} custom metrics"
        )

    async def submit_errors(self, metrics: List[Any]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting {len(metrics)} errors"
        )
        await self.selected_reporter.submit_errors(metrics)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted {len(metrics)} errors"
        )

    async def submit_system_metrics(self, metrics: List[Any]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting {len(metrics)} system metrics sets"
        )
        await self.selected_reporter.submit_session_system_metrics(metrics)
        await self.selected_reporter.submit_stage_system_metrics(metrics)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted {len(metrics)} system metrics sets"
        )

    async def close(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closing"
        )
        await self.selected_reporter.close()

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closed"
        )
