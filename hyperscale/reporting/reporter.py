from __future__ import annotations

import os
import threading
import uuid
from hyperscale.core.results.workflow_types import (
    WorkflowStats, 
    ResultSet, 
    MetricsSet, 
    CheckSet,
    CountResults
)
from typing import List, TypeVar, Union


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

    def __init__(self, reporter_config: Union[ReporterType]) -> None:
        self.reporter_id = str(uuid.uuid4())

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

        await self.selected_reporter.connect()

    async def submit_workflow_results(self, results: WorkflowStats):

        workflow_stats: CountResults = results.get('stats')

        workflow_results = [
            {
                "metric_workflow": results.get('workflow'),
                "metric_type": "COUNT",
                "metric_group": "workflow",
                "metric_name": count_name,
                "metric_value": count_value,

            } for count_name, count_value in workflow_stats.items()
        ]

        workflow_results.append({
                "metric_workflow": results.get('workflow'),
                "metric_type": "RATE",
                "metric_group": "workflow",
                "metric_name": "rps",
                "metric_value": results.get('rps')
        })

        workflow_results.append({
                "metric_workflow": results.get('workflow'),
                "metric_type": "TIMING",
                "metric_group": "workflow",
                "metric_name": "elapsed",
                "metric_value": results.get('elapsed')

        })

        await self.selected_reporter.submit_workflow_results(workflow_results)

    async def submit_step_results(self, results: WorkflowStats):

        results_set: List[ResultSet] = results.get('results', [])

        step_results = [
            {
                "metric_workflow": results_metrics.get('workflow'),
                "metric_step": results_metrics.get("step"),
                "metric_type": results_metrics.get("metric_type"),
                "metric_group": timing_name,
                "metric_name": metric_name,
                "metric_value": metric_value
            }
            for results_metrics in results_set
            for timing_name, timing_metrics in results_metrics.get(
                'timings', {}
            ).items()
            for metric_name, metric_value in timing_metrics.items()
        ]

        step_results.extend([
            {
                "metric_workflow": results_metrics.get('workflow'),
                "metric_step": results_metrics.get("step"),
                "metric_type": results_metrics.get("metric_type"),
                "metric_group": "counts",
                "metric_name": count_name,
                "metric_value": count_metric,
            }
            for results_metrics in results_set
            for count_name, count_metric in results_metrics.get(
                'counts',
                {}
            )
        ])


        metrics_set: List[MetricsSet] = results.get('metrics', [])

        step_results.extend([
            {
                "metric_workflow": metrics.get('workflow'),
                "metric_step": metrics.get("step"),
                "metric_type": metrics.get("metric_type"),
                "metric_group": "custom",
                "metric_name": metric_name,
                "metric_value": metric_value,
            } for metrics in metrics_set
            for metric_name, metric_value in metrics.get(
                'stats',
                {}
            ).items()
        ])

        step_results.extend([
            {
                "metric_workflow": metrics.get('workflow'),
                "metric_step": metrics.get("step"),
                "metric_metric_type": metrics.get("metric_type"),
                "metric_group": "counts",
                "metric_name": f'status_{status_count_name}',
                "metric_value": status_count,
                

            } 
            for metrics in results_set
            for status_count_name, status_count in metrics.get(
                'counts',
                {}
            ).get(
                'statuses'
            ).items()
        ])

        check_set: List[CheckSet] = results.get('checks', [])

        step_results.extend([
            {
                "metric_workflow": check_metrics.get('workflow'),
                "metric_step": check_metrics.get("step"),
                "metric_type": check_metrics.get("metric_type"),
                "metric_group": "counts",
                "metric_name": metric_name,
                "metric_value": metric_value,
            } for check_metrics in check_set
            for metric_name, metric_value in check_metrics.get(
                "counts",
                {}
            ).items()
        ])

        step_results.extend([
            {
                "metric_workflow": check_metrics.get('workflow'),
                "metric_step": check_metrics.get("step"),
                "metric_type": check_metrics.get("metric_type"),
                "metric_group": "counts",
                "metric_name": context_metric.get('context'),
                "metric_value": context_metric.get('count'),
            } for check_metrics in check_set
            for context_metric in check_metrics.get(
                "contexts",
                {}
            )

        ])
        

        await self.selected_reporter.submit_step_results(step_results)

    async def close(self):
        await self.selected_reporter.close()
