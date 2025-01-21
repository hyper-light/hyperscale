from __future__ import annotations

import os
import threading
import uuid
from .common.results_types import (
    WorkflowStats,
    ResultSet,
    MetricsSet,
    CheckSet,
    CountResults,
)
from typing import List, TypeVar, Generic

from .common import (
    ReporterTypes as ReporterTypes,
    StepMetricSet as StepMetricSet,
    StepMetricSet as StepMetricSet,
    WorkflowMetric as WorkflowMetric,
    WorkflowMetricSet as WorkflowMetricSet,
)
from .aws_lambda import AWSLambda as AWSLambda, AWSLambdaConfig as AWSLambdaConfig
from .aws_timestream import (
    AWSTimestream as AWSTimestream,
    AWSTimestreamConfig as AWSTimestreamConfig,
)
from .bigquery import BigQuery as BigQuery, BigQueryConfig as BigQueryConfig
from .bigtable import BigTable as BigTable, BigTableConfig as BigTableConfig
from .cassandra import Cassandra as Cassandra, CassandraConfig as CassandraConfig
from .cloudwatch import Cloudwatch as Cloudwatch, CloudwatchConfig as CloudwatchConfig
from .cosmosdb import CosmosDB as CosmosDB, CosmosDBConfig as CosmosDBConfig
from .csv import CSV as CSV, CSVConfig as CSVConfig
from .datadog import Datadog as Datadog, DatadogConfig as DatadogConfig
from .dogstatsd import DogStatsD as DogStatsD, DogStatsDConfig as DogStatsDConfig
from .google_cloud_storage import (
    GoogleCloudStorage as GoogleCloudStorage,
    GoogleCloudStorageConfig as GoogleCloudStorageConfig,
)
from .graphite import Graphite as Graphite, GraphiteConfig as GraphiteConfig
from .honeycomb import Honeycomb as Honeycomb, HoneycombConfig as HoneycombConfig
from .influxdb import InfluxDB as InfluxDB, InfluxDBConfig as InfluxDBConfig
from .json import JSON as JSON, JSONConfig as JSONConfig
from .kafka import Kafka as Kafka, KafkaConfig as KafkaConfig
from .mongodb import MongoDB as MongoDB, MongoDBConfig as MongoDBConfig
from .mysql import MySQL as MySQL, MySQLConfig as MySQLConfig
from .netdata import Netdata as Netdata, NetdataConfig as NetdataConfig
from .newrelic import NewRelic as NewRelic, NewRelicConfig as NewRelicConfig
from .postgres import Postgres as Postgres, PostgresConfig as PostgresConfig
from .prometheus import Prometheus as Prometheus, PrometheusConfig as PrometheusConfig
from .redis import Redis as Redis, RedisConfig as RedisConfig
from .s3 import S3 as S3, S3Config as S3Config
from .snowflake import Snowflake as Snowflake, SnowflakeConfig as SnowflakeConfig
from .sqlite import SQLite as SQLite, SQLiteConfig as SQLiteConfig
from .statsd import StatsD as StatsD, StatsDConfig as StatsDConfig
from .telegraf import Telegraf as Telegraf, TelegrafConfig as TelegrafConfig
from .telegraf_statsd import (
    TelegrafStatsD as TelegrafStatsD,
    TelegrafStatsDConfig as TelegrafStatsDConfig,
)
from .timescaledb import (
    TimescaleDB as TimescaleDB,
    TimescaleDBConfig as TimescaleDBConfig,
)
from .xml import XML as XML, XMLConfig as XMLConfig


ReporterConfig = (
    AWSLambdaConfig
    | AWSTimestreamConfig
    | BigQueryConfig
    | BigTableConfig
    | CassandraConfig
    | CloudwatchConfig
    | CosmosDBConfig
    | CSVConfig
    | DatadogConfig
    | DogStatsDConfig
    | GoogleCloudStorageConfig
    | GraphiteConfig
    | HoneycombConfig
    | InfluxDBConfig
    | JSONConfig
    | KafkaConfig
    | MongoDBConfig
    | MySQLConfig
    | NetdataConfig
    | NewRelicConfig
    | PostgresConfig
    | PrometheusConfig
    | RedisConfig
    | S3Config
    | SnowflakeConfig
    | SQLiteConfig
    | StatsDConfig
    | TelegrafConfig
    | TelegrafStatsDConfig
    | TimescaleDBConfig
    | XMLConfig
)

T = TypeVar("T")


class Reporter(Generic[T]):
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

    def __init__(self, reporter_config: T) -> None:
        self.reporter_id = str(uuid.uuid4())

        self.metadata_string: str = None
        self.thread_id = threading.current_thread().ident
        self.process_id = os.getpid()

        self.reporter_config: T = reporter_config
        self.reporter_type = self.reporter_config.reporter_type
        self.reporter_type_name = self.reporter_type.name.capitalize()

        selected_reporter = self.reporters.get(self.reporter_type)
        if selected_reporter is None:
            self.selected_reporter = JSON(reporter_config)

        else:
            self.selected_reporter = selected_reporter(reporter_config)

    async def connect(self):
        self.selected_reporter.metadata_string = self.metadata_string

        await self.selected_reporter.connect()

    async def submit_workflow_results(self, results: WorkflowStats):
        workflow_stats: CountResults = results.get("stats")

        workflow_results = [
            {
                "metric_workflow": results.get("workflow"),
                "metric_type": "COUNT",
                "metric_group": "workflow",
                "metric_name": count_name,
                "metric_value": count_value,
            }
            for count_name, count_value in workflow_stats.items()
        ]

        workflow_results.append(
            {
                "metric_workflow": results.get("workflow"),
                "metric_type": "RATE",
                "metric_group": "workflow",
                "metric_name": "rps",
                "metric_value": results.get("rps"),
            }
        )

        workflow_results.append(
            {
                "metric_workflow": results.get("workflow"),
                "metric_type": "TIMING",
                "metric_group": "workflow",
                "metric_name": "elapsed",
                "metric_value": results.get("elapsed"),
            }
        )

        await self.selected_reporter.submit_workflow_results(workflow_results)

    async def submit_step_results(self, results: WorkflowStats):
        results_set: List[ResultSet] = results.get("results", [])

        step_results = [
            {
                "metric_workflow": results_metrics.get("workflow"),
                "metric_step": results_metrics.get("step"),
                "metric_type": "DISTRIBUTION" if "quantile" in metric_name else "TIMING",
                "metric_group": timing_name,
                "metric_name": metric_name,
                "metric_value": metric_value,
            }
            for results_metrics in results_set
            for timing_name, timing_metrics in results_metrics.get(
                "timings", {}
            ).items()
            for metric_name, metric_value in timing_metrics.items()
        ]

        step_results.extend(
            [
                {
                    "metric_workflow": results_metrics.get("workflow"),
                    "metric_step": results_metrics.get("step"),
                    "metric_type": "COUNT",
                    "metric_group": "counts",
                    "metric_name": count_name,
                    "metric_value": count_metric,
                }
                for results_metrics in results_set
                for count_name, count_metric in results_metrics.get(
                    "counts", 
                    {},
                ).items()
            ]
        )

        metrics_set: List[MetricsSet] = results.get("metrics", [])

        step_results.extend(
            [
                {
                    "metric_workflow": metrics.get("workflow"),
                    "metric_step": metrics.get("step"),
                    "metric_type": "DISTRIBUTION" if "quantile" in metric_name else metrics.get(
                        "metric_type"
                    ),
                    "metric_group": "custom",
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                }
                for metrics in metrics_set
                for metric_name, metric_value in metrics.get("stats", {}).items()
            ]
        )

        step_results.extend(
            [
                {
                    "metric_workflow": metrics.get("workflow"),
                    "metric_step": metrics.get("step"),
                    "metric_type": "COUNT",
                    "metric_group": "counts",
                    "metric_name": f"status_{status_count_name}",
                    "metric_value": status_count,
                }
                for metrics in results_set
                for status_count_name, status_count in metrics.get("counts", {})
                .get("statuses")
                .items()
            ]
        )

        check_set: List[CheckSet] = results.get("checks", [])

        step_results.extend(
            [
                {
                    "metric_workflow": check_metrics.get("workflow"),
                    "metric_step": check_metrics.get("step"),
                    "metric_type": "COUNT",
                    "metric_group": "counts",
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                }
                for check_metrics in check_set
                for metric_name, metric_value in check_metrics.get("counts", {}).items() 
                if metric_name in ["succeeded", "failed", "executed"]
            ]
        )

        step_results.extend(
            [
                {
                    "metric_workflow": check_metrics.get("workflow"),
                    "metric_step": check_metrics.get("step"),
                    "metric_type": "COUNT",
                    "metric_group": "counts",
                    "metric_name": context_metric.get("context"),
                    "metric_value": context_metric.get("count"),
                }
                for check_metrics in check_set
                for context_metric in check_metrics.get("contexts", {})
            ]
        )

        await self.selected_reporter.submit_step_results(step_results)

    async def close(self):
        await self.selected_reporter.close()
