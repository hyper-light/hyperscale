from __future__ import annotations

import importlib
import os
import threading
import uuid
from typing import Generic, List, TypeVar

from .aws_lambda import AWSLambdaConfig as AWSLambdaConfig
from .aws_timestream import (
    AWSTimestreamConfig as AWSTimestreamConfig,
)
from .bigquery import BigQueryConfig as BigQueryConfig
from .bigtable import BigTableConfig as BigTableConfig
from .cassandra import CassandraConfig as CassandraConfig
from .cloudwatch import CloudwatchConfig as CloudwatchConfig
from .common import (
    ReporterTypes as ReporterTypes,
)
from .common import StepMetricSet as StepMetricSet
from .common import WorkflowMetric as WorkflowMetric
from .common import WorkflowMetricSet as WorkflowMetricSet
from .common.results_types import (
    CheckSet,
    CountResults,
    MetricsSet,
    ResultSet,
    WorkflowStats,
)
from .cosmosdb import CosmosDBConfig as CosmosDBConfig
from .custom import CustomReporter as CustomReporter
from .csv import CSVConfig as CSVConfig
from .datadog import DatadogConfig as DatadogConfig
from .dogstatsd import DogStatsDConfig as DogStatsDConfig
from .google_cloud_storage import (
    GoogleCloudStorageConfig as GoogleCloudStorageConfig,
)
from .graphite import GraphiteConfig as GraphiteConfig
from .honeycomb import HoneycombConfig as HoneycombConfig
from .influxdb import InfluxDBConfig as InfluxDBConfig
from .json import JSON as JSON
from .json import JSONConfig as JSONConfig
from .kafka import KafkaConfig as KafkaConfig
from .mongodb import MongoDBConfig as MongoDBConfig
from .mysql import MySQLConfig as MySQLConfig
from .netdata import NetdataConfig as NetdataConfig
from .newrelic import NewRelicConfig as NewRelicConfig
from .postgres import PostgresConfig as PostgresConfig
from .prometheus import PrometheusConfig as PrometheusConfig
from .redis import RedisConfig as RedisConfig
from .s3 import S3Config as S3Config
from .snowflake import SnowflakeConfig as SnowflakeConfig
from .sqlite import SQLiteConfig as SQLiteConfig
from .statsd import StatsDConfig as StatsDConfig
from .telegraf import TelegrafConfig as TelegrafConfig
from .telegraf_statsd import (
    TelegrafStatsDConfig as TelegrafStatsDConfig,
)
from .timescaledb import (
    TimescaleDBConfig as TimescaleDBConfig,
)
from .xml import XMLConfig as XMLConfig

ReporterConfig = (
    AWSLambdaConfig
    | AWSTimestreamConfig
    | BigQueryConfig
    | BigTableConfig
    | CassandraConfig
    | CloudwatchConfig
    | CosmosDBConfig
    | CSVConfig
    | CustomReporter
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

# Each backend's reporter class, by the subpackage that defines it. A reporter
# imports its client library, so it is loaded only when one is created.
REPORTER_SUBPACKAGES: dict[str, str] = {
    "AWSLambda": "aws_lambda",
    "AWSTimestream": "aws_timestream",
    "BigQuery": "bigquery",
    "BigTable": "bigtable",
    "CSV": "csv",
    "Cassandra": "cassandra",
    "Cloudwatch": "cloudwatch",
    "CosmosDB": "cosmosdb",
    "Datadog": "datadog",
    "DogStatsD": "dogstatsd",
    "GoogleCloudStorage": "google_cloud_storage",
    "Graphite": "graphite",
    "Honeycomb": "honeycomb",
    "InfluxDB": "influxdb",
    "JSON": "json",
    "Kafka": "kafka",
    "MongoDB": "mongodb",
    "MySQL": "mysql",
    "Netdata": "netdata",
    "NewRelic": "newrelic",
    "Postgres": "postgres",
    "Prometheus": "prometheus",
    "Redis": "redis",
    "S3": "s3",
    "SQLite": "sqlite",
    "Snowflake": "snowflake",
    "StatsD": "statsd",
    "Telegraf": "telegraf",
    "TelegrafStatsD": "telegraf_statsd",
    "TimescaleDB": "timescaledb",
    "XML": "xml",
}


def load_reporter_class(class_name: str):
    """A backend's reporter class, imported with its client library on first use."""
    return getattr(
        importlib.import_module(f"{__package__}.{REPORTER_SUBPACKAGES[class_name]}"),
        class_name,
    )


def __getattr__(name: str):
    # The reporter classes this module exported when it imported them eagerly.
    if name in REPORTER_SUBPACKAGES:
        return load_reporter_class(name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


T = TypeVar("T")


class Reporter(Generic[T]):
    reporters = {
        ReporterTypes.AWSLambda: lambda config: load_reporter_class("AWSLambda")(config),
        ReporterTypes.AWSTimestream: lambda config: load_reporter_class("AWSTimestream")(config),
        ReporterTypes.BigQuery: lambda config: load_reporter_class("BigQuery")(config),
        ReporterTypes.BigTable: lambda config: load_reporter_class("BigTable")(config),
        ReporterTypes.Cassandra: lambda config: load_reporter_class("Cassandra")(config),
        ReporterTypes.Cloudwatch: lambda config: load_reporter_class("Cloudwatch")(config),
        ReporterTypes.CosmosDB: lambda config: load_reporter_class("CosmosDB")(config),
        ReporterTypes.CSV: lambda config: load_reporter_class("CSV")(config),
        ReporterTypes.Datadog: lambda config: load_reporter_class("Datadog")(config),
        ReporterTypes.DogStatsD: lambda config: load_reporter_class("DogStatsD")(config),
        ReporterTypes.GCS: lambda config: load_reporter_class("GoogleCloudStorage")(config),
        ReporterTypes.Graphite: lambda config: load_reporter_class("Graphite")(config),
        ReporterTypes.Honeycomb: lambda config: load_reporter_class("Honeycomb")(config),
        ReporterTypes.InfluxDB: lambda config: load_reporter_class("InfluxDB")(config),
        ReporterTypes.JSON: lambda config: JSON(config),
        ReporterTypes.Kafka: lambda config: load_reporter_class("Kafka")(config),
        ReporterTypes.MongoDB: lambda config: load_reporter_class("MongoDB")(config),
        ReporterTypes.MySQL: lambda config: load_reporter_class("MySQL")(config),
        ReporterTypes.Netdata: lambda config: load_reporter_class("Netdata")(config),
        ReporterTypes.NewRelic: lambda config: load_reporter_class("NewRelic")(config),
        ReporterTypes.Postgres: lambda config: load_reporter_class("Postgres")(config),
        ReporterTypes.Prometheus: lambda config: load_reporter_class("Prometheus")(config),
        ReporterTypes.Redis: lambda config: load_reporter_class("Redis")(config),
        ReporterTypes.S3: lambda config: load_reporter_class("S3")(config),
        ReporterTypes.Snowflake: lambda config: load_reporter_class("Snowflake")(config),
        ReporterTypes.SQLite: lambda config: load_reporter_class("SQLite")(config),
        ReporterTypes.StatsD: lambda config: load_reporter_class("StatsD")(config),
        ReporterTypes.Telegraf: lambda config: load_reporter_class("Telegraf")(config),
        ReporterTypes.TelegrafStatsD: lambda config: load_reporter_class("TelegrafStatsD")(config),
        ReporterTypes.TimescaleDB: lambda config: load_reporter_class("TimescaleDB")(config),
        ReporterTypes.XML: lambda config: load_reporter_class("XML")(config),
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
        workflow_stats: CountResults = results.get("stats") or {}

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
                "metric_name": "aps",
                "metric_value": results.get("aps"),
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
                "metric_type": "DISTRIBUTION"
                if "quantile" in metric_name
                else "TIMING",
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
                    "metric_type": "DISTRIBUTION"
                    if "quantile" in metric_name
                    else metrics.get("metric_type"),
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
                .get("statuses", {})
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
