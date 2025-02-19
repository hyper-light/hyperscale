from enum import Enum
from typing import Dict, List, Literal

from hyperscale.reporting.common.results_types import MetricType


class ReporterTypes(Enum):
    AWSLambda = "aws_lambda"
    AWSTimestream = "aws_timestream"
    BigQuery = "bigquery"
    BigTable = "bigtable"
    Cassandra = "cassandra"
    Cloudwatch = "cloudwatch"
    CosmosDB = "cosmosdb"
    CSV = "csv"
    Datadog = "datadog"
    DogStatsD = "dogstatsd"
    GCS = "gcs"
    Graphite = "graphite"
    Honeycomb = "honeycomb"
    InfluxDB = "influxdb"
    JSON = "json"
    Kafka = "kafka"
    MongoDB = "mongodb"
    MySQL = "mysql"
    Netdata = "netdata"
    NewRelic = "newrelic"
    Postgres = "postgres"
    Prometheus = "prometheus"
    Redis = "redis"
    S3 = "s3"
    Snowflake = "snowflake"
    SQLite = "sqlite"
    StatsD = "statsd"
    Telegraf = "telegraf"
    TelegrafStatsD = "telegraf_statsd"
    TimescaleDB = "timescaledb"
    XML = "xml"


WorkflowMetric = Dict[
    Literal[
        "metric_workflow", "metric_type", "metric_group", "metric_name", "metric_value"
    ],
    str | MetricType | int | float,
]


StepMetric = Dict[
    Literal[
        "metric_workflow",
        "metric_step",
        "metric_type",
        "metric_group",
        "metric_name",
        "metric_value",
    ],
    str | MetricType | int | float,
]


WorkflowMetricSet = List[WorkflowMetric]
StepMetricSet = List[StepMetric]
