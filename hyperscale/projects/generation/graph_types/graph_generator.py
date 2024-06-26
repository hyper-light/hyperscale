from typing import List

from hyperscale.core.hooks import depends
from hyperscale.core.hooks.types.base.registrar import registrar
from hyperscale.projects.generation.generator import Generator

from .stages import (
    AnalyzeStage,
    OptimizeStage,
    SetupStage,
)
from .stages.execute import (
    ExecuteGraphQLHttp2Stage,
    ExecuteGraphQLStage,
    ExecuteHTTP2Stage,
    ExecuteHTTP3Stage,
    ExecuteHTTPStage,
    ExecutePlaywrightStage,
    ExecuteTaskStage,
    ExecuteUDPStage,
    ExecuteWebsocketStage,
)
from .stages.submit import (
    SubmitAWSLambdaResultsStage,
    SubmitAWSTimestreamResultsStage,
    SubmitBigQueryResultsStage,
    SubmitBigTableResultsStage,
    SubmitCassandraResultsStage,
    SubmitCloudwatchResultsStage,
    SubmitCosmosDBResultsStage,
    SubmitCSVResultsStage,
    SubmitDatadogResultsStage,
    SubmitDogStatsDResultsStage,
    SubmitGoogleCloudStorageResultsStage,
    SubmitGraphiteResultsStage,
    SubmitHoneycombResultsStage,
    SubmitInfluxDBResultsStage,
    SubmitJSONResultsStage,
    SubmitKafkaResultsStage,
    SubmitMongoDBResultsStage,
    SubmitMySQLResultsStage,
    SubmitNetdataResultsStage,
    SubmitNewrelicResultsStage,
    SubmitPostgresResultsStage,
    SubmitPrometheusResultsStage,
    SubmitRedisResultsStage,
    SubmitS3ResultsStage,
    SubmitSnowflakeResultsStage,
    SubmitSQLiteResultsStage,
    SubmitStatsDResultsStage,
    SubmitTelegrafResultsStage,
    SubmitTelegrafStatsDResultsStage,
    SubmitTimescaleDBResultsStage,
)


class GraphGenerator(Generator):
    def __init__(self) -> None:
        super().__init__(
            {
                "analyze": AnalyzeStage,
                "aws-lambda": SubmitAWSLambdaResultsStage,
                "aws-timestream": SubmitAWSTimestreamResultsStage,
                "big-query": SubmitBigQueryResultsStage,
                "big-table": SubmitBigTableResultsStage,
                "cassandra": SubmitCassandraResultsStage,
                "cloudwatch": SubmitCloudwatchResultsStage,
                "cosmosdb": SubmitCosmosDBResultsStage,
                "csv": SubmitCSVResultsStage,
                "datadog": SubmitDatadogResultsStage,
                "dogstatsd": SubmitDogStatsDResultsStage,
                "google-cloud-storage": SubmitGoogleCloudStorageResultsStage,
                "graphite": SubmitGraphiteResultsStage,
                "graphql": ExecuteGraphQLStage,
                "graphql-http2": ExecuteGraphQLHttp2Stage,
                "honeycomb": SubmitHoneycombResultsStage,
                "http": ExecuteHTTPStage,
                "http2": ExecuteHTTP2Stage,
                "http3": ExecuteHTTP3Stage,
                "influxdb": SubmitInfluxDBResultsStage,
                "json": SubmitJSONResultsStage,
                "kafka": SubmitKafkaResultsStage,
                "mongodb": SubmitMongoDBResultsStage,
                "mysql": SubmitMySQLResultsStage,
                "netdata": SubmitNetdataResultsStage,
                "newrelic": SubmitNewrelicResultsStage,
                "optimize": OptimizeStage,
                "playwright": ExecutePlaywrightStage,
                "postgres": SubmitPostgresResultsStage,
                "prometheus": SubmitPrometheusResultsStage,
                "redis": SubmitRedisResultsStage,
                "s3": SubmitS3ResultsStage,
                "setup": SetupStage,
                "snowflake": SubmitSnowflakeResultsStage,
                "sqlite": SubmitSQLiteResultsStage,
                "statsd": SubmitStatsDResultsStage,
                "task": ExecuteTaskStage,
                "telegraf": SubmitTelegrafResultsStage,
                "telegraf-statsd": SubmitTelegrafStatsDResultsStage,
                "timescaledb": SubmitTimescaleDBResultsStage,
                "udp": ExecuteUDPStage,
                "websocket": ExecuteWebsocketStage,
                "depends": depends,
            },
            registrar.module_paths,
        )

        self.valid_types = [
            "analyze",
            "checkpoint",
            "execute",
            "optimize",
            "setup",
            "submit",
            "teardown",
            "validate",
        ]

    def generate_graph(
        self, stages: List[str], engine: str = None, reporter: str = None
    ):
        if engine not in self.generator_types:
            engine = "http"

        if reporter not in self.generator_types:
            reporter = "json"

        for stage in stages:
            generator_type = stage
            if stage == "execute":
                generator_type = engine

            elif stage == "submit":
                generator_type = reporter

            modules = self.gather_required_items(generator_type)

            self.collect_imports(generator_type, modules)

        self.collect_imports(None, {"depends": depends})

        self.serialize_items()

        serialized_imports = "\n".join(
            [*self.serialized_global_imports, *self.serialized_local_imports]
        )

        for idx, serialized_stage in enumerate(self.serialized_locals):
            if idx > 0:
                previous_stage_name = self.locals[idx - 1].__name__
                self.serialized_locals[idx] = (
                    f"@depends({previous_stage_name})\n{serialized_stage}"
                )

        return "\n\n".join([serialized_imports, *self.serialized_locals])
