import uuid
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

from .influxdb_config import InfluxDBConfig

try:
    from influxdb_client import Point, WriteApi
    from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

    has_connector = True

except Exception:
    Point = None
    has_connector = False

    class WriteApi:
        pass

    class Point:
        pass

    class InfluxDBClientAsync:
        pass


class InfluxDB:
    def __init__(self, config: InfluxDBConfig) -> None:
        self.host = config.host
        self.port = config.port
        self.token = config.token
        self.protocol = "https" if config.secure else "http"

        self.organization = config.organization
        self.connect_timeout = config.connect_timeout

        self._workflow_results_bucket_name = config.workflow_results_bucket_name
        self._step_results_bucket_name = config.step_results_bucket_name

        self.client: InfluxDBClientAsync = None
        self.write_api: WriteApi = None

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.InfluxDB
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        self.client = InfluxDBClientAsync(
            f"{self.protocol}://{self.host}:{self.port}",
            token=self.token,
            org=self.organization,
            timeout=self.connect_timeout,
        )

        self.write_api = self.client.write_api()

    async def submit_workflow_results(
        self,
        workflow_results: WorkflowMetricSet,
    ):
        points = []
        for result in workflow_results:
            metric_workflow = result.get("metric_workflow")
            metric_name = result.get("metric_name")

            point = Point(f"{metric_workflow}_{metric_name}")

            for field, value in result.items():
                point.field(field, value)

            points.append(point)

        await self.write_api.write(
            bucket=self._workflow_results_bucket_name,
            record=points,
        )

    async def submit_metrics(self, step_results: StepMetricSet):
        points = []
        for result in step_results:
            metric_workflow = result.get("metric_workflow")
            metric_step = result.get("metric_step")
            metric_name = result.get("metric_name")

            point = Point(f"{metric_workflow}_{metric_step}_{metric_name}")

            for field, value in result.items():
                point.field(field, value)

            points.append(point)

        await self.write_api.write(
            bucket=self._workflow_results_bucket_name,
            record=points,
        )

    async def close(self):
        await self.client.close()
