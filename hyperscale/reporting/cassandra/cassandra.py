import asyncio
import functools
import os
import uuid


from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

from .cassandra_config import CassandraConfig


try:
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
    from cassandra.cqlengine import columns, connection
    from cassandra.cqlengine.management import sync_table
    from cassandra.cqlengine.models import Model

    has_connector = True

except Exception:
    has_connector = False
    columns = object
    connection = object

    class PlainTextAuthProvider:
        pass

    class Cluster:
        pass

    class Model:
        pass

    def sync_table(*args, **kwargs):
        pass


class Cassandra:
    def __init__(self, config: CassandraConfig) -> None:
        self.cluster = None
        self.session = None

        self.hosts = config.hosts
        self.port = config.port

        self.username = config.username
        self.password = config.password
        self.keyspace = config.keyspace

        self._workflow_results_table_name = config.workflow_results_table_name
        self._step_results_table_name = config.step_results_table_name

        self.replication_strategy = config.replication_strategy
        self.replication = config.replication
        self.ssl = config.ssl

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None

        self._workflow_results_table = None
        self._step_results_table = None

        self._loop = asyncio.get_event_loop()
        self.reporter_type = ReporterTypes.Cassandra
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        auth = None
        if self.username and self.password:
            auth = PlainTextAuthProvider(self.username, self.password)

        self.cluster = Cluster(
            self.hosts,
            port=self.port,
            auth_provider=auth,
            ssl_context=self.ssl,
        )

        self.session = await self._loop.run_in_executor(None, self.cluster.connect)

        keyspace_options = f"'class' : '{self.replication_strategy}', 'replication_factor' : {self.replication}"
        keyspace_query = (
            f"CREATE KEYSPACE IF NOT EXISTS {self.keyspace} WITH REPLICATION = "
            + "{"
            + keyspace_options
            + "};"
        )

        await self._loop.run_in_executor(
            None,
            self.session.execute,
            keyspace_query,
        )

        await self._loop.run_in_executor(
            None,
            self.session.set_keyspace,
            self.keyspace,
        )

        allow_schema_management = await self._loop.run_in_executor(
            None,
            os.getenv,
            "CQLENG_ALLOW_SCHEMA_MANAGEMENT",
        )

        if allow_schema_management is None:
            await self._loop.run_in_executor(
                None,
                os.environ.__setitem__,
                "CQLENG_ALLOW_SCHEMA_MANAGEMENT",
                "1",
            )

        await self._loop.run_in_executor(
            None,
            functools.partial(
                connection.setup,
                self.hosts,
                self.keyspace,
                protocol_version=3,
            ),
        )

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        if self._workflow_results_table is None:
            fields = {
                "id": columns.UUID(primary_key=True, default=uuid.uuid4),
                "metric_name": columns.Text(min_length=1),
                "metric_group": columns.Text(min_length=1),
                "metric_type": columns.Text(min_length=1),
                "metric_workflow": columns.Text(min_length=1, index=True),
                "metric_value": columns.Float(),
            }

            self._workflow_results_table = type(
                self._workflow_results_table_name.capitalize(),
                (Model,),
                fields,
            )

            await self._loop.run_in_executor(
                None,
                functools.partial(
                    sync_table,
                    self._workflow_results_table,
                    keyspaces=[self.keyspace],
                ),
            )

        for result in workflow_results:
            field_value = result.get("metric_value")
            if isinstance(field_value, int):
                result["metric_value"] = float(field_value)

        await asyncio.gather(
            *[
                self._loop.run_in_executor(
                    None,
                    functools.partial(
                        self._workflow_results_table.create,
                        **result,
                    ),
                )
                for result in workflow_results
            ]
        )

    async def submit_step_results(self, step_results: StepMetricSet):
        if self._step_results_table is None:
            fields = {
                "id": columns.UUID(primary_key=True, default=uuid.uuid4),
                "metric_name": columns.Text(min_length=1),
                "metric_group": columns.Text(min_length=1),
                "metric_type": columns.Text(min_length=1),
                "metric_step": columns.Text(min_length=1, index=True),
                "metric_workflow": columns.Text(min_length=1, index=True),
                "metric_value": columns.Float(),
            }

            self._step_results_table = type(
                self._step_results_table_name.capitalize(),
                (Model,),
                fields,
            )

            await self._loop.run_in_executor(
                None,
                functools.partial(
                    sync_table,
                    self._step_results_table,
                    keyspaces=[self.keyspace],
                ),
            )

        for result in step_results:
            field_value = result.get("metric_value")
            if isinstance(field_value, int):
                result["metric_value"] = float(field_value)

        await asyncio.gather(
            *[
                self._loop.run_in_executor(
                    None,
                    functools.partial(
                        self._step_results_table.create,
                        **result,
                    ),
                )
                for result in step_results
            ]
        )

    async def close(self):
        await self._loop.run_in_executor(None, self.cluster.shutdown)
