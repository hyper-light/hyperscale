import asyncio
import functools
import os
import signal
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List

import psutil


from hyperscale.reporting.metric import MetricsSet, MetricType

from .cassandra_config import CassandraConfig


def noop_sync_table():
    pass


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


def handle_loop_stop(
    signame, executor: ThreadPoolExecutor, loop: asyncio.AbstractEventLoop
):
    try:
        executor.shutdown(wait=False, cancel_futures=True)
        loop.stop()
    except Exception:
        pass


class Cassandra:
    def __init__(self, config: CassandraConfig) -> None:
        self.cluster = None
        self.session = None

        self.hosts = config.hosts
        self.port = config.port or 9042

        self.username = config.username
        self.password = config.password
        self.keyspace = config.keyspace

        self.events_table_name: str = config.events_table
        self.metrics_table_name: str = config.metrics_table
        self.streams_table_name: str = config.streams_table

        self.experiments_table_name: str = config.experiments_table
        self.variants_table_name: str = f"{config.experiments_table}_variants"
        self.mutations_table_name: str = f"{config.experiments_table}_mutations"

        self.shared_metrics_table_name = f"{config.metrics_table}_shared"
        self.custom_metrics_table_name = f"{config.metrics_table}_custom"
        self.errors_table_name = f"{config.metrics_table}_errors"

        self.session_system_metrics_table_name = (
            f"{config.system_metrics_table}_session"
        )
        self.stage_system_metrics_table_name = f"{config.system_metrics_table}_stage"

        self.replication_strategy = config.replication_strategy
        self.replication = config.replication
        self.ssl = config.ssl

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self._metrics_table = None
        self._errors_table = None
        self._events_table = None
        self._streams_table = None

        self._experiments_table = None
        self._variants_table = None
        self._mutations_table = None

        self._session_system_metrics_table = None
        self._stage_system_metrics_table = None

        self._shared_metrics_table = None
        self._custom_metrics_table = None

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame, self._executor, self._loop
                ),
            )

        host_port_combinations = ", ".join(
            [f"{host}:{self.port}" for host in self.hosts]
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Opening amd authorizing connection to Cassandra Cluster at - {host_port_combinations}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Opening session - {self.session_uuid}"
        )

        auth = None
        if self.username and self.password:
            auth = PlainTextAuthProvider(self.username, self.password)

        self.cluster = Cluster(
            self.hosts, port=self.port, auth_provider=auth, ssl_context=self.ssl
        )

        self.session = await self._loop.run_in_executor(None, self.cluster.connect)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connected to Cassandra Cluster at - {host_port_combinations}"
        )

        if self.keyspace is None:
            self.keyspace = "hyperscale"

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Creating Keyspace - {self.keyspace}"
        )

        keyspace_options = f"'class' : '{self.replication_strategy}', 'replication_factor' : {self.replication}"
        keyspace_query = (
            f"CREATE KEYSPACE IF NOT EXISTS {self.keyspace} WITH REPLICATION = "
            + "{"
            + keyspace_options
            + "};"
        )

        await self._loop.run_in_executor(None, self.session.execute, keyspace_query)

        await self._loop.run_in_executor(None, self.session.set_keyspace, self.keyspace)
        if os.getenv("CQLENG_ALLOW_SCHEMA_MANAGEMENT") is None:
            os.environ["CQLENG_ALLOW_SCHEMA_MANAGEMENT"] = "1"

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                connection.setup, self.hosts, self.keyspace, protocol_version=3
            ),
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Created Keyspace - {self.keyspace}"
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Metrics to - Keyspace: {self.keyspace} - Table: {self.metrics_table_name}"
        )

        for metrics_set in metrics:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            if self._metrics_table is None:
                await self.logger.filesystem.aio["hyperscale.reporting"].info(
                    f"{self.metadata_string} - Creating Metrics table - {self.metrics_table_name} - under keyspace - {self.keyspace}"
                )

                fields = {
                    "id": columns.UUID(primary_key=True, default=uuid.uuid4),
                    "name": columns.Text(min_length=1, index=True),
                    "stage": columns.Text(min_length=1),
                    "group": columns.Text(),
                    "median": columns.Float(),
                    "mean": columns.Float(),
                    "variance": columns.Float(),
                    "stdev": columns.Float(),
                    "minimum": columns.Float(),
                    "maximum": columns.Float(),
                    "created_at": columns.DateTime(default=datetime.now),
                }

                for quantile_name in metrics_set.quantiles:
                    fields[quantile_name] = columns.Float()

                for custom_field_name, custom_field_type in metrics_set.custom_schemas:
                    fields[custom_field_name] = custom_field_type

                self._metrics_table = type(
                    self.metrics_table_name.capitalize(), (Model,), fields
                )

                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        sync_table, self._metrics_table, keyspaces=[self.keyspace]
                    ),
                )

                await self.logger.filesystem.aio["hyperscale.reporting"].info(
                    f"{self.metadata_string} - Created Metrics table - {self.metrics_table_name} - under keyspace - {self.keyspace}"
                )

            for group_name, group in metrics_set.groups.items():
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Metrics Group - {group_name}:{group.metrics_group_id}"
                )
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self._metrics_table.create,
                        **{**group.record, "group": group_name},
                    ),
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Metrics Set to - Keyspace: {self.keyspace} - Table: {self.metrics_table_name}"
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Custom Metrics Set to - Keyspace: {self.keyspace} - Table: {self.metrics_table_name}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Submitting Custom Metrics Group - Custom"
        )

        if self._custom_metrics_table is None:
            await self.logger.filesystem.aio["hyperscale.reporting"].info(
                f"{self.metadata_string} - Creating Custom Metrics table - {self.custom_metrics_table_name} - under keyspace - {self.keyspace}"
            )

            custom_table = {
                "id": columns.UUID(primary_key=True, default=uuid.uuid4),
                "name": columns.Text(min_length=1, index=True),
                "stage": columns.Text(min_length=1),
                "group": columns.Text(min_length=1),
            }

            for metrics_set in metrics_sets:
                for (
                    custom_metric_name,
                    custom_metric,
                ) in metrics_set.custom_metrics.items():
                    cassandra_column = columns.Integer()

                    if custom_metric.metric_type == MetricType.COUNT:
                        cassandra_column = columns.Integer()

                    else:
                        cassandra_column = columns.Float()

                    custom_table[custom_metric_name] = cassandra_column
                    custom_table[custom_metric_name] = cassandra_column

            self._custom_metrics_table: Model = type(
                self.custom_metrics_table_name.capitalize(), (Model,), custom_table
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    sync_table, self._custom_metrics_table, keyspaces=[self.keyspace]
                ),
            )

            await self.logger.filesystem.aio["hyperscale.reporting"].info(
                f"{self.metadata_string} - Created Custom Metrics table - {self.custom_metrics_table_name} - under keyspace - {self.keyspace}"
            )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self._custom_metrics_table.create,
                    {
                        "name": metrics_set.name,
                        "stage": metrics_set.stage,
                        "group": "Custom",
                        **{
                            custom_metric_name: custom_metric.metric_value
                            for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                        },
                    },
                ),
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Custom Metrics Set to - Keyspace: {self.keyspace} - Table: {self.metrics_table_name}"
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Errors Metrics to - Keyspace: {self.keyspace} - Table: {self.errors_table_name}"
        )

        if self._errors_table is None:
            await self.logger.filesystem.aio["hyperscale.reporting"].info(
                f"{self.metadata_string} - Creating Errors Metrics table - {self.errors_table_name} - under keyspace - {self.keyspace}"
            )

            errors_table_fields = {
                "id": columns.UUID(primary_key=True, default=uuid.uuid4),
                "name": columns.Text(min_length=1, index=True),
                "stage": columns.Text(min_length=1),
                "error_message": columns.Text(min_length=1),
                "error_count": columns.Integer(),
            }

            self._errors_table = type(
                self.errors_table_name.capitalize()(
                    Model,
                ),
                errors_table_fields,
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    sync_table, self._errors_table, keyspaces=[self.keyspace]
                ),
            )

            await self.logger.filesystem.aio["hyperscale.reporting"].info(
                f"{self.metadata_string} - Created Errors Metrics table - {self.errors_table_name} - under keyspace - {self.keyspace}"
            )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Errors Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for error in metrics_set.errors:
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self._errors_table.create,
                        {
                            "name": metrics_set.name,
                            "stage": metrics_set.stage,
                            "error_message": error.get("message"),
                            "error_count": error.get("count"),
                        },
                    ),
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Errors Metrics to - Keyspace: {self.keyspace} - Table: {self.errors_table_name}"
        )

    async def close(self):
        host_port_combinations = ", ".join(
            [f"{host}:{self.port}" for host in self.hosts]
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Closing session - {self.session_uuid}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closing connection to Cassandra Cluster at - {host_port_combinations}"
        )

        await self._loop.run_in_executor(self._executor, self.cluster.shutdown)

        self._executor.shutdown(cancel_futures=True)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closed connection to Cassandra Cluster at - {host_port_combinations}"
        )
