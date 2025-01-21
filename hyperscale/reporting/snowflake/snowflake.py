import asyncio
import functools
import uuid

import psutil

from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

from .snowflake_config import SnowflakeConfig


try:
    import sqlalchemy
    from snowflake.sqlalchemy import URL
    from sqlalchemy import create_engine, Engine, Connection
    from sqlalchemy_utils import database_exists, create_database

    has_connector = True

except Exception:
    sqlalchemy = object

    has_connector = False

    class URL:
        pass

    class CreateTable:
        pass

    class Engine:
        pass

    class Connection:
        pass

    def database_exists(db: str):
        return False

    def create_database(db: str):
        return None

    def create_engine(*args, **kwargs):
        pass


class Snowflake:
    def __init__(self, config: SnowflakeConfig) -> None:
        self.username = config.username
        self.password = config.password
        self.organization_id = config.organization_id
        self.account_id = config.account_id
        self.private_key = config.private_key
        self.warehouse = config.warehouse
        self.database = config.database
        self.schema = config.database_schema

        self._workflow_results_table_name = config.workflow_results_table_name
        self._step_results_table_name = config.step_results_table_name

        self.connect_timeout = config.connect_timeout

        self.metadata = sqlalchemy.MetaData()

        self._engine: Engine = None
        self._connection: Connection = None

        self._workflow_results_table = sqlalchemy.Table(
            self._workflow_results_table,
            self.metadata,
            sqlalchemy.Column(
                "id",
                sqlalchemy.Integer,
                primary_key=True,
                autoincrement=True,
            ),
            sqlalchemy.Column("metric_name", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_type", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_group", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_workflow", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_value", sqlalchemy.FLOAT),
        )

        self._step_results_table = sqlalchemy.Table(
            self._step_results_table,
            self.metadata,
            sqlalchemy.Column(
                "id",
                sqlalchemy.Integer,
                primary_key=True,
                autoincrement=True,
            ),
            sqlalchemy.Column("metric_name", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_type", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_step", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_group", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_workflow", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_value", sqlalchemy.FLOAT),
        )

        self.session_uuid = str(uuid.uuid4())
        self._loop = asyncio.get_event_loop()
        self.reporter_type = ReporterTypes.Snowflake
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

        self.sql_type = "Snowflake"

    async def connect(self):
        try:
            connection_uri = URL(
                user=self.username,
                password=self.password,
                account=self.account_id,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
            )

            self._engine = await self._loop.run_in_executor(
                None,
                create_engine,
                connection_uri,
            )

            if not await self._loop.run_in_executor(
                None,
                database_exists,
                connection_uri,
            ):
                await self._loop.run_in_executor(
                    None,
                    functools.partial(
                        create_database,
                        connection_uri,
                    ),
                )

            self._connection = await asyncio.wait_for(
                self._loop.run_in_executor(None, self._engine.connect),
                timeout=self.connect_timeout,
            )

            await self._loop.run_in_executor(
                None,
                self.metadata.create_all,
                self._connection,
            )

        except asyncio.TimeoutError:
            raise Exception(
                "Err. - Connection to Snowflake timed out - check your account id, username, and password."
            )

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        await asyncio.gather(
            *[
                self._loop.run_in_executor(
                    None,
                    self._connection.execute,
                    self._workflow_results_table.insert(
                        values={
                            **result,
                            "metric_value": float(result.get("metric_value", 0)),
                        }
                    ),
                )
                for result in workflow_results
            ]
        )

    async def submit_step_results(self, step_results: StepMetricSet):
        await asyncio.gather(
            *[
                self._loop.run_in_executor(
                    None,
                    self._connection.execute,
                    self._workflow_results_table.insert(
                        values={
                            **result,
                            "metric_value": float(result.get("metric_value", 0)),
                        }
                    ),
                )
                for result in step_results
            ]
        )

    async def close(self):
        await self._loop.run_in_executor(None, self._connection.close)
