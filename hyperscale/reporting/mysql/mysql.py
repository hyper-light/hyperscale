import asyncio
import functools
import uuid
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

from .mysql_config import MySQLConfig

try:
    import sqlalchemy

    from sqlalchemy.ext.asyncio.engine import AsyncEngine, create_async_engine
    from sqlalchemy_utils import database_exists, create_database

    has_connector = True

except Exception:
    has_connector = False

    class AsyncEngine:
        pass

    def database_exists(db: str):
        return False

    def create_database(db: str):
        return None

    async def create_async_engine(uri: str, echo: bool = False):
        pass

    class sqlalchemy:
        pass


class MySQL:
    def __init__(self, config: MySQLConfig) -> None:
        self.host = config.host
        self.port = config.port
        self.database = config.database
        self.username = config.username
        self.password = config.password

        self._workflow_results_table_ame = config.worfklow_results_table_name
        self._step_results_table_name = config.step_results_table_name

        self._metadata = sqlalchemy.MetaData()

        self._workflow_results_table = sqlalchemy.Table(
            self._workflow_results_table,
            self._metadata,
            sqlalchemy.Column(
                "id",
                sqlalchemy.Integer,
                primary_key=True,
                autoincrement=True,
            ),
            sqlalchemy.Column("metric_name", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_workflow", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_type", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_group", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_value", sqlalchemy.FLOAT),
        )

        self._step_results_table = sqlalchemy.Table(
            self._step_results_table,
            self._metadata,
            sqlalchemy.Column(
                "id",
                sqlalchemy.Integer,
                primary_key=True,
                autoincrement=True,
            ),
            sqlalchemy.Column("metric_name", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_workflow", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_step", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_type", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_group", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_value", sqlalchemy.FLOAT),
        )

        self._engine: AsyncEngine = None

        self.session_uuid = str(uuid.uuid4())
        self._loop = asyncio.get_event_loop()
        self.reporter_type = ReporterTypes.MySQL
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None
        self.sql_type = "MySQL"

    async def connect(self):
        connection_uri = "mysql+asyncmy://"

        if self.username and self.password:
            connection_uri = f"{connection_uri}{self.username}:{self.password}@"

        connection_uri = f"{connection_uri}{self.host}:{self.port}/{self.database}"
        self._engine: AsyncEngine = await create_async_engine(
            connection_uri,
            echo=False,
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

        try:
            async with self._engine.connect() as connection:
                await connection.run_sync(self._metadata.create_all)

        except Exception:
            pass

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        async with self._engine.connect() as connection:
            async with connection.begin() as transaction:
                await asyncio.gather(
                    *[
                        connection.execute(
                            self._workflow_results_table.insert(
                                values={
                                    **result,
                                    "metric_value": float(
                                        result.get("metric_value", 0)
                                    ),
                                }
                            )
                        )
                        for result in workflow_results
                    ]
                )

                await transaction.commit()

    async def submit_step_results(self, step_results: StepMetricSet):
        async with self._engine.connect() as connection:
            async with connection.begin() as transaction:
                await asyncio.gather(
                    *[
                        connection.execute(
                            self._step_results_table.insert(
                                values={
                                    **result,
                                    "metric_value": float(
                                        result.get("metric_value", 0)
                                    ),
                                }
                            )
                        )
                        for result in step_results
                    ]
                )

                await transaction.commit()

    async def close(self):
        await self._engine.dispose()
