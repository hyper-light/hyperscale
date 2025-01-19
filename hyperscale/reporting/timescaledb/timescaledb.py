import asyncio
from datetime import datetime
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

try:
    import sqlalchemy
    from hyperscale.reporting.types.postgres.postgres import Postgres
    from sqlalchemy.dialects.postgresql import UUID
    from sqlalchemy.schema import CreateTable

    from .timescaledb_config import TimescaleDBConfig

    has_connector = True

except Exception:
    sqlalchemy = object

    class Postgres:
        pass

    has_connector = False

    class TimescaleDBConfig:
        pass

    class CreateTable:
        pass

    class UUID:
        pass


class TimescaleDB(Postgres):
    def __init__(self, config: TimescaleDBConfig) -> None:
        super().__init__(config)

        self._metadata = sqlalchemy.MetaData()

        self._workflow_results_table = sqlalchemy.Table(
            self._workflow_results_table_name,
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
            sqlalchemy.Column(
                "insert_time",
                sqlalchemy.TIMESTAMP(timezone=False),
                nullable=False,
                default=datetime.now(),
            ),
        )

        self._step_results_table = sqlalchemy.Table(
            self._step_results_table_name,
            self._metadata,
            sqlalchemy.Column(
                "id",
                sqlalchemy.Integer,
                primary_key=True,
                autoincrement=True,
            ),
            sqlalchemy.Column("metric_name", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_step", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_workflow", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_type", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_group", sqlalchemy.VARCHAR(255)),
            sqlalchemy.Column("metric_value", sqlalchemy.FLOAT),
            sqlalchemy.Column(
                "insert_time",
                sqlalchemy.TIMESTAMP(timezone=False),
                nullable=False,
                default=datetime.now(),
            ),
        )

        self.reporter_type = ReporterTypes.TimescaleDB
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.sql_type = "TimescaleDB"

    async def connect(self):
        await super().connect()

        async with self._engine.connect() as connection:
            await connection.execute(
                f"SELECT create_hypertable('{self._workflow_results_table}', 'insert_time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
            )

            await connection.execute(
                f"CREATE INDEX ON {self._workflow_results_table} (metric_workflow, insert_time DESC);"
            )

            await connection.execute(
                f"SELECT create_hypertable('{self._step_results_table}', 'insert_time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
            )

            await connection.execute(
                f"CREATE INDEX ON {self._step_results_table} (metric_workflow, metric_step, insert_time DESC);"
            )

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
