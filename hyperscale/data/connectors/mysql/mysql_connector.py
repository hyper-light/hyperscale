import asyncio
import uuid
import warnings
from typing import Any, Coroutine, Dict, List

from hyperscale.core.engines.client.config import Config
from hyperscale.core.engines.types.common.results_set import ResultsSet
from hyperscale.core.hooks.types.action.hook import ActionHook
from hyperscale.data.connectors.common.connector_type import ConnectorType
from hyperscale.data.connectors.common.execute_stage_summary_validator import (
    ExecuteStageSummaryValidator,
)
from hyperscale.data.parsers.parser import Parser
from hyperscale.logging.hyperscale_logger import HyperscaleLogger

from .mysql_connector_config import MySQLConnectorConfig

try:
    # Aiomysql will raise warnings if a table exists despite us
    # explicitly passing "IF NOT EXISTS", so we're going to
    # ignore them.
    import aiomysql
    import sqlalchemy as sa

    warnings.filterwarnings("ignore", category=aiomysql.Warning)

    from aiomysql.sa import SAConnection, create_engine
    from aiomysql.sa.result import RowProxy

    has_connector = True

except Exception:
    SAConnection = object
    RowProxy = object
    sqlalchemy = object
    sa = object
    create_engine = object
    CreateTable = object
    OperationalError = object
    has_connector = object


class MySQLConnector:
    connection_type = ConnectorType.MySQL

    def __init__(
        self,
        config: MySQLConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        self.host = config.host
        self.database = config.database
        self.username = config.username
        self.password = config.password
        self.stage = stage
        self.parser_config = parser_config

        self.table_name = config.table_name
        self._table = None

        self.metadata = sa.MetaData()
        self._engine = None
        self._connection = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.parser = Parser()

    async def connect(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connecting to MySQL instance at - {self.host} - Database: {self.database}"
        )
        self._engine = await create_engine(
            db=self.database, host=self.host, user=self.username, password=self.password
        )

        self._connection: SAConnection = await self._engine.acquire()

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connected to MySQL instance at - {self.host} - Database: {self.database}"
        )

    async def load_execute_stage_summary(
        self, options: Dict[str, Any] = {}
    ) -> Coroutine[Any, Any, ExecuteStageSummaryValidator]:
        execute_stage_summary = await self.load_data(options=options)

        return ExecuteStageSummaryValidator(**execute_stage_summary)

    async def load_actions(
        self, options: Dict[str, Any] = {}
    ) -> Coroutine[Any, Any, List[ActionHook]]:
        actions = await self.load_data(options=options)

        return await asyncio.gather(
            *[
                self.parser.parse_action(
                    action_data, self.stage, self.parser_config, options
                )
                for action_data in actions
            ]
        )

    async def load_results(
        self, options: Dict[str, Any] = {}
    ) -> Coroutine[Any, Any, ResultsSet]:
        results = await self.load_data(options=options)

        return ResultsSet(
            {
                "stage_results": await asyncio.gather(
                    *[
                        self.parser.parse_result(
                            results_data, self.stage, self.parser_config, options
                        )
                        for results_data in results
                    ]
                )
            }
        )

    async def load_data(
        self, options: Dict[str, Any] = {}
    ) -> Coroutine[Any, Any, List[Dict[str, Any]]]:
        if self._table is None:
            self._table = sa.Table(
                self.table_name, self.metadata, autoload_with=self._engine
            )

        rows: List[RowProxy] = [
            row async for row in self._connection.execute(self._table.select(**options))
        ]

        return [{column: value for column, value in result.items()} for result in rows]

    async def close(self):
        await self._connection.close()
