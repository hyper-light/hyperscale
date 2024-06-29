import asyncio
import collections
import collections.abc
import functools
import os
import pathlib
import signal
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Coroutine, Dict, List, TextIO, Union

import psutil

from hyperscale.core.engines.client.config import Config
from hyperscale.core.engines.types.common.results_set import ResultsSet
from hyperscale.core.hooks.types.action.hook import ActionHook
from hyperscale.data.connectors.common.connector_type import ConnectorType
from hyperscale.data.connectors.common.execute_stage_summary_validator import (
    ExecuteStageSummaryValidator,
)
from hyperscale.data.parsers.parser import Parser
from hyperscale.logging.hyperscale_logger import HyperscaleLogger

from .xml_connector_config import XMLConnectorConfig

try:
    import xmltodict
except Exception:
    xmltodict = object


collections.Iterable = collections.abc.Iterable


def handle_loop_stop(
    signame,
    executor: ThreadPoolExecutor,
    loop: asyncio.AbstractEventLoop,
    events_file: TextIO,
):
    try:
        events_file.close()
        executor.shutdown(wait=False, cancel_futures=True)
        loop.stop()
    except Exception:
        pass


class XMLConnector:
    connector_type = ConnectorType.XML

    def __init__(
        self,
        config: XMLConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop: asyncio.AbstractEventLoop = None
        self.stage = stage
        self.parser_config = parser_config

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.filepath: str = config.filepath

        self.xml_file: Union[TextIO, None] = None

        self.file_mode = config.file_mode
        self.parser = Parser()

    async def connect(self):
        self._loop = asyncio._get_running_loop()
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Setting filepaths"
        )

        if self.filepath[:2] == "~/":
            user_directory = pathlib.Path.home()
            self.filepath = os.path.join(user_directory, self.filepath[2:])

        self.filepath = await self._loop.run_in_executor(
            self._executor, functools.partial(os.path.abspath, self.filepath)
        )

        if self.xml_file is None:
            self.xml_file = await self._loop.run_in_executor(
                self._executor, functools.partial(open, self.filepath, self.file_mode)
            )

            for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
                self._loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame, self._executor, self._loop, self.xml_file
                    ),
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Opening from file - {self.filepath}"
        )

    async def load_execute_stage_summary(
        self, options: Dict[str, Any] = {}
    ) -> Coroutine[Any, Any, ExecuteStageSummaryValidator]:
        execute_stage_summary = await self.load_data(options=options)

        return ExecuteStageSummaryValidator(**execute_stage_summary)

    async def load_actions(
        self, options: Dict[str, Any] = {}
    ) -> Coroutine[Any, Any, List[ActionHook]]:
        actions: List[Dict[str, Any]] = await self.load_data(options=options)

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
        file_data = await self._loop.run_in_executor(self._executor, self.xml_file.read)

        return await self._loop.run_in_executor(
            self._executor, functools.partial(xmltodict.parse, file_data)
        )

    async def close(self):
        await self._loop.run_in_executor(self._executor, self.xml_file.close)

        self._executor.shutdown(cancel_futures=True)
