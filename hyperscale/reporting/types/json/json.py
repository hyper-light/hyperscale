from __future__ import annotations

import asyncio
import functools
import json
import os
import re
import signal
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, TextIO, Union

import psutil


from hyperscale.reporting.metric import MetricsSet

from .json_config import JSONConfig

has_connector = True


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


class JSON:
    def __init__(self, config: JSONConfig) -> None:
        self.events_filepath = config.events_filepath
        self.metrics_filepath = config.metrics_filepath
        self.experiments_filepath = config.experiments_filepath
        self.streams_filepath = config.streams_filepath
        self.system_metrics_filepath = config.system_metrics_filepath

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop: asyncio.AbstractEventLoop = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.events_file: TextIO = None
        self.experiments_file: TextIO = None
        self.metrics_file: TextIO = None
        self.streams_file: TextIO = None
        self.system_metrics_file: TextIO = None

        self.write_mode = "w" if config.overwrite else "a"
        self.pattern = re.compile("_copy[0-9]+")

    async def connect(self):
        self._loop = asyncio._get_running_loop()
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Skipping connect"
        )

        original_filepath = Path(self.events_filepath)

        directory = original_filepath.parent
        filename = original_filepath.stem

        events_file_timestamp = time.time()
        self.events_filepath = os.path.join(
            directory, f"{filename}_{events_file_timestamp}.json"
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Skipping Custom Metrics"
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Skipping Error Metrics"
        )

    async def close(self):
        if self.events_file:
            await self._loop.run_in_executor(self._executor, self.events_file.close)

        if self.experiments_file:
            await self._loop.run_in_executor(
                self._executor, self.experiments_file.close
            )

        if self.metrics_file:
            await self._loop.run_in_executor(self._executor, self.metrics_file.close)

        if self.streams_file:
            await self._loop.run_in_executor(self._executor, self.streams_file.close)

        if self.system_metrics_file:
            await self._loop.run_in_executor(
                self._executor, self.system_metrics_file.close
            )

        self._executor.shutdown(wait=False, cancel_futures=True)

        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Closing session - {self.session_uuid}"
        )
