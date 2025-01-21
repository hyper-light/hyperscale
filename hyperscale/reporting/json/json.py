from __future__ import annotations

import asyncio
import functools
import json
import os
import pathlib
import uuid
from typing import TextIO, Literal

from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)


from .json_config import JSONConfig

has_connector = True


class JSON:
    def __init__(self, config: JSONConfig) -> None:
        self._workflow_results_filepath = config.workflow_results_filepath
        self._step_results_filepath = config.step_results_filepath

        self.session_uuid = str(uuid.uuid4())
        self._loop = asyncio._get_running_loop()
        self.reporter_type = ReporterTypes.JSON
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        pass

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        filepath = await self._get_filepath(self._workflow_results_filepath)

        workflow_results_file: TextIO = await self._loop.run_in_executor(
            None,
            open,
            filepath,
            "w",
        )

        try:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    json.dump,
                    workflow_results,
                    workflow_results_file,
                    indent=4,
                ),
            )

        except Exception:
            pass

        await self._loop.run_in_executor(None, workflow_results_file.close)

    async def submit_step_results(self, step_results: StepMetricSet):
        filepath = await self._get_filepath(self._step_results_filepath)

        step_results_filepath: TextIO = await self._loop.run_in_executor(
            None,
            open,
            filepath,
            "w",
        )

        try:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    json.dump,
                    step_results,
                    step_results_filepath,
                    indent=4,
                ),
            )

        except Exception:
            pass

        await self._loop.run_in_executor(None, step_results_filepath.close)

    async def _get_filepath(self, filepath: str):
        filename_offset = 0
        base_path = await self._loop.run_in_executor(
            None,
            pathlib.Path,
            filepath,
        )

        base_file_stem = base_path.stem
        parent_dir = base_path.parent

        while await self._loop.run_in_executor(
            None,
            os.path.exists,
            filepath,
        ):
            filename_offset += 1

            next_filename = f"{base_file_stem}_{filename_offset}.json"

            filepath = await self._loop.run_in_executor(
                None, 
                os.path.join,
                  parent_dir, 
                  next_filename
            )

        return filepath

    async def close(self):
        pass
