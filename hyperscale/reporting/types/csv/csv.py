import asyncio
import csv
import functools
import uuid
from typing import TextIO, Literal

from hyperscale.reporting.types.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet
)

from .csv_config import CSVConfig

has_connector = True


class CSV:
    def __init__(self, config: CSVConfig) -> None:
        self._workflow_results_fileapth = config.workflow_results_filepath
        self._step_results_filepath = config.step_results_filepath

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None

        self._workflow_results_csv_writer: csv.DictWriter = None
        self._step_results_csv_writer: csv.DictWriter = None

        self._workflow_results_file: TextIO = None
        self._step_results_file: TextIO = None

        self.write_mode: Literal[
            'w',
            'a',
        ] = "w" if config.overwrite else "a"

        self._workflow_results_csv_headers = [
            "metric_workflow",
            "metric_type",
            "metric_group",
            "metric_name",
            "metric_value"
        ]

        self._step_results_csv_headers = [
            "metric_workflow",
            "metric_step",
            "metric_type",
            "metric_group",
            "metric_name",
            "metric_value"
        ]

        self._loop = asyncio._get_running_loop()
        self.reporter_type = ReporterTypes.CSV
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None


    async def connect(self):
        pass

    async def submit_workflow_results(
        self, 
        workflow_results: WorkflowMetricSet,
    ):

        self._workflow_results_file = await self._loop.run_in_executor(
            None, 
            functools.partial(
                open, 
                self._workflow_results_fileapth, 
                self.write_mode,
            ),
        )

        if self._workflow_results_csv_writer is None or self.write_mode == 'w':
            self._workflow_results_csv_writer = csv.DictWriter(
                self._workflow_results_file, 
                fieldnames=self._workflow_results_csv_headers,
            )

            await self._loop.run_in_executor(
                None,
                self._workflow_results_csv_writer.writeheader,
            )

        for result in workflow_results:
            await self._loop.run_in_executor(
                None,
                self._workflow_results_csv_writer.writerow,
                result,
            )

        await self._loop.run_in_executor(None, self._workflow_results_file.close)

    async def submit_step_results(
        self, 
        step_results: StepMetricSet,
    ):

        self._step_results_file = await self._loop.run_in_executor(
            None, 
            functools.partial(
                open, 
                self._step_results_filepath, 
                self.write_mode,
            ),
        )

        if self._step_results_csv_writer is None or self.write_mode == 'w':
            self._step_results_csv_writer = csv.DictWriter(
                self._step_results_file, 
                fieldnames=self._step_results_csv_headers,
            )

            await self._loop.run_in_executor(
                None,
                self._workflow_results_csv_writer.writeheader,
            )

        for result in step_results:
            await self._loop.run_in_executor(
                None,
                self._step_results_csv_writer.writerow,
                result,
            )

        await self._loop.run_in_executor(None, self._step_results_file.close)

    async def close(self):
        if self._workflow_results_file and not self._workflow_results_file.closed:
            await self._loop.run_in_executor(
                None,
                self._workflow_results_file.close,
            )

        if self._step_results_file and self._step_results_file.closed:
            await self._loop.run_in_executor(
                None,
                self._step_results_file.close,
            )
