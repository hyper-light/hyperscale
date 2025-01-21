import asyncio
import csv
import pathlib
import os
import functools
import uuid

from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

from .csv_config import CSVConfig

has_connector = True


class CSV:
    def __init__(self, config: CSVConfig) -> None:
        self._workflow_results_filepath = config.workflow_results_filepath
        self._step_results_filepath = config.step_results_filepath

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None

        self._workflow_results_csv_headers = [
            "metric_workflow",
            "metric_type",
            "metric_group",
            "metric_name",
            "metric_value",
        ]

        self._step_results_csv_headers = [
            "metric_workflow",
            "metric_step",
            "metric_type",
            "metric_group",
            "metric_name",
            "metric_value",
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
        filepath = await self._get_filepath(self._workflow_results_filepath)

        workflow_results_file = await self._loop.run_in_executor(
            None,
            functools.partial(
                open,
                filepath,
                "w",
            ),
        )

        try:
            workflow_results_csv_writer = csv.DictWriter(
                workflow_results_file,
                fieldnames=self._workflow_results_csv_headers,
            )

            await self._loop.run_in_executor(
                None,
                workflow_results_csv_writer.writeheader,
            )
            
            await self._loop.run_in_executor(
                None,
                workflow_results_csv_writer.writerows,
                workflow_results,
            )

        except Exception:
            pass

        await self._loop.run_in_executor(None, workflow_results_file.close)

    async def submit_step_results(
        self,
        step_results: StepMetricSet,
    ):
        filepath = await self._get_filepath(self._step_results_filepath)

        step_results_file = await self._loop.run_in_executor(
            None,
            functools.partial(
                open,
                filepath,
                "w",
            ),
        )

        try:
            step_results_csv_writer = csv.DictWriter(
                step_results_file,
                fieldnames=self._step_results_csv_headers,
            )

            await self._loop.run_in_executor(
                None,
                step_results_csv_writer.writeheader,
            )

            await self._loop.run_in_executor(
                None,
                step_results_csv_writer.writerows,
                step_results,
            )

        except Exception:
            pass

        await self._loop.run_in_executor(None, step_results_file.close)

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

            next_filename = f"{base_file_stem}_{filename_offset}.csv"

            filepath = await self._loop.run_in_executor(
                None, 
                os.path.join,
                  parent_dir, 
                  next_filename
            )

        return filepath

    async def close(self):
        pass
