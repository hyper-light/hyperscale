import asyncio
import collections
import collections.abc
import functools
import os
import pathlib
import uuid
from typing import Dict, List, Union
from xml.dom.minidom import parseString

from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
    WorkflowMetric,
    StepMetric,
)

from .xml_config import XMLConfig

try:
    from dicttoxml import dicttoxml

    has_connector = True

except Exception:
    has_connector = False

    def dicttoxml(*args, **kwargs):
        pass


collections.Iterable = collections.abc.Iterable


MetricRecord = Dict[str, Union[int, float, str]]
MetricRecordGroup = Dict[str, List[MetricRecord]]
MetricRecordCollection = Dict[str, MetricRecord]


class XML:
    def __init__(self, config: XMLConfig) -> None:
        self._workflow_results_filepath = config.workflow_results_filepath
        self._step_results_filepath = config.step_results_filepath

        self._loop = asyncio.get_event_loop()

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.XML
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        pass

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
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
            workflow_records: Dict[str, WorkflowMetric] = {}

            for result in workflow_results:
                metric_workflow = result.get("metric_workflow")
                metric_name = result.get("metric_name")

                record_key = f"{metric_workflow}_{metric_name}"

                workflow_records[record_key] = result

            workflow_results_xml = dicttoxml(workflow_records, custom_root="system")

            workflow_results_xml = parseString(workflow_results_xml)

            await self._loop.run_in_executor(
                None,
                workflow_results_file.write,
                workflow_results_xml.toprettyxml(),
            )

        except Exception:
            pass

        await self._loop.run_in_executor(
            None,
            workflow_results_file.close
        )

    async def submit_step_results(self, step_results: StepMetricSet):
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
            step_records: Dict[str, StepMetric] = {}

            for result in step_results:
                metric_workflow = result.get("metric_workflow")
                metric_step = result.get("metric_step")
                metric_name = result.get("metric_name")

                record_key = f"{metric_workflow}_{metric_step}_{metric_name}"

                step_records[record_key] = result

            step_results_xml = dicttoxml(step_records, custom_root="system")

            step_results_xml = parseString(step_results_xml)

            await self._loop.run_in_executor(
                None,
                step_results_file.write,
                step_results_xml.toprettyxml(),
            )

        except Exception:
            pass

        await self._loop.run_in_executor(
            None,
            step_results_file.close
        )
        
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

            next_filename = f"{base_file_stem}_{filename_offset}.xml"

            filepath = await self._loop.run_in_executor(
                None, 
                os.path.join,
                  parent_dir, 
                  next_filename
            )

        return filepath

    async def close(self):
        pass
