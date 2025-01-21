import asyncio
import uuid
from datetime import datetime
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)
from .bigtable_config import BigTableConfig

try:
    from google.auth import load_credentials_from_file
    from google.cloud import bigtable

    has_connector = True

except Exception:
    has_connector = False

    bigtable = object

    def load_credential_from_file(*args, **kwargs):
        pass


class BigTable:
    def __init__(self, config: BigTableConfig) -> None:
        self.service_account_json_path = config.service_account_json_path

        self.instance_id = config.instance_id
        self._workflow_results_table_id = config.workflow_results_table_id
        self._step_results_table_id = config.step_results_table_id

        self._workflow_results_column_family_id = (
            f"{self._workflow_results_table_id}_columns"
        )
        self._step_results_column_family_id = f"{self._step_results_table_id}_columns"

        self.instance = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None

        self._workflow_results_table = None
        self._step_results_table = None

        self._workflow_results_columns = None
        self._step_results_columns = None

        self.credentials = None
        self.client = None
        self._loop = asyncio.get_event_loop()
        self.reporter_type = ReporterTypes.BigTable
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        credentials, project_id = load_credentials_from_file(
            self.service_account_json_path
        )
        self.client = bigtable.Client(
            project=project_id, credentials=credentials, admin=True
        )
        self.instance = self.client.instance(self.instance_id)

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        self._workflow_results_table = self.instance.table(
            self._workflow_results_table_id
        )

        try:
            await self._loop.run_in_executor(
                None,
                self._workflow_results_table.create,
            )

        except Exception:
            pass

        self._workflow_results_columns = self._workflow_results_table.column_family(
            self._workflow_results_column_family_id,
        )

        try:
            await self._loop.run_in_executor(
                None,
                self._workflow_results_columns.create,
            )

        except Exception:
            pass

        rows = []
        for result in workflow_results:
            row_key = f"{self._workflow_results_table_id}_{str(uuid.uuid4())}"
            row = self._workflow_results_table.direct_row(row_key)

            for field, value in result.items():
                row.set_cell(
                    self._workflow_results_column_family_id,
                    field,
                    f"{value}".encode(),
                    timestamp=datetime.now(),
                )

            rows.append(row)

        await self._loop.run_in_executor(
            None,
            self._workflow_results_table.mutate_rows,
            rows,
        )

    async def submit_metrics(self, step_results: StepMetricSet):
        self._step_results_table = self.instance.table(self.metrics_table_id)

        try:
            await self._loop.run_in_executor(None, self._step_results_table.create)

        except Exception:
            pass

        self._step_results_columns = self._step_results_table.column_family(
            self._step_results_column_family_id
        )

        try:
            await self._loop.run_in_executor(
                None,
                self._step_results_columns.create,
            )

        except Exception:
            pass

        rows = []
        for result in step_results:
            row_key = f"{self._step_results_table_id}_{str(uuid.uuid4())}"
            row = self._step_results_table.direct_row(row_key)

            for field, value in result.items():
                row.set_cell(
                    self._step_results_column_family_id,
                    field,
                    f"{value}".encode(),
                    timestamp=datetime.now(),
                )

            rows.append(row)

    async def close(self):
        await self._loop.run_in_executor(None, self.client.close)
