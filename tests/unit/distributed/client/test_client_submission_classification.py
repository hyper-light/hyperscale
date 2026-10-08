"""
How a client classifies what it submits and what it is told.

* A manager with no workers registered yet (the cluster is still booting)
  rejects a job with "No workers registered"; that rejection was not
  retryable, so a client submitting during boot failed outright. It is
  now transient: the client retries with backoff.
* A manager whose datacenter has not formed its cluster membership yet
  (AD-52: at boot, or while a cluster that could no longer commit is
  founded anew) rejects a job until it has; that rejection is transient.
* File reporters (JSON, CSV, XML) on a workflow are written by the
  client. They were matched by name against the reporter type's Enum,
  which never matched, so every client wrote default JSON files to its
  working directory instead. They are now matched by type; a reporter
  the cluster writes is not taken.
"""

from types import SimpleNamespace

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.client.models.client_config import ClientConfig
from hyperscale.distributed.nodes.client.submission import ClientJobSubmitter
from hyperscale.distributed.protocol.transient_errors import is_transient_rejection
from hyperscale.reporting.common import ReporterTypes
from hyperscale.reporting.csv.csv_config import CSVConfig
from hyperscale.reporting.json.json_config import JSONConfig

NO_WORKERS_REJECTION = "No workers registered in this datacenter; rejecting job submission"
MEMBERSHIP_NOT_FORMED_REJECTION = "Cluster membership not formed yet; retry"


class LogicalIds:
    def __init__(self) -> None:
        self._next = 0

    def generate(self, prefix: str) -> str:
        self._next += 1
        return f"{prefix}-{self._next}"


def make_submitter() -> ClientJobSubmitter:
    submitter = object.__new__(ClientJobSubmitter)
    submitter._config = ClientConfig.from_env(Env(), host="127.0.0.1", tcp_port=8500, managers=[], gates=[])
    submitter._logical_id_generator = LogicalIds()
    return submitter


def test_a_manager_without_workers_yet_is_a_transient_rejection() -> None:
    assert is_transient_rejection(NO_WORKERS_REJECTION)


def test_a_manager_before_its_membership_forms_is_a_transient_rejection() -> None:
    assert is_transient_rejection(MEMBERSHIP_NOT_FORMED_REJECTION)


def test_file_reporters_are_written_by_the_client() -> None:
    json_reporter = JSONConfig(workflow_results_filepath="results.json", step_results_filepath="steps.json")
    csv_reporter = CSVConfig(workflow_results_filepath="results.csv", step_results_filepath="steps.csv")
    cluster_reporter = SimpleNamespace(reporter_type=ReporterTypes.Kafka)
    workflow = SimpleNamespace(reporting=[json_reporter, csv_reporter, cluster_reporter])

    workflows_with_ids, local_reporters = make_submitter()._prepare_workflows([([], workflow)])

    assert local_reporters == [json_reporter, csv_reporter]
    assert [workflow_id for workflow_id, _, _ in workflows_with_ids] == ["wf-1"]
