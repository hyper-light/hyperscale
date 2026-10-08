"""The text of a run's CI-safe summary lines: what the run is, and where
its results go."""

import pathlib

from hyperscale.core.graph import Workflow
from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.reporting.custom import CustomReporter
from hyperscale.reporting.reporter import ReporterConfig

# The fields of a reporter's config that name a file its results are
# written to (JSON, CSV and XML: ``*_filepath``; SQLite: ``database_path``).
RESULT_FILE_FIELD_SUFFIXES = ("_filepath", "_path")


def workflow_description(workflow: Workflow) -> str:
    """A workflow as the run's start line names it: its VUs and duration."""
    return f"{workflow.name} {workflow.vus} VUs for {workflow.duration}"


def start_text(workflows: list[Workflow], degraded_reason: str | None, log_path: pathlib.Path) -> str:
    """The run's start line: its test file, each workflow's VUs and
    duration, why the output is CI-safe, and where its logs go."""
    test_files = ", ".join(dict.fromkeys(pathlib.Path(workflow.graph).name for workflow in workflows))
    return " | ".join(
        (
            "run start",
            f"file {test_files}",
            f"workflows {', '.join(map(workflow_description, workflows))}",
            degraded_reason or "terminal mode ci-safe",
            f"logs {log_path}",
        )
    )


def reporter_destination(reporter: ReporterConfig | CustomReporter) -> str:
    """Where ``reporter`` writes results: its type, and every file its
    config names."""
    reporter_name = getattr(getattr(reporter, "reporter_type", None), "name", type(reporter).__name__)
    result_files = [
        str(value)
        for field_name, value in getattr(reporter, "__dict__", {}).items()
        if field_name.endswith(RESULT_FILE_FIELD_SUFFIXES)
    ]
    return " ".join((reporter_name, *result_files))


def workflow_result_destinations(workflow: Workflow) -> list[str]:
    """Where ``workflow``'s results are written: each reporter it
    configures, or, when its ``reporting`` chooses them as it runs, that."""
    if callable(workflow.reporting):
        return [f"{workflow.name} reporters chosen by its reporting at run time"]

    reporters = workflow.reporting if isinstance(workflow.reporting, list) else [workflow.reporting]
    return list(map(reporter_destination, reporters))


def results_text(workflows: list[Workflow]) -> str:
    """Where the run's results are written, each destination once."""
    destinations = dict.fromkeys(
        destination for workflow in workflows for destination in workflow_result_destinations(workflow)
    )
    return f"results {'; '.join(destinations)}"


def final_results_text(workflow_title: str, final_stats: WorkflowStats, last_step: str) -> str:
    """A workflow's final summary from its final results, as its reporters
    write them: its total actions over its elapsed time, its rate, each
    step's total, succeeded (ok) and failed (err) counts, and its last
    step."""
    step_counts = ", ".join(
        f"{result_set['step']} total {result_set['counts']['executed']} "
        f"ok {result_set['counts']['succeeded']} err {result_set['counts']['failed']}"
        for result_set in final_stats["results"]
    )
    return (
        f"{workflow_title}: {final_stats['stats']['executed']} actions in {final_stats['elapsed']:.1f}s, "
        f"{final_stats['aps']:.1f} actions/s, steps [{step_counts}], {last_step}"
    )
