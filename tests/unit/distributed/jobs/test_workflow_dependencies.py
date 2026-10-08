"""
A job's workflow dependencies are validated before the job is created.

Workflows depend on each other by name. The dispatcher used to link a
dependency only if its workflow came EARLIER in the submitted list, and
silently dropped an unknown one -- either way the dependent workflow ran
without waiting. Order no longer matters; an unknown dependency, a
duplicate name, or a cycle refuses the job with the reason.
"""

import pytest

from hyperscale.distributed.jobs.workflow_dependencies import (
    validate_workflow_dependencies,
)
from hyperscale.graph import Workflow, step


class Extract(Workflow):
    vus = 1

    @step()
    async def run(self) -> dict[str, str]:
        return {"status": "ok"}


class Transform(Workflow):
    vus = 1

    @step()
    async def run(self) -> dict[str, str]:
        return {"status": "ok"}


class Load(Workflow):
    vus = 1

    @step()
    async def run(self) -> dict[str, str]:
        return {"status": "ok"}


def test_dependencies_may_be_listed_in_any_order():
    validate_workflow_dependencies(
        [
            ("load-id", ["Transform"], Load()),
            ("transform-id", ["Extract"], Transform()),
            ("extract-id", [], Extract()),
        ]
    )


def test_independent_workflows_are_accepted():
    validate_workflow_dependencies(
        [("extract-id", [], Extract()), ("load-id", [], Load())]
    )


def test_a_dependency_on_a_workflow_outside_the_job_is_refused():
    with pytest.raises(ValueError, match=r"'Load' depends on \['Transfrom'\]"):
        validate_workflow_dependencies(
            [("extract-id", [], Extract()), ("load-id", ["Transfrom"], Load())]
        )


def test_duplicate_workflow_names_are_refused():
    with pytest.raises(ValueError, match=r"unique within a job: \['Extract'\]"):
        validate_workflow_dependencies(
            [("first-id", [], Extract()), ("second-id", [], Extract())]
        )


def test_a_dependency_cycle_is_refused():
    with pytest.raises(ValueError, match="cycle: .*Extract.*Load.*"):
        validate_workflow_dependencies(
            [("extract-id", ["Load"], Extract()), ("load-id", ["Extract"], Load())]
        )


def test_a_workflow_depending_on_itself_is_a_cycle():
    with pytest.raises(ValueError, match="cycle: Load -> Load"):
        validate_workflow_dependencies([("load-id", ["Load"], Load())])


def test_a_job_with_no_workflows_is_refused():
    """Nothing in it could complete it: accepted, it sat RUNNING until its
    timeout failed it."""
    with pytest.raises(ValueError, match="at least one workflow"):
        validate_workflow_dependencies([])
