"""
UniqueSubWorkflowTokens (SCENARIOS.md §11 "Sub-workflow tokens are unique
per (job, workflow) pair").

A sub-workflow token (``dc:manager:job:workflow:worker``) names one
dispatch of one workflow to one worker. It is unique when:

* no two live workers run the same token at once (a token running twice
  is a workflow executing twice under one identity);
* the worker running a token is the worker the token names;
* no manager lists one token twice in a job, under one parent workflow
  or under two.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.models.jobs import JobInfo, TrackingToken

from tests.simulation.harness.invariant_checks.live_nodes import live_handles
from tests.simulation.harness.server_handle import ServerHandle, ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


def unique_sub_workflow_token_violation(harness: "ClusterHarness") -> str:
    """The first breach of sub-workflow token uniqueness, or ``""``."""
    return _worker_token_violation(harness) or _manager_listing_violation(harness)


def _worker_token_violation(harness: "ClusterHarness") -> str:
    owners: dict[str, str] = {}
    details = [
        _running_token_violation(handle, token, owners)
        for handle in live_handles(harness, ServerKind.WORKER)
        for token in list(handle.instance._worker_state._active_workflows)
    ]
    return next(filter(None, details), "")


def _running_token_violation(handle: ServerHandle, token: str, owners: dict[str, str]) -> str:
    previous_owner = owners.setdefault(token, handle.node_id)
    if previous_owner != handle.node_id:
        return f"sub-workflow token {token!r} runs on both {previous_owner} and {handle.node_id}"
    return _attribution_violation(handle, token)


def _attribution_violation(handle: ServerHandle, token: str) -> str:
    try:
        named_worker = TrackingToken.parse(token).worker_id
    except ValueError as parse_error:
        return f"{handle.node_id} runs unparseable sub-workflow token {token!r}: {parse_error}"
    if named_worker == handle.instance._node_id.full:
        return ""
    return (
        f"{handle.node_id} ({handle.instance._node_id.full}) runs sub-workflow "
        f"token {token!r} that names worker {named_worker!r}"
    )


def _manager_listing_violation(harness: "ClusterHarness") -> str:
    details = [
        _job_listing_violation(handle.node_id, job)
        for handle in live_handles(harness, ServerKind.MANAGER)
        for job in handle.instance._job_manager.iter_jobs()
    ]
    return next(filter(None, details), "")


def _job_listing_violation(node_id: str, job: JobInfo) -> str:
    listed_tokens = _listed_sub_workflow_tokens(job)
    if len(listed_tokens) == len(set(listed_tokens)):
        return ""
    return (
        f"{node_id} lists sub-workflow tokens more than once in job "
        f"{job.job_id!r}: {_repeated(listed_tokens)}"
    )


def _listed_sub_workflow_tokens(job: JobInfo) -> list[str]:
    return [
        token
        for workflow in list(job.workflows.values())
        for token in list(workflow.sub_workflow_tokens)
    ]


def _repeated(tokens: list[str]) -> list[str]:
    return sorted({token for token in tokens if tokens.count(token) > 1})
