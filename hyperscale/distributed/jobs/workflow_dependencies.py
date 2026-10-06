from collections import Counter

import networkx

from hyperscale.core.graph.workflow import Workflow
from hyperscale.distributed.health.deadline_resolver import (
    resolve_worker_deadline_seconds,
)


def workflow_name(instance: Workflow) -> str:
    """The name a job's workflows depend on each other by."""
    return getattr(instance, "name", None) or type(instance).__name__


def validate_workflow_dependencies(
    workflows: list[tuple[str, list[str], Workflow]],
) -> None:
    """
    Refuse a job whose workflows cannot all run in dependency order.

    A job runs at least one workflow: with none, nothing it holds could
    ever complete it, and it would sit until its timeout failed it.
    Workflows depend on each other by name, so every name in the job must
    be unique and every dependency must name a workflow of the same job;
    and the dependencies must not form a cycle, since a workflow on a
    cycle never becomes ready.

    Raises:
        ValueError: a job with no workflows, a duplicate workflow name, a
            dependency on a workflow the job does not contain, or a
            dependency cycle.
    """
    if not workflows:
        raise ValueError("a job must contain at least one workflow")

    names = [workflow_name(instance) for _, _, instance in workflows]

    if duplicates := sorted(name for name, count in Counter(names).items() if count > 1):
        raise ValueError(f"workflow names must be unique within a job: {duplicates}")

    known_names = set(names)
    graph = networkx.DiGraph()
    graph.add_nodes_from(names)

    for (_, dependencies, _), name in zip(workflows, names):
        if unknown := [dependency for dependency in dependencies if dependency not in known_names]:
            raise ValueError(
                f"workflow {name!r} depends on {unknown}, which this job does not contain"
            )

        graph.add_edges_from((dependency, name) for dependency in dependencies)

    if not networkx.is_directed_acyclic_graph(graph):
        cycle_edges = networkx.find_cycle(graph)
        cycle = " -> ".join([source for source, _ in cycle_edges] + [cycle_edges[0][0]])
        raise ValueError(f"workflow dependencies form a cycle: {cycle}")


def resolve_job_deadline_seconds(
    workflows: list[tuple[str, list[str], Workflow]],
    default_multiplier: float,
) -> float:
    """The time budget of a job submitted without a timeout of its own.

    A workflow starts once every workflow it depends on has finished, and
    independent workflows run side by side, so the job needs as long as
    its longest chain of dependent workflows -- each workflow taking the
    deadline its workers observe without an explicit submission timeout.
    Taken as zero, the budget timed the job out at its first check.

    ``workflows`` must have passed ``validate_workflow_dependencies``.
    """
    workflows_by_name = {
        workflow_name(instance): (dependency_names, instance)
        for _, dependency_names, instance in workflows
    }
    dependency_graph = networkx.DiGraph()
    dependency_graph.add_nodes_from(workflows_by_name)
    dependency_graph.add_edges_from(
        (dependency_name, name)
        for name, (dependency_names, _) in workflows_by_name.items()
        for dependency_name in dependency_names
    )

    finished_after_seconds: dict[str, float] = {}
    for name in networkx.topological_sort(dependency_graph):
        dependency_names, instance = workflows_by_name[name]
        finished_after_seconds[name] = max(
            (finished_after_seconds[dependency_name] for dependency_name in dependency_names),
            default=0.0,
        ) + resolve_worker_deadline_seconds(
            workflow=instance,
            submission_timeout_seconds=0.0,
            submission_timeout_explicit=False,
            default_multiplier=default_multiplier,
        )

    return max(finished_after_seconds.values(), default=0.0)


def select_rerun_workflows(
    workflows: list[tuple[str, list[str], Workflow]],
    rerun_workflow_ids: list[str],
) -> list[tuple[str, list[str], Workflow]]:
    """The workflows a re-run of a job's unfinished share runs (AD-36
    mid-flight failover): those named, and every workflow they depend on,
    directly or not -- each re-runs to regenerate the context its
    dependents read (AD-49: context flows along dependencies), so the
    selection runs in dependency order on its own. In the job's order.

    Raises:
        ValueError: a named workflow id the job does not contain.
    """
    names_by_workflow_id = {
        workflow_id: workflow_name(instance) for workflow_id, _, instance in workflows
    }
    if unknown := sorted(set(rerun_workflow_ids) - names_by_workflow_id.keys()):
        raise ValueError(f"re-run names workflows the job does not contain: {unknown}")

    dependency_names_by_name = {
        workflow_name(instance): dependency_names for _, dependency_names, instance in workflows
    }
    selected_names = {names_by_workflow_id[workflow_id] for workflow_id in rerun_workflow_ids}
    pending_names = list(selected_names)
    while pending_names:
        for dependency_name in dependency_names_by_name[pending_names.pop()]:
            if dependency_name not in selected_names:
                selected_names.add(dependency_name)
                pending_names.append(dependency_name)

    return [
        (workflow_id, dependency_names, instance)
        for workflow_id, dependency_names, instance in workflows
        if workflow_name(instance) in selected_names
    ]
