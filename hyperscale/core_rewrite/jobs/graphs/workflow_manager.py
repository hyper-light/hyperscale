import asyncio
from typing import Any, Dict, List

import networkx
import psutil

from hyperscale.core_rewrite.graph.dependent_workflow import DependentWorkflow
from hyperscale.core_rewrite.graph.workflow import Workflow
from hyperscale.core_rewrite.hooks import Hook


class WorkflowManager:
    def __init__(
        self,
        graph: str,
        workflows: List[Workflow | DependentWorkflow],
    ) -> None:
        self.graph = graph
        self.workflows = workflows
        self._max_active: Dict[str, int] = {}
        self._active: Dict[str, int] = {}

        self._active_waiters: Dict[str, asyncio.Future | None] = {}

        self._workflows_by_name: Dict[str, Workflow] = {}
        self._threads = psutil.cpu_count(logical=False)
        self._step_traversal_orders: Dict[
            str,
            List[
                Dict[
                    str,
                    Hook,
                ]
            ],
        ] = {}

        self.workflow_traversal_order: List[
            Dict[
                str,
                Workflow,
            ]
        ] = []

        self._workflow_configs: Dict[str, Dict[str, Any]] = {}
        self._workflows: Dict[str, Workflow] = {}

    def create_workflow_graph(self):
        workflow_graph = networkx.DiGraph()

        workflow_dependencies: Dict[str, List[str]] = {}

        sources = []

        for workflow in self.workflows:
            if (
                isinstance(workflow, DependentWorkflow)
                and len(workflow.dependencies) > 0
            ):
                dependent_workflow = workflow.dependent_workflow
                workflow_dependencies[dependent_workflow.name] = workflow.dependencies

                self._workflows[dependent_workflow.name] = dependent_workflow

                workflow_graph.add_node(dependent_workflow.name)

            else:
                self._workflows[workflow.name] = workflow
                sources.append(workflow.name)

                workflow_graph.add_node(workflow.name)

        for workflow_name, dependencies in workflow_dependencies.items():
            for dependency in dependencies:
                workflow_graph.add_edge(dependency, workflow_name)

        for traversal_layer in networkx.bfs_layers(workflow_graph, sources):
            self.workflow_traversal_order.append(
                {
                    workflow_name: self._workflows.get(workflow_name)
                    for workflow_name in traversal_layer
                }
            )
