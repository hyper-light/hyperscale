import asyncio
import warnings
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import psutil

from hyperscale.core_rewrite.graph import Graph
from hyperscale.core_rewrite.graph.workflow import Workflow
from hyperscale.core_rewrite.jobs.models import WorkflowStatus
from hyperscale.core_rewrite.results.workflow_types import WorkflowStats
from hyperscale.core_rewrite.state import Context

warnings.simplefilter("ignore")


class WorkflowRunner:
    def __init__(self) -> None:
        self.graph = __file__
        self.workflows: Dict[int, Workflow] = {}
        self.workflow_statuses: Dict[int, WorkflowStatus] = {}
        self.run_nodes: Dict[int, List[int]] = defaultdict(list)
        self.node_runs: Dict[int, List[int]] = defaultdict(list)

        self._workflows_by_name: Dict[str, Workflow] = {}

        self._workflow_configs: Dict[str, Dict[str, Any]] = {}

        self._workflows: Dict[int, Workflow] = {}
        self._results: Dict[int, WorkflowStats | Dict[str, Any | Exception]] = {}
        self.context: Optional[Context] = None
        self._expected_completions: Dict[int, int] = {}
        self._threads = psutil.cpu_count(logical=False)
        self._graphs: Dict[int, Graph] = {}
        self._context: Dict[int, Context] = {}

    def initialize_context(self):
        self.context = Context()

    async def store_results(
        self,
        node_id: int,
        run_id: int,
        results: WorkflowStats | Dict[str, Any | Exception],
        workflow_context: Dict[str, Dict[str, Any | Exception]],
    ):
        self._results[node_id] = results

        context = Context()

        await asyncio.gather(
            *[
                context.update(workflow_name, hook_name, value)
                for workflow_name, hook_context in workflow_context.items()
                for hook_name, value in hook_context.items()
            ]
        )

        self._context[run_id] = context

        return len(self._results)

    def get_expected_completions(self, run_id: int):
        return self._expected_completions.get(run_id, 0)

    async def run(
        self,
        node_id: int,
        run_id: int,
        workflow: Workflow,
        workflow_context: Dict[str, Dict[str, Any]],
    ) -> Tuple[
        WorkflowStats | Dict[str, Any | Exception],
        Context,
    ]:
        self.workflows[run_id] = workflow
        self.workflow_statuses[run_id] = WorkflowStatus.CREATED
        self.run_nodes[run_id].append(node_id)
        self.node_runs[node_id].append(run_id)

        if self._graphs.get(run_id) is None:
            self._graphs[run_id] = Graph([workflow])

        graph = self._graphs[run_id]

        context = Context()

        await asyncio.gather(
            *[
                context.update(workflow_name, hook_name, value)
                for workflow_name, hook_context in workflow_context.items()
                for hook_name, value in hook_context.items()
            ]
        )

        (results, updated_context) = await graph.run_workflow(
            workflow,
            context,
        )

        await asyncio.gather(
            *[
                context.update(workflow_name, hook_name, value)
                for workflow_name, hook_context in updated_context.iter_workflow_contexts()
                for hook_name, value in hook_context.items()
            ]
        )

        self._results[run_id] = results

        return {"run_id": run_id, **results}, context
