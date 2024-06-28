import asyncio
import os
from collections import defaultdict
from typing import Any, Dict, Set

from hyperscale.core_rewrite.graph import Workflow
from hyperscale.core_rewrite.jobs.models import (
    AcknowledgedCompletion,
    Response,
    WorkflowJob,
    WorkflowResults,
)
from hyperscale.core_rewrite.jobs.models.env import Env
from hyperscale.core_rewrite.results.workflow_types import WorkflowStats
from hyperscale.core_rewrite.snowflake import Snowflake
from hyperscale.core_rewrite.state import Context

from .graphs import WorkflowRunner
from .hooks import (
    receive,
    send,
    task,
)
from .models import JobContext, WorkflowStatus, WorkflowStatusUpdate
from .protocols import TCPProtocol


class JobServer(TCPProtocol[JobContext[Any], JobContext[Any]]):
    def __init__(
        self,
        host: str,
        port: int,
        env: Env,
    ) -> None:
        super().__init__(host, port, env)

        self._workflows = WorkflowRunner()
        self._results: Dict[
            int,
            Dict[
                str,
                WorkflowStats
                | Dict[
                    str,
                    Any | Exception,
                ],
            ],
        ] = defaultdict(dict)
        self._contexts: Dict[int, Context] = {}

        self._completions: Dict[int, Set[int]] = defaultdict(set)
        self._run_workflow_node_ids: Dict[int, Dict[str, int]] = defaultdict(dict)

    @send()
    async def submit(
        self,
        workflow: Workflow,
        context: Context,
    ) -> Response[JobContext[WorkflowStatusUpdate]]:
        return await self.send(
            "start_workflow",
            JobContext(
                WorkflowJob(
                    workflow,
                    context,
                ),
            ),
        )

    @send()
    async def send_stop(self):
        pass

    @send()
    async def push_results(
        self,
        node_id: str,
        results: WorkflowResults,
        run_id: int,
    ) -> Response[JobContext[AcknowledgedCompletion]]:
        return await self.send(
            "process_results",
            JobContext(
                results,
                run_id=run_id,
            ),
            node_id=node_id,
        )

    @receive()
    async def process_results(
        self,
        shard_id: int,
        workflow_results: JobContext[WorkflowResults],
    ) -> JobContext[AcknowledgedCompletion]:
        snowflake = Snowflake.parse(shard_id)
        node_id = snowflake.instance

        run_id = workflow_results.run_id
        workflow_name = workflow_results.data.workflow
        results = workflow_results.data.results
        workflow_context = workflow_results.data.context

        context = Context()

        await asyncio.gather(
            *[
                context.update(
                    workflow,
                    key,
                    value,
                )
                for workflow, ctx in workflow_context.items()
                for key, value in ctx.items()
            ]
        )

        self._contexts[run_id] = context

        self._results[run_id][workflow_name] = results
        self._completions[run_id].add(node_id)

        return JobContext(
            AcknowledgedCompletion(
                workflow_results.data.workflow,
                node_id,
            ),
            run_id=workflow_results.run_id,
        )

    @receive()
    async def stop_server(
        self,
        _: int,
        acknowleged_completion: JobContext[AcknowledgedCompletion],
    ):
        self._completions[acknowleged_completion.run_id].add(
            acknowleged_completion.data.completed_node,
        )

    @receive()
    async def start_workflow(
        self,
        shard_id: int,
        context: JobContext[WorkflowJob],
    ) -> JobContext[WorkflowStatusUpdate]:
        run_id = self.tasks.create_task_id()

        snowflake = Snowflake.parse(shard_id)
        node_id = snowflake.instance

        workflow_name = context.data.workflow.name

        self._run_workflow_node_ids[run_id][workflow_name] = node_id

        self.tasks.run(
            "run_workflow",
            node_id,
            run_id,
            context.data,
            run_id=run_id,
        )

        return JobContext(
            WorkflowStatusUpdate(
                WorkflowStatus.SUBMITTED,
                f"Workflow - {context.data.workflow} - submitted to worker - {self.node_id}",
            ),
            run_id=run_id,
        )

    @task(
        keep=int(
            os.getenv("HYPERSCALE_MAX_JOBS", 100),
        ),
        repeat="NEVER",
    )
    async def run_workflow(
        self,
        node_id: int,
        run_id: int,
        job: WorkflowJob,
    ):
        results = {}
        (
            run_id,
            results,
            context,
        ) = await self._workflows.run(
            run_id,
            job.workflow,
            job.context,
        )

        await self.push_results(
            node_id,
            WorkflowResults(
                job.workflow.name,
                results,
                context,
            ),
            run_id,
        )
