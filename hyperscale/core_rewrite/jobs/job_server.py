import asyncio
import os
from collections import defaultdict
from socket import socket
from typing import Any, Dict, Set, Tuple

from hyperscale.core_rewrite.graph import Workflow
from hyperscale.core_rewrite.jobs.models import (
    ReceivedReceipt,
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
from .models import JobContext, WorkflowStatusUpdate
from .models.workflow_status import WorkflowStatus
from .protocols import TCPProtocol


class JobServer(TCPProtocol[JobContext[Any], JobContext[Any]]):
    def __init__(
        self,
        host: str,
        port: int,
        env: Env,
    ) -> None:
        super().__init__(host, port, env)

        self._workflows = WorkflowRunner(env)
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

        self._errors: Dict[
            int,
            Dict[
                str,
                Exception,
            ],
        ] = defaultdict(dict)
        self._contexts: Dict[int, Context] = {}
        self._statuses: Dict[int, Dict[str, Dict[int, WorkflowStatus]]] = defaultdict(
            lambda: defaultdict(dict)
        )

        self._completions: Dict[int, Set[int]] = defaultdict(set)
        self._run_workflow_node_ids: Dict[int, Dict[str, int]] = defaultdict(dict)

    async def start_server(
        self,
        cert_path: str | None = None,
        key_path: str | None = None,
        worker_socket: socket | None = None,
        worker_server: asyncio.Server | None = None,
    ) -> None:
        self._workflows.setup()
        return await super().start_server(
            cert_path,
            key_path,
            worker_socket,
            worker_server,
        )

    async def connect_client(
        self,
        address: Tuple[str, int],
        cert_path: str | None = None,
        key_path: str | None = None,
        worker_socket: socket | None = None,
    ) -> None:
        self._workflows.setup()
        return await super().connect_client(
            address,
            cert_path,
            key_path,
            worker_socket,
        )

    @send()
    async def submit(
        self,
        workflow: Workflow,
        context: Context,
    ) -> Response[JobContext[WorkflowStatusUpdate]]:
        response: Response[JobContext[WorkflowStatusUpdate]] = await self.send(
            "start_workflow",
            JobContext(
                WorkflowJob(
                    workflow,
                    context,
                ),
            ),
        )

        (shard_id, workflow_status) = response

        status = workflow_status.data.status
        workflow_name = workflow_status.data.workflow
        run_id = workflow_status.run_id

        snowflake = Snowflake.parse(shard_id)
        node_id = snowflake.instance

        self._statuses[run_id][workflow_name][node_id] = (
            WorkflowStatus.map_value_to_status(status)
        )

        return response

    @send()
    async def send_stop(self):
        pass

    @send()
    async def push_results(
        self,
        node_id: str,
        results: WorkflowResults,
        run_id: int,
    ) -> Response[JobContext[ReceivedReceipt]]:
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
    ) -> JobContext[ReceivedReceipt]:
        snowflake = Snowflake.parse(shard_id)
        node_id = snowflake.instance

        run_id = workflow_results.run_id
        workflow_name = workflow_results.data.workflow

        results = workflow_results.data.results
        workflow_context = workflow_results.data.context
        error = workflow_results.data.error
        status = workflow_results.data.status

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
        self._statuses[run_id][workflow_name] = status
        self._errors[run_id][workflow_name] = Exception(error)

        self._completions[run_id].add(node_id)

        return JobContext(
            ReceivedReceipt(
                workflow_name,
                node_id,
            ),
            run_id=run_id,
        )

    @receive()
    async def stop_server(
        self,
        _: int,
        receipt: JobContext[ReceivedReceipt],
    ):
        self._completions[receipt.run_id].add(
            receipt.data.node_id,
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

        self.tasks.run(
            "push_workflow_status_update",
            node_id,
            run_id,
            context.data,
            run_id=run_id,
        )

        return JobContext(
            WorkflowStatusUpdate(
                workflow_name,
                node_id,
                WorkflowStatus.SUBMITTED,
            ),
            run_id=run_id,
        )

    @receive()
    async def receive_status_update(
        self,
        shard_id: int,
        update: JobContext[WorkflowStatusUpdate],
    ) -> JobContext[ReceivedReceipt]:
        snowflake = Snowflake.parse(shard_id)
        node_id = snowflake.instance

        run_id = update.run_id
        workflow = update.data.workflow
        status = update.data.status

        self._statuses[run_id][workflow][node_id] = WorkflowStatus.map_value_to_status(
            status
        )

        return JobContext(
            ReceivedReceipt(
                workflow,
                node_id,
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
        (
            run_id,
            results,
            context,
            error,
            status,
        ) = await self._workflows.run(
            run_id,
            job.workflow,
            job.context,
        )

        if context is None:
            context = job.context

        await self.push_results(
            node_id,
            WorkflowResults(
                job.workflow.name,
                results,
                context,
                error,
                status,
            ),
            run_id,
        )

    @task(
        trigger="MANUAL",
        repeat="ALWAYS",
        schedule="1s",
        max_age="1m",
        keep_policy="AGE",
    )
    async def push_workflow_status_update(
        self,
        node_id: int,
        run_id: int,
        job: WorkflowJob,
    ):
        workflow_name = job.workflow.name

        status = self._workflows.get_workflow_status(
            run_id,
            workflow_name,
        )

        if status in [
            WorkflowStatus.COMPLETED,
            WorkflowStatus.REJECTED,
            WorkflowStatus.FAILED,
        ]:
            self.tasks.stop("push_workflow_status_update")

        await self.broadcast(
            "receive_status_update",
            JobContext(
                WorkflowStatusUpdate(
                    workflow_name,
                    node_id,
                    status,
                ),
                run_id=run_id,
            ),
        )
