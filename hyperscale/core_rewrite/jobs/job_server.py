import os
from asyncio import Server
from socket import socket
from typing import Any

from hyperscale.core_rewrite.graph import Workflow
from hyperscale.core_rewrite.jobs.models import (
    AcknowledgedCompletion,
    WorkflowJob,
    WorkflowResults,
)
from hyperscale.core_rewrite.jobs.models.env import Env
from hyperscale.core_rewrite.snowflake import Snowflake
from hyperscale.core_rewrite.state import Context

from .graphs import WorkflowRunner
from .hooks import (
    broadcast,
    push,
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

    async def start_server(
        self,
        cert_path: str | None = None,
        key_path: str | None = None,
        worker_socket: socket | None = None,
        worker_server: Server | None = None,
    ):
        self._workflows.initialize_context()
        return await super().start_server(
            cert_path,
            key_path,
            worker_socket,
            worker_server,
        )

    @send("start_workflow")
    async def submit(
        self, workflow: Workflow, context: Context
    ) -> JobContext[Workflow]:
        return JobContext(
            WorkflowJob(
                workflow,
                context,
            ),
        )

    @push("process_results")
    async def push_results(
        self,
        node_id: str,
        results: WorkflowResults,
        run_id: int,
    ) -> JobContext[WorkflowResults]:
        return (
            node_id,
            JobContext(
                results,
                run_id=run_id,
            ),
        )

    @broadcast("stop_server")
    async def process_results(
        self,
        shard_id: int,
        workflow_results: JobContext[WorkflowResults],
    ) -> JobContext[AcknowledgedCompletion]:
        snowflake = Snowflake.parse(shard_id)

        completed = self._workflows.store_results(
            snowflake.instance,
            workflow_results.data.results,
            workflow_results.data.context,
        )

        return JobContext(
            AcknowledgedCompletion(
                workflow_results.data.workflow,
                completed,
            ),
            run_id=workflow_results.run_id,
        )

    @receive()
    async def stop_server(
        self,
        _: int,
        acknowleged_completion: JobContext[AcknowledgedCompletion],
    ):
        try:
            print(acknowleged_completion.data.completions)
            print(
                self._workflows.get_expected_completions(acknowleged_completion.run_id)
            )
            if (
                acknowleged_completion.data.completions
                == self._workflows.get_expected_completions(
                    acknowleged_completion.run_id,
                )
            ):
                self.stop()

        except Exception:
            import traceback

            print(traceback.format_exc())

    @receive()
    async def start_workflow(
        self,
        shard_id: int,
        context: JobContext[WorkflowJob],
    ) -> JobContext[WorkflowStatusUpdate]:
        run_id = self.tasks.create_task_id()

        snowflake = Snowflake.parse(shard_id)
        self.tasks.run(
            "run_workflow",
            snowflake.instance,
            run_id,
            context.data,
            run_id=run_id,
        )

        return JobContext(
            WorkflowStatusUpdate(
                WorkflowStatus.SUBMITTED,
                f"Workflow - {context.data.name} - submitted to worker - {self.node_id}",
            ),
            run_id=run_id,
        )

    @task(
        keep=int(
            os.getenv("HYPERSCALE_MAX_JOBS", 100),
        ),
    )
    async def run_workflow(
        self,
        node_id: int,
        run_id: int,
        job: WorkflowJob,
    ):
        (results, context) = await self._workflows.run(
            node_id, run_id, job.workflow, job.context
        )

        print(context)

        try:
            await self.push_results(
                node_id,
                WorkflowResults(
                    job.workflow.name,
                    results,
                    context,
                ),
                run_id,
            )

        except Exception:
            import traceback

            print(traceback.format_exc())
