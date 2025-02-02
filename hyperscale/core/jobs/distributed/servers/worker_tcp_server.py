import asyncio
import os
import psutil
from hyperscale.core.jobs.protocols import TCPProtocol
from hyperscale.core.jobs.models import (
    JobContext,
    ReceivedReceipt,
    Response,
    WorkflowJob,
    WorkflowResults,
    WorkflowStatusUpdate,
    Env
)
from hyperscale.core.jobs.graphs import WorkflowRunner
from hyperscale.core.jobs.models.workflow_status import WorkflowStatus
from hyperscale.core.snowflake import Snowflake
from hyperscale.core.state import Context
from hyperscale.logging import Logger, Entry, LogLevel
from hyperscale.logging.hyperscale_logging_models import (
    RunTrace,
    RunDebug,
    RunInfo,
    RunError,
    RunFatal,
    StatusUpdate
)
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.graph import Workflow
from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.core.jobs.runner.local_server_pool import LocalServerPool
from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.ui import HyperscaleInterface, InterfaceUpdatesController
from typing import Any, Tuple, TypeVar, Dict, Literal

T = TypeVar("T")

WorkflowResult = Tuple[
    int,
    WorkflowStats | Dict[str, Any | Exception],
]


NodeContextSet = Dict[int, Context]

NodeData = Dict[
    int,
    Dict[
        str,
        Dict[int, T],
    ],
]

StepStatsType = Literal[
    "total",
    "ok",
    "err",
]


StepStatsUpdate = Dict[str, Dict[StepStatsType, int]]


class WorkerTCPServer(TCPProtocol[JobContext[Any], JobContext[Any]]):
    
    def __init__(
        self,
        host: str,
        port: int,
        env: Env,
        manager: RemoteGraphManager,
    ):
        super().__init__(host, port, env)
        self._manager = manager 