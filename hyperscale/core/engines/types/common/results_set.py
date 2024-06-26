from collections import defaultdict
from typing import Any, Dict, List, Union

import psutil

from hyperscale.core.engines.types.common.base_result import BaseResult
from hyperscale.core.engines.types.common.types import RequestTypes
from hyperscale.core.engines.types.graphql import GraphQLResult
from hyperscale.core.engines.types.graphql_http2 import GraphQLHTTP2Result
from hyperscale.core.engines.types.grpc import GRPCResult
from hyperscale.core.engines.types.http import HTTPResult
from hyperscale.core.engines.types.http2 import HTTP2Result
from hyperscale.core.engines.types.playwright import PlaywrightResult
from hyperscale.core.engines.types.udp import UDPResult
from hyperscale.core.engines.types.websocket import WebsocketResult
from hyperscale.core.personas.streaming.stream_analytics import StreamAnalytics
from hyperscale.data.serializers.serializer import Serializer
from hyperscale.core.engines.types.task.result import TaskResult
from hyperscale.monitoring import CPUMonitor, MemoryMonitor

ResultsBatch = Dict[str, Union[List[BaseResult], float]]

MutationConfig = Dict[str, Union[str, float, List[str]]]

VariantConfig = Dict[str, Union[int, str, float, List[float], MutationConfig]]

ExperimentConfig = Dict[str, Union[str, bool, VariantConfig]]

MemoryMonitorGroup = Dict[str, MemoryMonitor]

CPUMonitorGroup = Dict[str, CPUMonitor]

MonitorGroup = Dict[str, Union[CPUMonitorGroup, MemoryMonitorGroup]]


class ResultsSet:
    def __init__(
        self,
        execution_results: Dict[
            str, Union[int, float, List[ResultsBatch], ExperimentConfig]
        ],
    ) -> None:
        self.stage: str = execution_results.get("stage")
        self.stage_streamed_analytics: Union[List[StreamAnalytics], None] = (
            execution_results.get("streamed_analytics")
        )

        self.total_elapsed: float = execution_results.get("total_elapsed", 0)
        self.total_results: int = execution_results.get("total_results", 0)

        self.stage_batch_size = execution_results.get("stage_batch_size", 0)
        self.stage_optimized = execution_results.get("stage_optimized", False)
        self.stage_persona_type = execution_results.get("stage_persona_type", "default")
        self.stage_workers = execution_results.get(
            "stage_workers", psutil.cpu_count(logical=False)
        )

        self.results: List[BaseResult] = execution_results.get("stage_results", [])

        self.serialized_results: List[Dict[str, Any]] = execution_results.get(
            "serialized_results", []
        )
        self.experiment = execution_results.get("experiment")
        self.serializer = Serializer()

        self.types = {
            RequestTypes.GRAPHQL: GraphQLResult,
            RequestTypes.GRAPHQL_HTTP2: GraphQLHTTP2Result,
            RequestTypes.GRPC: GRPCResult,
            RequestTypes.HTTP: HTTPResult,
            RequestTypes.HTTP2: HTTP2Result,
            RequestTypes.PLAYWRIGHT: PlaywrightResult,
            RequestTypes.TASK: TaskResult,
            RequestTypes.UDP: UDPResult,
            RequestTypes.WEBSOCKET: WebsocketResult,
        }

    def __iter__(self):
        for result in self.results:
            yield result

    def to_serializable(self):
        return {
            "total_elapsed": self.total_elapsed,
            "total_results": self.total_results,
            "results": [
                self.serializer.serialize_result(result) for result in self.results
            ],
        }

    def load_results(self):
        self.results = [
            self.serializer.deserialize_result(result)
            for result in self.serialized_results
        ]

    def group(self) -> Dict[str, List[BaseResult]]:
        grouped_results = defaultdict(list)
        for result in self.results:
            grouped_results[result.name].append(result)

        return grouped_results

    def copy(self):
        return ResultsSet(
            {
                "stage": self.stage,
                "streamed_analytics": list(self.stage_streamed_analytics),
                "stage_batch_size": self.stage_batch_size,
                "stage_persona_type": self.stage_persona_type,
                "stage_workers": self.stage_workers,
                "stage_optimized": self.stage_optimized,
                "total_elapsed": self.total_elapsed,
                "total_results": self.total_results,
                "stage_results": list(self.results),
                "serialized_results": list(self.serialized_results),
            }
        )
