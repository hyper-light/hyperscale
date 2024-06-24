from collections import Counter
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Type,
)

import numpy as np

from hyperscale.core_rewrite.engines.client.graphql import GraphQLResponse
from hyperscale.core_rewrite.engines.client.graphql_http2 import GraphQLHTTP2Response
from hyperscale.core_rewrite.engines.client.grpc import GRPCResponse
from hyperscale.core_rewrite.engines.client.http import HTTPRequest
from hyperscale.core_rewrite.engines.client.http2 import HTTP2Request
from hyperscale.core_rewrite.engines.client.http3 import HTTP3Request
from hyperscale.core_rewrite.engines.client.playwright import PlaywrightResult
from hyperscale.core_rewrite.engines.client.shared.models import RequestType
from hyperscale.core_rewrite.engines.client.udp import UDPResponse
from hyperscale.core_rewrite.engines.client.websocket import WebsocketResponse
from hyperscale.core_rewrite.hooks import Hook, HookType
from hyperscale.core_rewrite.testing.models.metric import (
    COUNT,
    DISTRIBUTION,
    RATE,
    SAMPLE,
    Metric,
)

StatTypes = Literal["max", "min", "mean", "med", "stdev", "var", "mad"]

StatusCounts = Dict[int, int]
StatsResults = Dict[StatTypes, int | float]
CountResults = Dict[
    Literal["succeeded", "failed"] | Optional[Literal["statuses"]],
    int | Optional[StatusCounts],
]
FailedResults = Dict[Literal["failed"], int]
ContextResults = List[Dict[Literal["context", "count"], str | int]]
ResultSet = Dict[
    Literal[
        "workflow",
        "step",
        "timings",
        "counts",
        "contexts",
    ],
    str | StatsResults | CountResults | ContextResults,
]

ExceptionSet = Dict[
    Literal[
        "workflow",
        "step",
        "counts",
        "contexts",
    ],
    str | FailedResults | ContextResults,
]


class WorkflowResults:
    def __init__(
        self,
        hooks: Dict[str, Hook],
        precision: int = 4,
    ) -> None:
        self._result_type: Dict[
            Type[GraphQLResponse]
            | Type[GraphQLHTTP2Response]
            | Type[GRPCResponse]
            | Type[HTTPRequest]
            | Type[HTTP2Request]
            | Type[HTTP3Request]
            | Type[PlaywrightResult]
            | Type[UDPResponse]
            | Type[WebsocketResponse]
            | Type[Metric]
            | Type[Exception],
            Callable[
                [
                    str,
                    List[GraphQLResponse]
                    | List[GraphQLHTTP2Response]
                    | List[GRPCResponse]
                    | List[HTTPRequest]
                    | List[HTTP2Request]
                    | List[HTTP3Request]
                    | List[PlaywrightResult]
                    | List[UDPResponse]
                    | List[WebsocketResponse]
                    | List[Metric]
                    | List[Exception],
                ],
                int | float,
            ],
        ] = {}

        self._hooks = hooks
        self._quantiles = [10, 20, 25, 30, 40, 50, 60, 70, 75, 80, 90, 99]
        self._precision = precision

    def process(
        self,
        workflow: str,
        results: Dict[
            str,
            List[Any],
        ],
    ):
        results_sets = []
        for step in results:
            step_results = results[step]

            results_set = {
                "workflow": workflow,
                "step": step,
            }

            hook = self._hooks[step]
            hook_type = hook.hook_type

            match hook_type:
                case HookType.TEST:
                    results_set.update(
                        self._process_http_or_udp_timings_set(
                            workflow, step, hook.engine_type, step_results
                        )
                    )
                    results_sets.append(results_set)

                case HookType.METRIC:
                    results_set.update(
                        self._process_metrics_set(
                            workflow,
                            step,
                            hook.metric_type,
                            hook.tags,
                            step_results,
                        )
                    )

                    results_sets.append(results_set)

                case HookType.CHECK:
                    results_set.update(
                        self._process_exception_set(
                            workflow,
                            step,
                            step_results,
                        )
                    )

                    results_sets.append(results_set)

                case _:
                    pass

        return results_sets

    def _process_exception_set(
        self,
        workflow: str,
        step_name: str,
        exceptions: List[Exception],
    ) -> ExceptionSet:
        exception_messages = [str(err) for err in exceptions]

        exception_contexts = Counter(exception_messages)

        return {
            "workflow": workflow,
            "step": step_name,
            "counts": {
                "failed": len(exception_messages),
            },
            "contexts": [
                {
                    "context": context,
                    "count": count,
                }
                for context, count in exception_contexts.items()
            ],
        }

    def _process_metrics_set(
        self,
        workflow: str,
        step_name: str,
        metric_type: COUNT | DISTRIBUTION | SAMPLE | RATE,
        tags: List[str],
        metrics: List[Metric],
    ):
        if metric_type == COUNT:
            return {
                "workflow": workflow,
                "step": step_name,
                "metric_type": "COUNT",
                "stats": {
                    "count": round(
                        sum(metrics),
                        self._precision,
                    ),
                },
                "tags": tags,
            }

        elif metric_type == DISTRIBUTION:
            stats = self._calculate_quantiles(metrics)
            stats["max"] = round(
                max(metrics),
                self._precision,
            )

            stats["min"] = round(
                min(metrics),
                self._precision,
            )

            return {
                "workflow": workflow,
                "step": step_name,
                "metric_type": "DISTRIBUTION",
                "stats": stats,
                "tags": tags,
            }

        elif metric_type == SAMPLE:
            stats = self._calculate_stats(metrics)
            stats.update(
                self._calculate_quantiles(metrics),
            )

            return {
                "workflow": workflow,
                "step": step_name,
                "metric_type": "SAMPLE",
                "stats": stats,
                "tags": tags,
            }

        elif metric_type == RATE:
            rates = [
                metric
                for metric in metrics
                if isinstance(metric, tuple) and len(metric) == 2
            ]
            values = [metric[0] for metric in rates]
            times = [metric[1] for metric in rates]

            elapsed = max(times) - min(times)

            return {
                "workflow": workflow,
                "step": step_name,
                "metric_type": "RATE",
                "stats": {
                    "rate": round(
                        sum(values) / elapsed,
                        self._precision,
                    ),
                },
                "tags": tags,
            }

    def _calculate_quantiles(self, values: List[int | float]):
        return {
            f"{quantile}th_quantile": round(
                round(
                    float(value),
                    self._precision,
                ),
                self._precision,
            )
            for quantile, value in zip(
                self._quantiles,
                np.percentile(
                    values,
                    self._quantiles,
                ),
            )
        }

    def _calculate_stats(self, values: List[int | float]):
        mean = float(np.mean(values))

        return {
            "mean": round(
                mean,
                self._precision,
            ),
            "max": round(
                max(values),
                self._precision,
            ),
            "min": round(
                min(values),
                self._precision,
            ),
            "med": round(
                float(np.median(values)),
                self._precision,
            ),
            "stdev": round(
                float(np.std(values)),
                self._precision,
            ),
            "var": round(
                float(np.var(values)),
                self._precision,
            ),
            "mad": round(
                float(np.mean([abs(el - mean) for el in values])),
                self._precision,
            ),
        }

    def _process_http_or_udp_timings_set(
        self,
        workflow: str,
        step_name: str,
        result_type: RequestType,
        results: List[GraphQLResponse]
        | List[GraphQLHTTP2Response]
        | List[GRPCResponse]
        | List[HTTPRequest]
        | List[HTTP2Request]
        | List[HTTP3Request]
        | List[PlaywrightResult]
        | List[UDPResponse]
        | List[WebsocketResponse],
    ) -> ResultSet:
        if result_type == RequestType.PLAYWRIGHT:
            timing_results_set = [
                self._process_playwright_timings(result) for result in results
            ]

            timing_stats: Dict[
                Literal["total"],
                Dict[StatTypes, int | float],
            ] = {}

            results_types = ["total"]

        else:
            timing_results_set = [
                self._process_http_or_udp_timings(result) for result in results
            ]

            timing_stats: Dict[
                Literal[
                    "total",
                    "connecting",
                    "writing",
                    "reading",
                ],
                Dict[StatTypes, int | float],
            ] = {}

            results_types = [
                "total",
                "connecting",
                "writing",
                "reading",
            ]

        for result_type in results_types:
            results_set = [
                timing_result[result_type]
                for timing_result in timing_results_set
                if timing_result.get(result_type) is not None
            ]

            if len(results_set) > 0:
                timing_stats[result_type] = self._calculate_stats(results_set)
                timing_stats[result_type].update(
                    self._calculate_quantiles(results_set),
                )

        checks = Counter([result.check() for result in results])

        contexts = Counter(
            [result.context() for result in results if result.context() is not None]
        )

        statuses = Counter([result.status for result in results])

        return {
            "workflow": workflow,
            "step": step_name,
            "timings": timing_stats,
            "counts": {
                "succeeded": checks.get(True),
                "failed": checks.get(False),
                "statuses": {code: count for code, count in statuses.items()},
            },
            "contexts": [
                {
                    "context": context,
                    "count": count,
                }
                for context, count in contexts.items()
            ],
        }

    def _process_playwright_timings(self, result: PlaywrightResult):
        timings = result.timings

        timing_results: Dict[
            Optional[Literal["total"]],
            int | float,
        ] = {
            "total": timings["command_end"] - timings["command_start"],
        }

        return timing_results

    def _process_http_or_udp_timings(
        self,
        result: GraphQLResponse
        | GraphQLHTTP2Response
        | GRPCResponse
        | HTTPRequest
        | HTTP2Request
        | HTTP3Request
        | UDPResponse
        | WebsocketResponse,
    ) -> Dict[
        Optional[
            Literal[
                "total",
                "connecting",
                "writing",
                "reading",
            ]
        ],
        int | float,
    ]:
        timings = result.timings

        timing_results: Dict[
            Optional[
                Literal[
                    "total",
                    "connecting",
                    "writing",
                    "reading",
                ]
            ],
            int | float,
        ] = {}

        if (request_end := timings.get("request_end")) and (
            request_start := timings.get("request_start")
        ):
            timing_results["total"] = request_end - request_start

        if (connect_end := timings.get("connect_end")) and (
            connect_start := timings.get("connect_start")
        ):
            timing_results["connecting"] = connect_end - connect_start

        if (read_end := timings.get("read_end")) and (
            read_start := timings.get("read_start")
        ):
            timing_results["reading"] = read_end - read_start

        if (write_end := timings.get("write_end")) and (
            write_start := timings.get("write_start")
        ):
            timing_results["writing"] = write_end - write_start

        return timing_results
