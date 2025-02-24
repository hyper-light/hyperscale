import statistics
from collections import Counter, defaultdict
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

from hyperscale.core.engines.client.custom import CustomResult
from hyperscale.core.engines.client.graphql import GraphQLResponse
from hyperscale.core.engines.client.graphql_http2 import GraphQLHTTP2Response
from hyperscale.core.engines.client.grpc import GRPCResponse
from hyperscale.core.engines.client.http import HTTPRequest
from hyperscale.core.engines.client.http2 import HTTP2Request
from hyperscale.core.engines.client.http3 import HTTP3Request
from hyperscale.core.engines.client.playwright import PlaywrightResult
from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.engines.client.smtp import SMTPResponse
from hyperscale.core.engines.client.tcp import TCPResponse
from hyperscale.core.engines.client.udp import UDPResponse
from hyperscale.core.engines.client.websocket import WebsocketResponse
from hyperscale.core.hooks import Hook, HookType

from .models.metric import (
    COUNT,
    DISTRIBUTION,
    RATE,
    SAMPLE,
    TIMING,
    Metric,
)
from hyperscale.reporting.common.results_types import (
    CheckSet,
    ContextCount,
    CountResults,
    MetricsSet,
    MetricType,
    MetricValue,
    QuantileSet,
    ResultSet,
    StatsResults,
    StatTypes,
    WorkflowStats,
)


class Results:
    def __init__(
        self,
        hooks: Dict[str, Hook] | None = None,
        precision: int = 8,
    ) -> None:
        self._result_type: Dict[
            Type[CustomResult]
            | Type[GraphQLResponse]
            | Type[GraphQLHTTP2Response]
            | Type[GRPCResponse]
            | Type[HTTPRequest]
            | Type[HTTP2Request]
            | Type[HTTP3Request]
            | Type[PlaywrightResult]
            | Type[SMTPResponse]
            | Type[TCPResponse]
            | Type[UDPResponse]
            | Type[WebsocketResponse]
            | Type[Metric]
            | Type[Exception],
            Callable[
                [
                    str,
                    List[CustomResult]
                    | List[GraphQLResponse]
                    | List[GraphQLHTTP2Response]
                    | List[GRPCResponse]
                    | List[HTTPRequest]
                    | List[HTTP2Request]
                    | List[HTTP3Request]
                    | List[PlaywrightResult]
                    | List[SMTPResponse]
                    | List[TCPResponse]
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
        elapsed: float,
        run_id: Optional[int] = None,
    ) -> WorkflowStats:
        workflow_stats: WorkflowStats = {
            "workflow": workflow,
            "elapsed": elapsed,
            "stats": {"executed": 0, "succeeded": 0, "failed": 0},
            "results": [],
            "checks": [],
            "metrics": [],
        }

        if run_id:
            workflow_stats["run"] = run_id

        for step in results:
            step_results = results[step]

            hook = self._hooks[step]
            hook_type = hook.hook_type

            executed: int = 0

            match hook_type:
                case HookType.TEST:
                    test_results = self._process_timings_set(
                        workflow, 
                        step, 
                        hook.engine_type,
                        step_results,
                    )

                    workflow_stats["results"].append(test_results)

                    executed += test_results["counts"]["executed"]

                    workflow_stats["stats"]["executed"] = executed
                    workflow_stats["stats"]["succeeded"] += test_results["counts"][
                        "succeeded"
                    ]
                    workflow_stats["stats"]["failed"] += test_results["counts"][
                        "failed"
                    ]

                case HookType.METRIC:
                    workflow_stats["metrics"].append(
                        self._process_metrics_set(
                            workflow,
                            step,
                            hook.metric_type,
                            hook.tags,
                            step_results,
                        )
                    )

                case HookType.CHECK:
                    workflow_stats["checks"].append(
                        self._process_check_set(
                            workflow,
                            step,
                            step_results,
                        )
                    )

                case _:
                    pass

        workflow_stats["aps"] = executed / elapsed

        return workflow_stats

    def _process_check_set(
        self,
        workflow: str,
        step_name: str,
        exceptions: List[Exception | None],
    ) -> CheckSet:
        executed = len(exceptions)

        failed_contexts = Counter([str(err) for err in exceptions if err is not None])

        failed = sum(failed_contexts.values())

        return {
            "workflow": workflow,
            "step": step_name,
            "counts": {
                "executed": executed,
                "succeeded": executed - failed,
                "failed": failed,
            },
            "contexts": [
                {
                    "context": context,
                    "count": count,
                }
                for context, count in failed_contexts.items()
            ],
        }

    def _process_metrics_set(
        self,
        workflow: str,
        step_name: str,
        metric_type: COUNT | DISTRIBUTION | SAMPLE | RATE,
        tags: List[str],
        metrics: List[Metric],
    ) -> MetricsSet:
        if metric_type == COUNT:
            return {
                "workflow": workflow,
                "step": step_name,
                "metric_type": "COUNT",
                "stats": {
                    "count": sum(metrics),
                },
                "tags": tags,
            }

        elif metric_type == DISTRIBUTION:
            stats = self._calculate_quantiles(metrics)
            stats["max"] = max(metrics)

            stats["min"] = min(metrics)

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
            values = [metric[0] for metric in metrics]
            times = [metric[1] for metric in metrics]

            elapsed = max(times) - min(times)

            return {
                "workflow": workflow,
                "step": step_name,
                "metric_type": "RATE",
                "stats": {
                    "rate": sum(values) / elapsed,
                },
                "tags": tags,
            }
        
        elif metric_type == TIMING:
            stats = self._calculate_stats(metrics)
            stats.update(
                self._calculate_quantiles(metrics)
            )
            return {
                "workflow": workflow,
                "step": step_name,
                "metric_type": "TIMING",
                "stats": stats,
                "tags": tags,
            }

    def _calculate_quantiles(self, values: List[int | float]):
        return {
            f"{quantile}th_quantile": float(value)
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
            "mean": mean,
            "max": max(values),
            "min": min(values),
            "med": float(np.median(values)),
            "stdev": float(np.std(values)),
            "var": float(np.var(values)),
            "mad": float(np.mean([abs(el - mean) for el in values])),
        }

    def _process_timings_set(
        self,
        workflow: str,
        step_name: str,
        result_type: RequestType | str,
        results: List[GraphQLResponse]
        | List[GraphQLHTTP2Response]
        | List[GRPCResponse]
        | List[HTTPRequest]
        | List[HTTP2Request]
        | List[HTTP3Request]
        | List[PlaywrightResult]
        | List[SMTPResponse]
        | List[TCPResponse]
        | List[UDPResponse]
        | List[WebsocketResponse],
    ) -> ResultSet:
        errors = [error for error in results if isinstance(error, Exception)]

        results = [result for result in results if result not in errors]

        if result_type == RequestType.PLAYWRIGHT:
            timing_results_set = [
                self._process_playwright_timings(result)
                for result in results
                if result not in errors
            ]

            timing_stats: Dict[
                Literal["total"],
                Dict[StatTypes, int | float],
            ] = {}

            results_types = ["total"]

        elif result_type == RequestType.CUSTOM:
            timing_results_set = [
                result.process_timings()
                for result in results
                if isinstance(result, CustomResult)
            ]

            timing_stats: dict[
                str,
                dict[StatTypes, int | float]
            ] = {}

            results_types = list(timing_results_set[0].keys())

        elif result_type == RequestType.SMTP:
            timing_results_set = [
                self._process_smtp_timings(result)
                for result in results
                if result not in errors
            ]

            timing_stats: Dict[
                Literal[
                    "total",
                    "connecting",
                    "ehlo",
                    "tls_check",
                    "tls_upgrade",
                    "ehlo_tls",
                    "login",
                    "send_mail",
                ],
                Dict[StatTypes, int | float],
            ] = {}

            results_types = [
                "total",
                "connecting",
                "ehlo",
                "tls_check",
                "tls_upgrade",
                "ehlo_tls",
                "login",
                "send_mail",
            ]


        else:
            timing_results_set = [
                self._process_http_or_udp_timings(result)
                for result in results
                if result not in errors
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

        checks = Counter([result.successful for result in results])
        statuses = Counter([
            result.status for result in results
            if not result_type == RequestType.PLAYWRIGHT and not isinstance(result_type, str)
        ])

        result_contexts = [
            result.context() for result in results if result.context() is not None
        ]
        errors_count = len(errors)
        error_contexts = [str(error) for error in errors]
        result_contexts.extend(error_contexts)

        contexts = Counter(result_contexts)

        return {
            "workflow": workflow,
            "step": step_name,
            "timings": timing_stats,
            "counts": {
                "executed": checks.get(True, 0) + checks.get(False, 0) + errors_count,
                "succeeded": checks.get(True, 0),
                "failed": checks.get(False, 0) + errors_count,
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

    def merge_results(
        self,
        workflow_stats_set: List[WorkflowStats],
        run_id: int | None = None,
    ) -> WorkflowStats:
        timing_results = [
            result_set
            for workflow_stats in workflow_stats_set
            for result_set in workflow_stats["results"]
        ]

        checks = [
            check_set
            for workflow_stats in workflow_stats_set
            for check_set in workflow_stats["checks"]
        ]

        metrics = [
            metric_set
            for workflow_stats in workflow_stats_set
            for metric_set in workflow_stats["metrics"]
        ]

        aggregate_workflow_stats = self._aggregate_counts(
            [
                {
                    "counts": workflow_stats["stats"],
                }
                for workflow_stats in workflow_stats_set
            ]
        )

        merged_timing_results = self._merge_timing_results(timing_results)

        merged: WorkflowStats = {
            "workflow": merged_timing_results[0].get("workflow"),
            "stats": aggregate_workflow_stats,
            "results": merged_timing_results,
            "checks": [],
            "metrics": [],
        }

        if len(checks) > 0:
            merged["checks"].extend(self._merge_check_results(checks))

        if len(metrics) > 0:
            merged["metrics"].extend(self._merge_metric_results(metrics))

        if run_id:
            merged["run_id"] = run_id

        median_elapsed: float = statistics.median(
            [workflow_set["elapsed"] for workflow_set in workflow_stats_set]
        )
        total_executed: int = sum(
            [workflow_set["stats"]["executed"] for workflow_set in workflow_stats_set]
        )

        merged["aps"] = total_executed / median_elapsed
        merged["elapsed"] = median_elapsed

        return merged

    def _merge_timing_results(self, results: List[ResultSet]):
        workflow = results[0].get("workflow")

        binned_timings: Dict[str, List[CheckSet]] = defaultdict(list)

        for result in results:
            binned_timings[result["step"]].append(result)

        merged_timings: List[ResultSet] = []

        for step_name, timings_result in binned_timings.items():
            aggregate_timings = self._aggregate_timings(timings_result)
            aggregate_counts = self._aggregate_counts(timings_result)
            aggregate_contexts = self._aggregate_contexts(timings_result)

            merged_timings.append(
                {
                    "workflow": workflow,
                    "step": step_name,
                    "timings": aggregate_timings,
                    "counts": aggregate_counts,
                    "contexts": aggregate_contexts,
                }
            )

        return merged_timings

    def _merge_check_results(self, results: List[CheckSet]):
        workflow = results[0].get("workflow")

        binned_checks: Dict[str, List[CheckSet]] = defaultdict(list)

        for result in results:
            binned_checks[result["step"]].append(result)

        merged_checks: List[CheckSet] = []

        for step_name, check_results in binned_checks.items():
            aggregate_counts = self._aggregate_counts(check_results)
            aggregate_contexts = self._aggregate_contexts(check_results)

            merged_checks.append(
                {
                    "workflow": workflow,
                    "step": step_name,
                    "counts": aggregate_counts,
                    "contexts": aggregate_contexts,
                }
            )

        return merged_checks

    def _merge_metric_results(self, results: List[MetricsSet]):
        workflow = results[0].get("workflow")
        binned_metrics: Dict[str, List[MetricsSet]] = defaultdict(list)

        for result in results:
            binned_metrics[result["step"]].append(result)

        merged_metrics: List[MetricsSet] = []
        for step_name, metric_results in binned_metrics.items():
            metric_type = metric_results[0].get("metric_type")
            tags = metric_results[0].get("tags")

            merged_metrics.append(
                {
                    "workflow": workflow,
                    "step": step_name,
                    "metric_type": metric_type,
                    "stats": self._aggregate_metrics(metric_results),
                    "tags": tags,
                }
            )

    def _aggregate_metrics(self, metrics: List[MetricsSet]) -> MetricValue:
        metric_type: MetricType = metrics[0].get("metric_type")

        if metric_type == COUNT:
            return {"count": sum([metric["stats"]["count"] for metric in metrics])}

        elif metric_type == DISTRIBUTION or metric_type == SAMPLE:
            return self._aggregate_stats(
                [metric["stats"] for metric in metrics],
            )

        elif metric_type == RATE:
            return {
                "rate": sum([metric["stats"]["rate"] for metric in metrics]),
            }
        
        elif metric_type == TIMING:
            return {
                "timing": statistics.median(metrics["stats"]["timing"]) for metric in metrics
            }

        else:
            raise Exception(f"Err. - Invalid metric type - {metric_type}")

    def _aggregate_timings(
        self, results: List[ResultSet]
    ) -> Dict[str, StatsResults | QuantileSet]:
        stats_by_name: Dict[str, List[StatsResults | QuantileSet]] = defaultdict(list)

        [
            stats_by_name[stat_name].append(stats)
            for results_set in results
            for stat_name, stats in results_set["timings"].items()
        ]

        aggregate_timings: Dict[str, StatsResults | QuantileSet] = {}
        for stat_name, timing_stats in stats_by_name.items():
            aggregate_timings[stat_name] = self._aggregate_stats(timing_stats)

        return aggregate_timings

    def _aggregate_stats(self, stats: List[StatsResults | QuantileSet]):
        aggregate_stat = {
            "max": max([stat["max"] for stat in stats]),
            "min": min([stat["min"] for stat in stats]),
        }

        grouped_stats: Dict[str, List[float]] = defaultdict(list)

        [
            grouped_stats[stat_type].append(value)
            for stat in stats
            for stat_type, value in stat.items()
            if stat_type not in aggregate_stat
        ]

        aggregate_stat.update(
            {
                stat_type: statistics.median(value)
                for stat_type, value in grouped_stats.items()
            }
        )

        return aggregate_stat

    def _aggregate_counts(self, results: List[ResultSet]) -> StatsResults:
        counts: List[CountResults] = [result["counts"] for result in results]

        aggregate_counts: CountResults = {
            "succeeded": sum([count_set["succeeded"] for count_set in counts]),
            "failed": sum([count_set["failed"] for count_set in counts]),
            "executed": sum([count_set["executed"] for count_set in counts]),
        }

        status_counts_data: Dict[str, List[int]] = defaultdict(list)

        [
            status_counts_data[status_type].append(count)
            for count_results in counts
            for status_type, count in count_results.get(
                "statuses",
                {},
            ).items()
        ]

        if len(status_counts_data) > 0:
            status_counts = {
                status_name: sum(counts)
                for status_name, counts in status_counts_data.items()
            }

            aggregate_counts["statuses"] = status_counts

        return aggregate_counts

    def _aggregate_contexts(self, results: List[ResultSet]) -> List[ContextCount]:
        context_counts_results: Dict[str, List[int]] = defaultdict(list)

        [
            context_counts_results[context_count["context"]].append(
                context_count["count"]
            )
            for result_set in results
            for context_count in result_set["contexts"]
        ]

        return [
            {
                "context": context_name,
                "count": sum(counts),
            }
            for context_name, counts in context_counts_results.items()
        ]

    def _process_playwright_timings(self, result: PlaywrightResult):
        timings = result.timings

        timing_results: Dict[
            Optional[Literal["total"]],
            int | float,
        ] = {
            "total": timings["command_end"] - timings["command_start"],
        }

        return timing_results
    
    def _process_smtp_timings(
        self,
        result: SMTPResponse
    ) -> Dict[
        Optional[
            Literal[
                "total",
                "connecting",
                "ehlo",
                "tls_check",
                "tls_upgrade",
                "ehlo_tls",
                "login",
                "send_mail",
            ]
        ],
        int | float
    ]:
        timings = result.timings
        timing_results: Dict[
            Optional[
                Literal[
                    "total",
                    "connecting",
                    "ehlo",
                    "tls_check",
                    "tls_upgrade",
                    "ehlo_tls",
                    "login",
                    "send_mail",
                ]
            ],
            int | float
        ] = {}

        if (request_end := timings.get("request_end")) and (
            request_start := timings.get("request_start")
        ):
            timing_results["total"] = request_end - request_start

        if (connect_end := timings.get("connect_end")) and (
            connect_start := timings.get("connect_start")
        ):
            timing_results["connecting"] = connect_end - connect_start

        if (ehlo_end := timings.get('ehlo_end')) and (
            ehlo_start := timings.get('ehlo_start')
        ):
            timing_results['ehlo'] = ehlo_end - ehlo_start

        if (tls_check_end := timings.get('tls_check_end')) and (
            tls_check_start := timings.get('tls_check_start')
        ):
            timing_results['tls_check'] = tls_check_end - tls_check_start

        if (tls_upgrade_end := timings.get('tls_upgrade_end')) and (
            tls_upgrade_start := timings.get('tls_upgrade_start')
        ):
            timing_results['tls_upgrade'] = tls_upgrade_end - tls_check_start

        if (ehlo_tls_end := timings.get('ehlo_tls_end')) and (
            ehlo_tls_start := timings.get('ehlo_tls_start')
        ):
            timing_results['ehlo_tls'] = ehlo_tls_end - ehlo_tls_start

        if (login_end := timings.get('login_end')) and (
            login_start := timings.get('login_start')
        ):
            timing_results['login'] = login_end - login_start

        if (send_email_end := timings.get('send_mail_end')) and (
            send_email_start := timings.get('send_mail_start')
        ):
            timing_results['send_mail'] = send_email_end - send_email_start

        return timing_results

    def _process_http_or_udp_timings(
        self,
        result: GraphQLResponse
        | GraphQLHTTP2Response
        | GRPCResponse
        | HTTPRequest
        | HTTP2Request
        | HTTP3Request
        | TCPResponse
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
