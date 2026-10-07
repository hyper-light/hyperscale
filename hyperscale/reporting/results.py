from collections import Counter, defaultdict
from fractions import Fraction
from itertools import chain
from operator import itemgetter
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Type,
    get_args,
)

from hyperscale.core.engines.client.custom import CustomResult
from hyperscale.core.engines.client.ftp import FTPResponse
from hyperscale.core.engines.client.graphql import GraphQLResponse
from hyperscale.core.engines.client.graphql_http2 import GraphQLHTTP2Response
from hyperscale.core.engines.client.grpc import GRPCResponse
from hyperscale.core.engines.client.http import HTTPResponse
from hyperscale.core.engines.client.http2 import HTTP2Response
from hyperscale.core.engines.client.http3 import HTTP3Response
from hyperscale.core.engines.client.playwright import PlaywrightResult
from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.engines.client.scp import SCPResponse
from hyperscale.core.engines.client.scp.models.scp.scp_response import SCPTimings
from hyperscale.core.engines.client.smtp import SMTPResponse, SMTPTimings
from hyperscale.core.engines.client.sftp import SFTPResponse, SFTPTimings
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
    CountMetric,
    CountResults,
    DistributionMetric,
    MetricsSet,
    MetricType,
    MetricValue,
    RateMetric,
    ResultSet,
    SampleDigestState,
    SampleMetric,
    StatsResults,
    WorkflowStats,
)


from .sample_digest import SampleDigest
from .timings_aggregate import TimingsAggregate

# A TEST step's result: a client's response, or the error raised instead.
TestResult = (
    CustomResult
    | FTPResponse
    | GraphQLResponse
    | GraphQLHTTP2Response
    | GRPCResponse
    | HTTPResponse
    | HTTP2Response
    | HTTP3Response
    | PlaywrightResult
    | SCPResponse
    | SFTPResponse
    | SMTPResponse
    | TCPResponse
    | UDPResponse
    | WebsocketResponse
    | Exception
)

# Any step's result: a TEST step's, a METRIC step's metric, or none.
StepResult = TestResult | Metric | None

# Clients whose responses carry a status code (HTTP, gRPC, the WebSocket
# handshake, SMTP replies). Other clients do not produce one.
STATUS_REQUEST_TYPES = frozenset(
    {
        RequestType.GRAPHQL,
        RequestType.GRAPHQL_HTTP2,
        RequestType.GRPC,
        RequestType.HTTP,
        RequestType.HTTP2,
        RequestType.HTTP3,
        RequestType.SMTP,
        RequestType.WEBSOCKET,
    }
)


class Results:
    def __init__(
        self,
        hooks: Dict[str, Hook] | None = None,
        precision: int = 8,
    ) -> None:
        self._result_type: Dict[
            Type[CustomResult]
            | Type[FTPResponse]
            | Type[GraphQLResponse]
            | Type[GraphQLHTTP2Response]
            | Type[GRPCResponse]
            | Type[HTTPResponse]
            | Type[HTTP2Response]
            | Type[HTTP3Response]
            | Type[PlaywrightResult]
            | Type[SCPResponse]
            | Type[SFTPResponse]
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
                    | List[FTPResponse]
                    | List[GraphQLResponse]
                    | List[GraphQLHTTP2Response]
                    | List[GRPCResponse]
                    | List[HTTPResponse]
                    | List[HTTP2Response]
                    | List[HTTP3Response]
                    | List[PlaywrightResult]
                    | List[SMTPResponse]
                    | List[SFTPResponse]
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
        self._step_processors: Dict[
            HookType,
            Callable[[str, str, TimingsAggregate | List[StepResult], WorkflowStats], None],
        ] = {
            HookType.TEST: self._add_test_step,
            HookType.METRIC: self._add_metric_step,
            HookType.CHECK: self._add_check_step,
        }
        self._metric_stats: Dict[MetricType, Callable[[SampleDigest], MetricValue]] = {
            "COUNT": self._count_stats,
            "DISTRIBUTION": self._distribution_stats,
            "SAMPLE": self._sample_stats,
            "TIMING": self._sample_stats,
            "RATE": self._rate_stats,
        }

    def process(
        self,
        workflow: str,
        results: Dict[
            str,
            List[StepResult],
        ],
        elapsed: float,
        run_id: Optional[int] = None,
    ) -> WorkflowStats:
        aggregates = self.create_aggregates(results)

        for step, step_results in results.items():
            for result in step_results:
                self.aggregate_result(aggregates, step, result)

        return self.process_aggregates(
            workflow,
            aggregates,
            elapsed,
            run_id=run_id,
        )

    def create_aggregates(
        self,
        steps: Iterable[str],
    ) -> Dict[str, TimingsAggregate | List[StepResult]]:
        """
        One aggregate per step: a TEST step's results are reduced as they are
        added (``aggregate_result``); any other step keeps its results.
        """
        return {
            step: TimingsAggregate()
            if self._hooks[step].hook_type == HookType.TEST
            else []
            for step in steps
        }

    def aggregate_result(
        self,
        aggregates: Dict[str, TimingsAggregate | List[StepResult]],
        step: str,
        result: StepResult,
    ) -> None:
        """Add one of a step's results to that step's aggregate."""
        hook = self._hooks[step]

        if hook.hook_type == HookType.TEST:
            self._aggregate_test_result(
                aggregates[step],
                hook.engine_type,
                result,
            )

        else:
            aggregates[step].append(result)

    def process_aggregates(
        self,
        workflow: str,
        aggregates: Dict[str, TimingsAggregate | List[StepResult]],
        elapsed: float,
        run_id: Optional[int] = None,
    ) -> WorkflowStats:
        """
        A run's step aggregates as its workflow stats: every TEST step's
        results and counts (the workflow's counts are their sums), every
        METRIC and CHECK step's set, and the actions per second -- the
        executed count over ``elapsed``, rounded once.
        """
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

        for step, step_aggregate in aggregates.items():
            self._process_step_aggregate(workflow, step, step_aggregate, workflow_stats)

        workflow_stats["aps"] = float(
            Fraction(workflow_stats["stats"]["executed"]) / Fraction(elapsed)
        )

        return workflow_stats

    def _process_step_aggregate(
        self,
        workflow: str,
        step: str,
        step_aggregate: TimingsAggregate | List[StepResult],
        workflow_stats: WorkflowStats,
    ) -> None:
        """Add one step's set to the workflow stats (an ACTION step has none)."""
        if (step_processor := self._step_processors.get(self._hooks[step].hook_type)) is not None:
            step_processor(workflow, step, step_aggregate, workflow_stats)

    def _add_test_step(
        self,
        workflow: str,
        step: str,
        step_aggregate: TimingsAggregate,
        workflow_stats: WorkflowStats,
    ) -> None:
        test_results = self._process_timings_aggregate(workflow, step, step_aggregate)
        workflow_stats["results"].append(test_results)

        step_counts = test_results["counts"]
        workflow_counts = workflow_stats["stats"]
        workflow_counts["executed"] += step_counts["executed"]
        workflow_counts["succeeded"] += step_counts["succeeded"]
        workflow_counts["failed"] += step_counts["failed"]

    def _add_metric_step(
        self,
        workflow: str,
        step: str,
        step_aggregate: List[StepResult],
        workflow_stats: WorkflowStats,
    ) -> None:
        hook = self._hooks[step]
        workflow_stats["metrics"].append(
            self._process_metrics_set(
                workflow,
                step,
                hook.metric_type,
                hook.tags,
                step_aggregate,
            )
        )

    def _add_check_step(
        self,
        workflow: str,
        step: str,
        step_aggregate: List[Exception | None],
        workflow_stats: WorkflowStats,
    ) -> None:
        workflow_stats["checks"].append(
            self._process_check_set(workflow, step, step_aggregate)
        )

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
        metric_type: COUNT | DISTRIBUTION | SAMPLE | RATE | TIMING,
        tags: List[str],
        metrics: List[int | float] | List[tuple[int | float, float]],
    ) -> MetricsSet:
        """
        A METRIC step's values as its set. The set carries the digest it is
        merged by: of the values, or for a RATE, of this source's rate.
        """
        (metric_name,) = get_args(metric_type)
        digest = self._metric_digest(metric_name, metrics)

        return {
            "workflow": workflow,
            "step": step_name,
            "metric_type": metric_name,
            "stats": self._metric_stats[metric_name](digest),
            "digest": digest.to_state(),
            "tags": tags,
        }

    def _metric_digest(
        self,
        metric_name: MetricType,
        metrics: List[int | float] | List[tuple[int | float, float]],
    ) -> SampleDigest:
        if metric_name == "RATE":
            return SampleDigest.from_values([self._source_rate(metrics)])
        return SampleDigest.from_values(metrics)

    def _source_rate(self, metrics: List[tuple[int | float, float]]) -> float:
        """One source's rate: its values' exact sum over the span of their timestamps, rounded once."""
        values_sum = SampleDigest.from_values([value for value, _ in metrics]).exact_sum()
        timestamps = [timestamp for _, timestamp in metrics]
        return float(values_sum / (Fraction(max(timestamps)) - Fraction(min(timestamps))))

    def _count_stats(self, digest: SampleDigest) -> CountMetric:
        return {"count": digest.total()}

    def _distribution_stats(self, digest: SampleDigest) -> DistributionMetric:
        stats: DistributionMetric = digest.quantile_stats(self._quantiles)
        stats["max"] = digest.maximum
        stats["min"] = digest.minimum
        return stats

    def _sample_stats(self, digest: SampleDigest) -> SampleMetric:
        return digest.stats(self._quantiles)

    def _rate_stats(self, digest: SampleDigest) -> RateMetric:
        """Concurrent sources' rates add: the exact sum of every source's rate, rounded once."""
        return {"rate": float(digest.exact_sum())}


    def _aggregate_test_result(
        self,
        aggregate: TimingsAggregate,
        result_type: RequestType | str,
        result: TestResult,
    ) -> None:
        if isinstance(result, Exception):
            aggregate.errors += 1
            aggregate.error_contexts[str(result)] += 1
            return

        match result_type:
            case RequestType.PLAYWRIGHT:
                timings = self._process_playwright_timings(result)

            case RequestType.CUSTOM:
                timings = (
                    result.process_timings()
                    if isinstance(result, CustomResult)
                    else None
                )

            case RequestType.SCP:
                timings = self._process_scp_timings(result)

            case RequestType.SFTP:
                timings = self._process_sftp_timings(result)

            case RequestType.SMTP:
                timings = self._process_smtp_timings(result)

            case _:
                timings = self._process_http_or_udp_timings(result)

        if timings is not None:
            if aggregate.timing_types is None:
                aggregate.timing_types = self._timing_types(result_type, timings)

            timing_values = aggregate.timing_values
            for timing_type in aggregate.timing_types:
                if (value := timings.get(timing_type)) is not None:
                    timing_values[timing_type].append(value)

        aggregate.successes[result.successful] += 1

        if result_type in STATUS_REQUEST_TYPES:
            aggregate.statuses[result.status] += 1

        if (context := result.context()) is not None:
            aggregate.result_contexts[context] += 1

    def _timing_types(
        self,
        result_type: RequestType | str,
        timings: Dict[str, int | float],
    ) -> List[str]:
        match result_type:
            case RequestType.PLAYWRIGHT:
                return ["total"]

            case RequestType.CUSTOM:
                # A custom result names its own timings.
                return list(timings.keys())

            case RequestType.SCP:
                return [
                    "total",
                    "connecting",
                    "initializing",
                    "transferring",
                ]

            case RequestType.SFTP:
                return [
                    "total",
                    "connecting",
                    "initializing",
                    "executing",
                    "closing",
                ]

            case RequestType.SMTP:
                return [
                    "total",
                    "connecting",
                    "ehlo",
                    "tls_check",
                    "tls_upgrade",
                    "ehlo_tls",
                    "login",
                    "send_mail",
                ]

            case _:
                return [
                    "total",
                    "connecting",
                    "writing",
                    "reading",
                ]

    def _process_timings_aggregate(
        self,
        workflow: str,
        step_name: str,
        aggregate: TimingsAggregate,
    ) -> ResultSet:
        digests = {
            timing_type: SampleDigest.from_values(timing_values)
            for timing_type in aggregate.timing_types or ()
            if (timing_values := aggregate.timing_values.get(timing_type))
        }

        succeeded = aggregate.successes.get(True, 0)
        unsucceeded = aggregate.successes.get(False, 0)

        # Results' contexts are listed before errors' contexts.
        contexts = Counter(aggregate.result_contexts)
        contexts.update(aggregate.error_contexts)

        return {
            "workflow": workflow,
            "step": step_name,
            "timings": self._timing_stats(digests),
            "digests": self._digest_states(digests),
            "counts": {
                "executed": succeeded + unsucceeded + aggregate.errors,
                "succeeded": succeeded,
                "failed": unsucceeded + aggregate.errors,
                "statuses": {
                    code: count for code, count in aggregate.statuses.items()
                },
            },
            "contexts": [
                {
                    "context": context,
                    "count": count,
                }
                for context, count in contexts.items()
            ],
        }

    def _timing_stats(self, digests: Dict[str, SampleDigest]) -> Dict[str, StatsResults]:
        return {
            timing_type: digest.stats(self._quantiles)
            for timing_type, digest in digests.items()
        }

    def _digest_states(self, digests: Dict[str, SampleDigest]) -> Dict[str, SampleDigestState]:
        return {timing_type: digest.to_state() for timing_type, digest in digests.items()}

    def merge_results(
        self,
        workflow_stats_set: List[WorkflowStats],
        run_id: int | None = None,
    ) -> WorkflowStats:
        """
        Workflow stats from many sources (worker cores, workers,
        datacenters) merged into one. Counts add, digests merge exactly, and
        every statistic is computed from the merged digests, so merging in
        any order and grouping equals processing the union of the samples.
        The sources run concurrently: the merged elapsed is the longest, and
        the actions per second are every action over it, rounded once.
        """
        merged: WorkflowStats = {
            "workflow": workflow_stats_set[0]["workflow"],
            "stats": self._aggregate_counts(list(map(itemgetter("stats"), workflow_stats_set))),
            "results": self._merge_timing_results(self._flatten(workflow_stats_set, "results")),
            "checks": self._merge_check_results(self._flatten(workflow_stats_set, "checks")),
            "metrics": self._merge_metric_results(self._flatten(workflow_stats_set, "metrics")),
        }

        if run_id:
            merged["run_id"] = run_id

        elapsed = max(map(itemgetter("elapsed"), workflow_stats_set))
        merged["aps"] = float(Fraction(merged["stats"]["executed"]) / Fraction(elapsed))
        merged["elapsed"] = elapsed

        return merged

    def _flatten(
        self,
        workflow_stats_set: List[WorkflowStats],
        sets_key: Literal["results", "checks", "metrics"],
    ) -> List[ResultSet] | List[CheckSet] | List[MetricsSet]:
        """Every source's result, check or metric sets, in one list."""
        return list(chain.from_iterable(map(itemgetter(sets_key), workflow_stats_set)))

    def _merge_timing_results(self, results: List[ResultSet]) -> List[ResultSet]:
        return [
            self._merge_step_results(step_results[0]["workflow"], step_name, step_results)
            for step_name, step_results in self._bin_by_step(results).items()
        ]

    def _merge_step_results(
        self,
        workflow: str,
        step_name: str,
        step_results: List[ResultSet],
    ) -> ResultSet:
        digests = self._merge_digests(
            [self._source_digest(result_set, "digests") for result_set in step_results]
        )

        return {
            "workflow": workflow,
            "step": step_name,
            "timings": self._timing_stats(digests),
            "digests": self._digest_states(digests),
            "counts": self._aggregate_counts(list(map(itemgetter("counts"), step_results))),
            "contexts": self._aggregate_contexts(step_results),
        }

    def _merge_digests(
        self,
        digest_states: List[Dict[str, SampleDigestState]],
    ) -> Dict[str, SampleDigest]:
        """Each timing's digests merged, in the order the timings first appear."""
        grouped_digests: Dict[str, List[SampleDigest]] = defaultdict(list)

        for states in digest_states:
            self._group_digest_states(grouped_digests, states)

        return {
            timing_type: SampleDigest.merge_all(digests)
            for timing_type, digests in grouped_digests.items()
        }

    def _group_digest_states(
        self,
        grouped_digests: Dict[str, List[SampleDigest]],
        states: Dict[str, SampleDigestState],
    ) -> None:
        for timing_type, digest_state in states.items():
            grouped_digests[timing_type].append(SampleDigest.from_state(digest_state))

    def _source_digest(
        self,
        result_set: ResultSet | MetricsSet,
        digest_key: Literal["digest", "digests"],
    ) -> Dict[str, SampleDigestState] | SampleDigestState:
        """A source's digest state: a set without one cannot be merged exactly."""
        if (digest_state := result_set.get(digest_key)) is None:
            raise ValueError(
                f"Step {result_set.get('step')} carries no {digest_key}: "
                "every source must report sample digests to be merged"
            )
        return digest_state

    def _bin_by_step(
        self,
        results: List[ResultSet] | List[CheckSet] | List[MetricsSet],
    ) -> Dict[str, List[ResultSet] | List[CheckSet] | List[MetricsSet]]:
        binned: Dict[str, List[ResultSet] | List[CheckSet] | List[MetricsSet]] = defaultdict(list)

        for result in results:
            binned[result["step"]].append(result)

        return binned

    def _merge_check_results(self, results: List[CheckSet]) -> List[CheckSet]:
        return [
            {
                "workflow": check_results[0]["workflow"],
                "step": step_name,
                "counts": self._aggregate_counts(list(map(itemgetter("counts"), check_results))),
                "contexts": self._aggregate_contexts(check_results),
            }
            for step_name, check_results in self._bin_by_step(results).items()
        ]

    def _merge_metric_results(self, results: List[MetricsSet]) -> List[MetricsSet]:
        return [
            self._merge_step_metrics(step_name, metric_results)
            for step_name, metric_results in self._bin_by_step(results).items()
        ]

    def _merge_step_metrics(
        self,
        step_name: str,
        metric_results: List[MetricsSet],
    ) -> MetricsSet:
        metric_name: MetricType = metric_results[0]["metric_type"]
        digest = SampleDigest.merge_all(
            [
                SampleDigest.from_state(self._source_digest(metric_set, "digest"))
                for metric_set in metric_results
            ]
        )

        return {
            "workflow": metric_results[0]["workflow"],
            "step": step_name,
            "metric_type": metric_name,
            "stats": self._metric_stats[metric_name](digest),
            "digest": digest.to_state(),
            "tags": metric_results[0]["tags"],
        }

    def _aggregate_counts(self, counts: List[CountResults]) -> CountResults:
        """Every source's counts added (exactly: ints never overflow)."""
        aggregate_counts: CountResults = {
            count_type: sum(map(itemgetter(count_type), counts))
            for count_type in ("succeeded", "failed", "executed")
        }

        if status_counts := self._aggregate_statuses(counts):
            aggregate_counts["statuses"] = dict(status_counts)

        return aggregate_counts

    def _aggregate_statuses(self, counts: List[CountResults]) -> Counter[int]:
        status_counts: Counter[int] = Counter()
        for count_results in counts:
            status_counts.update(count_results.get("statuses", {}))
        return status_counts

    def _aggregate_contexts(self, results: List[ResultSet]) -> List[ContextCount]:
        context_counts: Counter[str] = Counter()
        for result_set in results:
            for context_count in result_set["contexts"]:
                context_counts[context_count["context"]] += context_count["count"]

        return [
            {
                "context": context_name,
                "count": count,
            }
            for context_name, count in context_counts.items()
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
            timing_results['tls_upgrade'] = tls_upgrade_end - tls_upgrade_start

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
        | HTTPResponse
        | HTTP2Response
        | HTTP3Response
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
    
    def _process_scp_timings(
        self,
        result: SCPResponse,
    ) -> dict[
        Literal[
            "total",
            "connecting",
            "initializing",
            "transferring",
        ],
        int | float
    ]:
        timings = result.timings
        timing_results: Dict[
            Literal[
                "total",
                "connecting",
                "initializing",
                "transferring",
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

        if (initialization_end := timings.get("initialization_end")) and (
            initialization_start := timings.get("initialization_start")
        ):
            timing_results["initializing"] = initialization_end - initialization_start

        if (transfer_end := timings.get("transfer_end")) and (
            transfer_start := timings.get("transfer_start")
        ):
            timing_results["transferring"] = transfer_end - transfer_start

        return timing_results
    
    def _process_sftp_timings(
        self,
        result: SFTPResponse,
    ) -> dict[
        Literal[
            "total",
            "connecting",
            "initializing",
            "executing",
            "closing",
        ],
        int | float
    ]:
        
        timings = result.timings
        timing_results: Dict[
            Literal[
                "total",
                "connecting",
                "initializing",
                "executing",
                "closing",
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
            
        if (initialization_end := timings.get("initialization_end")) and (
            initialization_start := timings.get("initialization_start")
        ):
            timing_results["initializing"] = initialization_end - initialization_start

        if (execution_end := timings.get("execution_end")) and (
            execution_start := timings.get("execution_start")
        ):
            timing_results["executing"] = execution_end - execution_start

        if (close_end := timings.get("close_end")) and (
            close_start := timings.get("close_start")
        ):
            timing_results["closing"] = close_end - close_start

        return timing_results