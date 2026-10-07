from typing import (
    Dict,
    List,
    Literal,
)


QuantileSet = Dict[str, int | float]
StatTypes = Literal["max", "min", "mean", "med", "stdev", "var", "mad"]
CountTypes = Literal["succeeded", "failed", "executed"]

StatusCounts = Dict[int, int]
StatsResults = Dict[StatTypes, int | float] | QuantileSet
CountResults = Dict[
    CountTypes | Literal["statuses"],
    int | StatusCounts | None,
]


FailedResults = Dict[Literal["failed"], int]
ContextCount = Dict[Literal["context", "count"], str | int]
ContextResults = List[ContextCount]

# A sample digest's exact, mergeable state (see reporting/sample_digest.py):
# plain ints, floats and dicts, so any node unpickles it.
SampleDigestState = Dict[
    Literal[
        "count",
        "sum_units",
        "square_sum_units",
        "minimum",
        "maximum",
        "zero_count",
        "positive_buckets",
        "negative_buckets",
    ],
    int | float | Dict[int, int],
]

ResultSet = Dict[
    Literal[
        "workflow",
        "step",
        "timings",
        "digests",
        "counts",
        "contexts",
    ],
    str | StatsResults | Dict[str, SampleDigestState] | CountResults | ContextResults,
]

CheckSet = Dict[
    Literal[
        "workflow",
        "step",
        "counts",
        "contexts",
    ],
    str | FailedResults | ContextResults,
]

MetricType = Literal["COUNT", "DISTRIBUTION", "SAMPLE", "RATE", "TIMING"]

CountMetric = Dict[Literal["count"], int]

DistributionMetric = (
    QuantileSet
    | Dict[
        Literal["max", "min"],
        int | float,
    ]
)

SampleMetric = StatsResults | QuantileSet
RateMetric = Dict[Literal["rate"], int | float]
MetricValue = CountMetric | DistributionMetric | SampleMetric | RateMetric

MetricsSet = Dict[
    Literal[
        "workflow",
        "step",
        "metric_type",
        "stats",
        "digest",
        "tags",
    ],
    str | MetricType | MetricValue | SampleDigestState | List[str],
]

WorkflowStats = Dict[
    Literal["workflow", "stats", "results", "metrics", "checks", "elapsed", "aps"]
    | Literal["run_id"],
    int
    | str
    | float
    | CountResults
    | List[ResultSet]
    | List[MetricsSet]
    | List[CheckSet],
]


# A workflow context: user-set values, opaque here (an Exception when it failed).
WorkflowContextResult = Dict[str, object]


WorkflowResultsSet = WorkflowStats | WorkflowContextResult

TimeoutSet = Dict[str, Exception]


RunResults = Dict[
    Literal[
        "workflow",
        "results",
        "timeouts",
    ],
    str
    | Dict[
        str,
        WorkflowStats | WorkflowContextResult,
    ] | TimeoutSet,
]
