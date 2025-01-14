from typing import Dict, Literal, List, Any, Tuple


QuantileSet = Dict[str, int | float]
StatTypes = Literal["max", "min", "mean", "med", "stdev", "var", "mad"]

StatusCounts = Dict[int, int]
StatsResults = Dict[StatTypes, int | float] | QuantileSet
CountResults = Dict[
    Literal["succeeded", "failed", "executed"] | Literal["statuses"] | None,
    int | StatusCounts | None,
]


FailedResults = Dict[Literal["failed"], int]
ContextCount = Dict[Literal["context", "count"], str | int]
ContextResults = List[ContextCount]


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

CheckSet = Dict[
    Literal[
        "workflow",
        "step",
        "counts",
        "contexts",
    ],
    str | FailedResults | ContextResults,
]

MetricType = Literal["COUNT", "DISTRIBUTION", "SAMPLE", "RATE"]

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
        "tags",
    ],
    str | MetricType | MetricValue | List[str],
]


WorkflowStats = Dict[
    Literal["workflow", "stats", "results", "metrics", "checks", "elapsed", "rps"]
    | Literal["run_id"]
    | None,
    int
    | str
    | float
    | CountResults
    | List[ResultSet]
    | List[MetricsSet]
    | List[CheckSet],
]


WorkflowContextResult = Dict[str, Any | Exception]


WorkflowResultsSet = WorkflowStats | WorkflowContextResult


RunResults = Dict[
    Literal[
        "workflow",
        "results",
    ],
    str
    | Dict[
        str,
        WorkflowStats | WorkflowContextResult,
    ],
]
