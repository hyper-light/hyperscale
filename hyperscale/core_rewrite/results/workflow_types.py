from typing import (
    Dict,
    List,
    Literal,
    Optional,
)

StatTypes = Literal["max", "min", "mean", "med", "stdev", "var", "mad"]

StatusCounts = Dict[int, int]
StatsResults = Dict[StatTypes, int | float]
CountResults = Dict[
    Literal["succeeded", "failed", "executed"] | Optional[Literal["statuses"]],
    int | Optional[StatusCounts],
]

QuantileSet = Dict[str, int | float]

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

CheckSet = Dict[
    Literal[
        "workflow",
        "step",
        "counts",
        "contexts",
    ],
    str | FailedResults | ContextResults,
]

MetricsSet = Dict[
    Literal[
        "workflow",
        "step",
        "metric_type",
        "stats",
        "tags",
    ],
    str
    | Literal["COUNT", "DISTRIBUTION", "SAMPLE", "RATE"]
    | Dict[str, int | float | StatsResults | QuantileSet]
    | List[str],
]

WorkflowStats = Dict[
    Literal["workflow", "stats", "results", "metrics", "checks"]
    | Optional[Literal["run_id"]],
    int | str | CountResults | List[ResultSet] | List[MetricsSet] | List[CheckSet],
]
