from typing import Dict, Union

from hyperscale.reporting.experiment.experiment_metrics_set import ExperimentMetricsSet
from hyperscale.reporting.metric.stage_metrics_summary import StageMetricsSummary
from hyperscale.reporting.system.system_metrics_set import SystemMetricsSet

ExecutionResults = Dict[str, StageMetricsSummary]

GraphExecutionResults = Dict[
    str,
    Dict[
        str, Union[Dict[str, ExperimentMetricsSet], ExecutionResults, SystemMetricsSet]
    ],
]

SystemMetricsCollection = Dict[str, SystemMetricsSet]

GraphResults = Dict[str, Union[GraphExecutionResults, SystemMetricsCollection]]
