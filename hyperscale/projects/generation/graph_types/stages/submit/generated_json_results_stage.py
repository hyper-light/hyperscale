from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import JSONConfig


class SubmitJSONResultsStage(Submit):
    config = JSONConfig(
        events_filepath="./events.json", metrics_filepath="./metrics.json"
    )
