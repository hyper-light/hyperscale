from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import CSVConfig


class SubmitCSVResultsStage(Submit):
    config = CSVConfig(
        events_filepath="./events.json", metrics_filepath="./metrics.json"
    )
