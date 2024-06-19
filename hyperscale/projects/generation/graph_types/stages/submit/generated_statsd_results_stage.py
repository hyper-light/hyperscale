from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import StatsDConfig


class SubmitStatsDResultsStage(Submit):
    config=StatsDConfig(
        host='localhost',
        port=8125
    )