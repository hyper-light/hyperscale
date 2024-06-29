from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import TelegrafStatsDConfig


class SubmitTelegrafStatsDResultsStage(Submit):
    config = TelegrafStatsDConfig(host="0.0.0.0", port=8125)
