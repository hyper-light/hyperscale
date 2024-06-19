from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import TelegrafConfig


class SubmitTelegrafResultsStage(Submit):
    config=TelegrafConfig(
        host='localhost',
        port=8094
    )