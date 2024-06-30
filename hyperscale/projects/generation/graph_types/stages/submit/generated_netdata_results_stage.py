from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import NetdataConfig


class SubmitNetdataResultsStage(Submit):
    config = NetdataConfig(host="localhost", port=8125)
