from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import GraphiteConfig


class SubmitGraphiteResultsStage(Submit):
    config = GraphiteConfig(host="localhost", port=2003)
