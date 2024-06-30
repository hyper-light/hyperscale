from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import DogStatsDConfig


class SubmitDogStatsDResultsStage(Submit):
    config = DogStatsDConfig(host="localhost", port=8125)
