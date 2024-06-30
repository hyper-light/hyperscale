import os

from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import NewRelicConfig


class SubmitNewrelicResultsStage(Submit):
    config = NewRelicConfig(
        config_path=os.getenv("NEWRELIC_CONFIG_PATH", ""),
        environment=os.getenv("NEWRELIC_ENVIRONMENT", ""),
        newrelic_application_name="results",
    )
