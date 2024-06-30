import os

from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import TimescaleDBConfig


class SubmitTimescaleDBResultsStage(Submit):
    config = TimescaleDBConfig(
        host="127.0.0,1",
        database="results",
        username=os.getenv("TIMESCALEDB_USERNAME", ""),
        password=os.getenv("TIMESCALEDB_PASSWORD", ""),
        events_table="events",
        metrics_table="metrics",
    )
