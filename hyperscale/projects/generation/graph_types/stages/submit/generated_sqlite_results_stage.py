import os

from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import SQLiteConfig


class SubmitSQLiteResultsStage(Submit):
    config = SQLiteConfig(
        path=f"{os.getcwd}/results.db", events_table="events", metrics_table="metrics"
    )
