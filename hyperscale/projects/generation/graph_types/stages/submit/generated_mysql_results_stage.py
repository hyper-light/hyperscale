import os

from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import MySQLConfig


class SubmitMySQLResultsStage(Submit):
    config = MySQLConfig(
        host="127.0.0,1",
        database="results",
        username=os.getenv("MYSQL_USERNAME", ""),
        password=os.getenv("MYSQL_PASSWORD", ""),
        events_table="events",
        metrics_table="metrics",
    )
