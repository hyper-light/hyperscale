import os

from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import PostgresConfig


class SubmitPostgresResultsStage(Submit):
    config=PostgresConfig(
        host='127.0.0,1',
        database='results',
        username=os.getenv('POSTGRES_USERNAME', ''),
        password=os.getenv('POSTGRES_PASSWORD', ''),
        events_table='events',
        metrics_table='metrics'
    )