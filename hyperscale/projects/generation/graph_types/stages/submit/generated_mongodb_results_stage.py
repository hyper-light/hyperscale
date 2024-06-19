import os

from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import MongoDBConfig


class SubmitMongoDBResultsStage(Submit):
    config=MongoDBConfig(
        host='localhost:27017',
        username=os.getenv('MONGODB_USERNAME', ''),
        password=os.getenv('MONGODB_PASSWORD', ''),
        database='results',
        events_collection='events',
        metrics_collection='metrics'
    )