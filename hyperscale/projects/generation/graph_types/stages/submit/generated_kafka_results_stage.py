from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import KafkaConfig


class SubmitKafkaResultsStage(Submit):
    config=KafkaConfig(
        host='localhost:9092',
        client_id='results',
        events_topic='events',
        metrics_topic='metrics',
        compression_type=None
    )