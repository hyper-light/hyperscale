import os

from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import PrometheusConfig


class SubmitPrometheusResultsStage(Submit):
    config=PrometheusConfig(
        pushgateway_address='localhost:9091',
        auth_request_method='GET',
        username=os.getenv('PROMETHEUS_PUSHGATEWAY_USERNAME', ''),
        password=os.getenv('PROMETHEUS_PUSHGATEWAY_PASSWORD', ''),
        namespace='results',
        job_name='results'
    )