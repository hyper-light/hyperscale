from typing import Optional

from pydantic import BaseModel

from hyperscale.reporting.types.common.types import ReporterTypes


class AWSLambdaConfig(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str
    events_lambda: str = "hyperscale_events"
    metrics_lambda: str = "hyperscale_metrics"
    system_metrics_lambda: str = "hyperscale_system_metrics"
    experiments_lambda: Optional[str]
    streams_lambda: Optional[str]
    reporter_type: ReporterTypes = ReporterTypes.AWSLambda
