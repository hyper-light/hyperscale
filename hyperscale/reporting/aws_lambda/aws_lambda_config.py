from typing import Optional

from pydantic import BaseModel, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class AWSLambdaConfig(BaseModel):
    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr
    region_name: StrictStr
    workflow_results_lambda_name: StrictStr = "hyperscale_workflow_results"
    step_results_lambda_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.AWSLambda
