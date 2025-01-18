from typing import Optional

from pydantic import BaseModel, StrictStr

from hyperscale.reporting.types.common.types import ReporterTypes


class AWSLambdaConfig(BaseModel):
    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr
    region_name: StrictStr
    workflow_results_lambda_name: StrictStr | None = None
    step_results_lambda_name: StrictStr | None = None
    reporter_type: ReporterTypes = ReporterTypes.AWSLambda
