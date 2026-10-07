from typing import List

from pydantic import BaseModel, ConfigDict, conlist, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes

from .cloudwatch_target import _CloudwatchTarget as _CloudwatchTarget

# Workflows carrying a CloudwatchConfig are pickled to workers: keep the
# target's pickled class path the one it had before the split.
_CloudwatchTarget.__module__ = __name__


class CloudwatchConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr
    region_name: StrictStr
    iam_role_arn: StrictStr
    schedule_rate: StrictStr | None = None
    workflow_results_rule_name: StrictStr = "hyperscale-workflow-results"
    step_results_rule_name: StrictStr = "hyperscale-step-results"
    cloudwatch_targets: conlist(_CloudwatchTarget, min_length=1)
    aws_resource_arns: List[StrictStr] = []
    cloudwatch_source: StrictStr = "hyperscale"
    submit_timeout: StrictInt = 60
    reporter_type: ReporterTypes = ReporterTypes.Cloudwatch
