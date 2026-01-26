from typing import Literal

from pydantic import BaseModel, ConfigDict, StrictStr, StrictInt, StrictBool

from hyperscale.reporting.common.types import ReporterTypes


RedisChannelType = Literal["channel", "pipeline"]


class RedisConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    host: StrictStr = "localhost"
    port: StrictInt = 6379
    username: StrictStr | None = None
    password: StrictStr | None = None
    database: StrictInt = 0
    workflow_results_channel_name: StrictStr = "hyperscale_workflow_results"
    step_results_channel_name: StrictStr = "hyperscale_step_results"
    channel_type: RedisChannelType = "pipeline"
    secure: StrictBool = False
    reporter_type: ReporterTypes = ReporterTypes.Redis
