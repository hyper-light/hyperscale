from pydantic import BaseModel, ConfigDict, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class NewRelicConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    config_path: StrictStr
    environment: StrictStr | None = None
    registration_timeout: StrictInt = 60
    shutdown_timeout: StrictInt = 60
    newrelic_application_name: StrictStr = "hyperscale"
    reporter_type: ReporterTypes = ReporterTypes.NewRelic
