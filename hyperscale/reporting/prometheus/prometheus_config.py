from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class PrometheusConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    pushgateway_host: StrictStr = "localhost"
    pushgateway_port: StrictInt = 9091
    auth_request_method: StrictStr = "GET"
    auth_request_timeout: StrictInt = 60000
    auth_request_data: Dict[StrictStr, Any] = {}
    username: StrictStr | None = None
    password: StrictStr | None = None
    namespace: StrictStr = "hyperscale"
    job_name: StrictStr = "hyperscale"
    reporter_type: ReporterTypes = ReporterTypes.Prometheus
