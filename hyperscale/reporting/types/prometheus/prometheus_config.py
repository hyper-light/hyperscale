from typing import Any, Dict, Optional

from pydantic import BaseModel

from hyperscale.reporting.types.common.types import ReporterTypes


class PrometheusConfig(BaseModel):
    pushgateway_address: str = "localhost:9091"
    auth_request_method: str = "GET"
    auth_request_timeout: int = 60000
    auth_request_data: Dict[str, Any] = {}
    username: Optional[str]
    password: Optional[str]
    namespace: Optional[str]
    job_name: str = "hyperscale"
    custom_fields: Dict[str, str] = {}
    reporter_type: ReporterTypes = ReporterTypes.Prometheus
