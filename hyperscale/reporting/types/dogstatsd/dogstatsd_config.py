from typing import Dict

from pydantic import BaseModel

from hyperscale.reporting.types.common.types import ReporterTypes


class DogStatsDConfig(BaseModel):
    host: str = "localhost"
    port: int = 8125
    custom_fields: Dict[str, str] = {}
    reporter_type: ReporterTypes = ReporterTypes.DogStatsD
