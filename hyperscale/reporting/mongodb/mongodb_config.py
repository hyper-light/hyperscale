from typing import Optional

from pydantic import BaseModel, ConfigDict, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class MongoDBConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    host: StrictStr = "localhost"
    port: StrictInt = 27017
    username: StrictStr | None = None
    password: StrictStr | None = None
    database: StrictStr = "hyperscale"
    workflow_results_collection_name: StrictStr = "hyperscale_workflow_results"
    step_results_collection_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.MongoDB
